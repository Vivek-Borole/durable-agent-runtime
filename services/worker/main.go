package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

type queuedRun struct {
	RunID string `json:"runId"`
}

type workflowDefinition struct {
	AllowedHosts []string          `json:"allowedHosts"`
	Steps        []json.RawMessage `json:"steps"`
}

type step struct {
	Kind      string `json:"kind"`
	Provider  string `json:"provider"`
	Operation string `json:"operation"`
	Tool      string `json:"tool"`
}

type leasedRun struct {
	ID          string
	TenantID    string
	CurrentStep int
	Input       map[string]any
	Definition  workflowDefinition
}

type runtimeStore struct{ pool *pgxpool.Pool }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	databaseURL := os.Getenv("DAR_WORKER_POSTGRES_URL")
	if databaseURL == "" {
		slog.Error("DAR_WORKER_POSTGRES_URL is required")
		return
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		slog.Error("worker database connection failed", "error", err)
		return
	}
	defer pool.Close()
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	nc, err := nats.Connect(natsURL, nats.Name("dar-worker"))
	if err != nil {
		slog.Error("worker NATS connection failed", "error", err)
		return
	}
	defer nc.Drain()
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("JetStream unavailable", "error", err)
		return
	}
	if _, err = js.StreamInfo("DAR"); errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(&nats.StreamConfig{Name: "DAR", Subjects: []string{"dar.run.queued"}, Storage: nats.FileStorage})
	}
	if err != nil {
		slog.Error("JetStream stream setup failed", "error", err)
		return
	}
	sub, err := js.PullSubscribe("dar.run.queued", "dar-worker", nats.BindStream("DAR"))
	if err != nil {
		slog.Error("JetStream consumer setup failed", "error", err)
		return
	}

	store := runtimeStore{pool: pool}
	workerID := "dar-worker-" + hostname()
	slog.Info("durable agent worker started", "delivery", "at-least-once", "worker", workerID)
	for ctx.Err() == nil {
		messages, fetchErr := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(fetchErr, nats.ErrTimeout) {
			continue
		}
		if fetchErr != nil {
			slog.Warn("JetStream fetch failed", "error", fetchErr)
			continue
		}
		for _, message := range messages {
			if err := store.process(ctx, workerID, message.Data); err != nil {
				slog.Warn("run delivery not acknowledged", "error", err)
				_ = message.Nak()
				continue
			}
			_ = message.Ack()
		}
	}
	slog.Info("durable agent worker stopped")
}

func (s runtimeStore) process(ctx context.Context, workerID string, data []byte) error {
	var message queuedRun
	if err := json.Unmarshal(data, &message); err != nil || message.RunID == "" {
		return fmt.Errorf("invalid queued run message")
	}
	run, err := s.claim(ctx, message.RunID, workerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} // duplicate delivery or a cancelled/active run
	if err != nil {
		return err
	}
	return s.execute(ctx, run)
}

func (s runtimeStore) claim(ctx context.Context, runID, workerID string) (*leasedRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var tenantID, definitionRaw, inputRaw []byte
	var currentStep int
	err = tx.QueryRow(ctx, `select r.tenant_id::text, r.current_step, r.input, w.definition
      from workflow_runs r join workflow_definitions w on w.id = r.workflow_id
      where r.id = $1 and (r.state = 'queued' or (r.state in ('leased', 'running') and r.lease_expires_at < now()))
      for update skip locked`, runID).Scan(&tenantID, &currentStep, &inputRaw, &definitionRaw)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `update workflow_runs set state = 'running', lease_owner = $1,
      lease_expires_at = now() + interval '30 seconds', attempt_count = attempt_count + 1, updated_at = now() where id = $2`, workerID, runID); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `insert into run_events (tenant_id, run_id, event_type, detail) values ($1::uuid, $2::uuid, 'leased', 'Worker lease acquired'), ($1::uuid, $2::uuid, 'started', 'Worker execution started')`, string(tenantID), runID); err != nil {
		return nil, err
	}
	var definition workflowDefinition
	var input map[string]any
	if err = json.Unmarshal(definitionRaw, &definition); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(inputRaw, &input); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &leasedRun{ID: runID, TenantID: string(tenantID), CurrentStep: currentStep, Input: input, Definition: definition}, nil
}

func (s runtimeStore) execute(ctx context.Context, run *leasedRun) error {
	for index := run.CurrentStep; index < len(run.Definition.Steps); index++ {
		var current step
		if err := json.Unmarshal(run.Definition.Steps[index], &current); err != nil {
			return s.fail(ctx, run, "invalid_workflow_step")
		}
		switch current.Kind {
		case "approval":
			return s.awaitApproval(ctx, run, index)
		case "model":
			if current.Provider != "mock" {
				return s.fail(ctx, run, "provider_not_configured")
			}
			if err := s.recordStep(ctx, run, index, "model", "succeeded", "mock-model"); err != nil {
				return err
			}
		case "transform":
			if current.Operation != "extract_json" && current.Operation != "template" {
				return s.fail(ctx, run, "unsupported_transform")
			}
			if err := s.recordStep(ctx, run, index, "transform", "succeeded", current.Operation); err != nil {
				return err
			}
		case "tool":
			if err := s.executeTool(ctx, run, index, current); err != nil {
				return err
			}
		default:
			return s.fail(ctx, run, "unsupported_step")
		}
		if _, err := s.pool.Exec(ctx, "update workflow_runs set current_step = $1, lease_expires_at = now() + interval '30 seconds', updated_at = now() where id = $2", index+1, run.ID); err != nil {
			return err
		}
	}
	_, err := s.pool.Exec(ctx, `update workflow_runs set state = 'succeeded', lease_owner = null, lease_expires_at = null, terminal_evidence = jsonb_build_object('kind', 'completed') where id = $1`, run.ID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `insert into run_events (tenant_id, run_id, event_type, detail) values ($1::uuid, $2::uuid, 'succeeded', 'All workflow steps completed')`, run.TenantID, run.ID)
	return err
}

func (s runtimeStore) executeTool(ctx context.Context, run *leasedRun, index int, current step) error {
	switch current.Tool {
	case "mock_data_read":
		return s.recordStep(ctx, run, index, "tool", "succeeded", "mock-data-read")
	case "mock_ticket_write":
		effectKey := fmt.Sprintf("%s:%d:mock-ticket", run.ID, index)
		command, err := s.pool.Exec(ctx, `insert into effect_commits (tenant_id, run_id, step_index, effect_key, outcome)
          values ($1::uuid, $2::uuid, $3, $4, '{"tool":"mock_ticket_write"}'::jsonb) on conflict (tenant_id, effect_key) do nothing`, run.TenantID, run.ID, index, effectKey)
		if err != nil {
			return err
		}
		if command.RowsAffected() == 0 {
			return s.recordStep(ctx, run, index, "tool", "succeeded", "mock-ticket-replayed")
		}
		return s.recordStep(ctx, run, index, "tool", "succeeded", "mock-ticket-committed")
	case "allowlisted_http_fetch":
		target, _ := run.Input["url"].(string)
		if err := validateURL(target, run.Definition.AllowedHosts); err != nil {
			return s.fail(ctx, run, "http_url_not_allowed")
		}
		client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
		response, err := client.Get(target)
		if err != nil || response.StatusCode >= 400 {
			return s.fail(ctx, run, "http_fetch_failed")
		}
		defer response.Body.Close()
		if response.ContentLength > 1_000_000 {
			return s.fail(ctx, run, "http_response_too_large")
		}
		contentType, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if parseErr != nil || (contentType != "application/json" && contentType != "text/plain") {
			return s.fail(ctx, run, "http_content_type_not_allowed")
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1_000_001))
		if readErr != nil {
			return s.fail(ctx, run, "http_fetch_failed")
		}
		if len(body) > 1_000_000 {
			return s.fail(ctx, run, "http_response_too_large")
		}
		return s.recordStep(ctx, run, index, "tool", "succeeded", "allowlisted-http-fetch")
	default:
		return s.fail(ctx, run, "tool_not_registered")
	}
}

func (s runtimeStore) recordStep(ctx context.Context, run *leasedRun, index int, kind, status, safeLabel string) error {
	hash := sha256.Sum256([]byte(run.ID + ":" + fmt.Sprint(index) + ":" + safeLabel))
	_, err := s.pool.Exec(ctx, `insert into run_step_results (tenant_id, run_id, step_index, result_kind, status, output_ref, evidence)
      values ($1::uuid, $2::uuid, $3, $4, $5, $6, jsonb_build_object('label', $7::text))
      on conflict (tenant_id, run_id, step_index) do nothing`, run.TenantID, run.ID, index, kind, status, hex.EncodeToString(hash[:]), safeLabel)
	return err
}

func (s runtimeStore) awaitApproval(ctx context.Context, run *leasedRun, index int) error {
	if err := s.recordStep(ctx, run, index, "approval", "awaiting_approval", "approval-required"); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `update workflow_runs set state = 'awaiting_approval', lease_owner = null, lease_expires_at = null, current_step = $1 where id = $2`, index, run.ID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `insert into run_events (tenant_id, run_id, event_type, detail) values ($1::uuid, $2::uuid, 'approval_requested', 'Configured side effect requires approval')`, run.TenantID, run.ID)
	return err
}

func (s runtimeStore) fail(ctx context.Context, run *leasedRun, code string) error {
	_, err := s.pool.Exec(ctx, `update workflow_runs set state = 'failed', lease_owner = null, lease_expires_at = null, last_error_code = $1,
      terminal_evidence = jsonb_build_object('errorCode', $1) where id = $2`, code, run.ID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `insert into run_events (tenant_id, run_id, event_type, detail) values ($1::uuid, $2::uuid, 'failed', $3)`, run.TenantID, run.ID, code)
	return err
}

func validateURL(raw string, hosts []string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return errors.New("invalid HTTPS URL")
	}
	for _, host := range hosts {
		if strings.EqualFold(parsed.Hostname(), host) {
			return nil
		}
	}
	return errors.New("host is not allowlisted")
}

func hostname() string {
	host, err := os.Hostname()
	if err != nil {
		return "local"
	}
	return host
}
