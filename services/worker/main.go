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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type queuedRun struct {
	RunID string `json:"runId"`
}

var errInvalidQueueMessage = errors.New("invalid queued run message")

var (
	queueDelay      = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "dar_worker_queue_delay_seconds", Help: "Delay from durable run creation to lease acquisition.", Buckets: []float64{.005, .01, .05, .1, .25, .5, 1, 5, 30}})
	leaseRecoveries = prometheus.NewCounter(prometheus.CounterOpts{Name: "dar_worker_lease_recoveries_total", Help: "Expired run leases recovered by another worker."})
	runTerminals    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "dar_worker_terminal_runs_total", Help: "Terminal workflow outcomes."}, []string{"state"})
	effectReplays   = prometheus.NewCounter(prometheus.CounterOpts{Name: "dar_worker_effect_replays_total", Help: "Duplicate effect attempts suppressed by the durable ledger."})
	workerFailures  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "dar_worker_failures_total", Help: "Safe worker failure classes."}, []string{"error_class"})
	draining        = prometheus.NewGauge(prometheus.GaugeOpts{Name: "dar_worker_draining", Help: "One while this worker is draining."})
	metricsReady    atomic.Bool
)

func init() {
	prometheus.MustRegister(queueDelay, leaseRecoveries, runTerminals, effectReplays, workerFailures, draining)
}

type workflowDefinition struct {
	SchemaVersion string            `json:"schemaVersion"`
	AllowedHosts  []string          `json:"allowedHosts"`
	Steps         []json.RawMessage `json:"steps"`
}

type step struct {
	Kind           string `json:"kind"`
	Provider       string `json:"provider"`
	PromptTemplate string `json:"promptTemplate"`
	Operation      string `json:"operation"`
	Tool           string `json:"tool"`
}

type leasedRun struct {
	ID               string
	TenantID         string
	CurrentStep      int
	Input            map[string]any
	Definition       workflowDefinition
	CredentialHandle *string
}

type runtimeStore struct{ pool *pgxpool.Pool }

func main() {
	receiveCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	shutdownTelemetry := startTelemetry(workCtx)
	defer shutdownTelemetry(context.Background())
	databaseURL := os.Getenv("DAR_WORKER_POSTGRES_URL")
	if databaseURL == "" {
		slog.Error("DAR_WORKER_POSTGRES_URL is required")
		return
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		slog.Error("worker database configuration failed", "error", err)
		return
	}
	poolConfig.MaxConns = int32(workerConcurrency())
	pool, err := pgxpool.NewWithConfig(workCtx, poolConfig)
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
	streamName := environmentOr("DAR_STREAM", "DAR")
	queueSubject := environmentOr("DAR_QUEUE_SUBJECT", "dar.run.queued")
	consumerName := environmentOr("DAR_CONSUMER", "dar-worker-v1")
	legacyJS, err := nc.JetStream()
	if err != nil {
		slog.Error("JetStream unavailable", "error", err)
		return
	}
	if _, err = legacyJS.StreamInfo(streamName); errors.Is(err, nats.ErrStreamNotFound) {
		_, err = legacyJS.AddStream(&nats.StreamConfig{Name: streamName, Subjects: []string{queueSubject}, Storage: nats.FileStorage})
	}
	if err != nil {
		slog.Error("JetStream stream setup failed", "error", err)
		return
	}
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("JetStream client setup failed", "error", err)
		return
	}
	consumer, err := js.CreateOrUpdateConsumer(workCtx, streamName, jetstream.ConsumerConfig{
		// Consumer names are deployment state. Bumping the name avoids silently
		// inheriting an incompatible pre-continuous-pull consumer configuration.
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
		MaxAckPending: workerConcurrency() * 4,
		FilterSubject: queueSubject,
	})
	if err != nil {
		slog.Error("JetStream consumer setup failed", "error", err)
		return
	}
	metricsReady.Store(true)
	shutdownMetrics := startWorkerMetricsServer()
	defer shutdownMetrics(context.Background())

	store := runtimeStore{pool: pool}
	workerID := fmt.Sprintf("dar-worker-%s-%d", hostname(), os.Getpid())
	concurrency := workerConcurrency()
	iterator, err := consumer.Messages(jetstream.PullMaxMessages(concurrency*4), jetstream.PullThresholdMessages(concurrency*2), jetstream.PullExpiry(2*time.Second))
	if err != nil {
		slog.Error("JetStream message iterator setup failed", "error", err)
		return
	}
	defer iterator.Stop()
	jobs := make(chan jetstream.Msg, concurrency*4)
	var inFlight sync.WaitGroup
	for range concurrency {
		inFlight.Add(1)
		go func() {
			defer inFlight.Done()
			for message := range jobs {
				if err := store.process(workCtx, workerID, message.Data()); err != nil {
					if errors.Is(err, errInvalidQueueMessage) {
						slog.Warn("discarded invalid queue message")
						_ = message.Ack()
						continue
					}
					// Error values can originate in an upstream service. Keep telemetry
					// and logs to a fixed operational class, never tenant content.
					slog.Warn("run delivery not acknowledged", "error_class", "run_delivery_failed")
					_ = message.Nak()
					continue
				}
				_ = message.Ack()
			}
		}()
	}
	slog.Info("durable agent worker started", "delivery", "at-least-once", "worker", workerID, "concurrency", concurrency)
	for receiveCtx.Err() == nil {
		message, nextErr := iterator.Next(jetstream.NextContext(receiveCtx))
		if nextErr != nil {
			if receiveCtx.Err() == nil {
				slog.Warn("JetStream next message failed", "error_class", "jetstream_delivery_failed")
			}
			continue
		}
		select {
		case jobs <- message:
		case <-receiveCtx.Done():
		}
	}
	// Stop taking new work first. In-flight handlers retain a live context so
	// they can reach the next durable boundary during a Kubernetes drain.
	iterator.Stop()
	draining.Set(1)
	close(jobs)
	drained := make(chan struct{})
	go func() {
		inFlight.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		slog.Info("durable agent worker drain completed")
	case <-time.After(workerDrainTimeout()):
		slog.Warn("durable agent worker drain timed out", "error_class", "drain_timeout")
		cancelWork()
		<-drained
	}
	slog.Info("durable agent worker stopped")
}

func startWorkerMetricsServer() func(context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health/live", func(response http.ResponseWriter, _ *http.Request) { response.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/health/ready", func(response http.ResponseWriter, _ *http.Request) {
		if !metricsReady.Load() {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: environmentOr("DAR_WORKER_METRICS_ADDR", ":9091"), Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("worker metrics server stopped", "error_class", "metrics_server_failed")
		}
	}()
	return server.Shutdown
}

func workerConcurrency() int {
	if parsed, err := strconv.Atoi(os.Getenv("DAR_WORKER_CONCURRENCY")); err == nil && parsed > 0 && parsed <= 128 {
		return parsed
	}
	return 16
}

func workerDrainTimeout() time.Duration {
	if parsed, err := time.ParseDuration(os.Getenv("DAR_WORKER_DRAIN_TIMEOUT")); err == nil && parsed >= time.Second && parsed <= 2*time.Minute {
		return parsed
	}
	return 25 * time.Second
}

func environmentOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func (s runtimeStore) process(ctx context.Context, workerID string, data []byte) error {
	var message queuedRun
	if err := json.Unmarshal(data, &message); err != nil || !isUUID(message.RunID) {
		return errInvalidQueueMessage
	}
	ctx, span := otel.Tracer("durable-agent-runtime.worker").Start(ctx, "run.process")
	span.SetAttributes(attribute.String("run.id", message.RunID))
	defer span.End()
	run, err := s.claim(ctx, message.RunID, workerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} // duplicate delivery or a cancelled/active run
	if err != nil {
		return err
	}
	return s.execute(ctx, run)
}

func isUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if char != '-' {
				return false
			}
			continue
		}
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func (s runtimeStore) claim(ctx context.Context, runID, workerID string) (*leasedRun, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var tenantID, definitionRaw, inputRaw []byte
	var credentialHandle *string
	var currentStep int
	var previousState string
	var createdAt time.Time
	err = tx.QueryRow(ctx, `select r.tenant_id::text, r.current_step, r.input, w.definition, r.provider_credential_handle::text, r.state::text, r.created_at
      from workflow_runs r join workflow_definitions w on w.id = r.workflow_id
      where r.id = $1 and (r.state = 'queued' or (r.state in ('leased', 'running') and r.lease_expires_at < now()))
		for update of r skip locked`, runID).Scan(&tenantID, &currentStep, &inputRaw, &definitionRaw, &credentialHandle, &previousState, &createdAt)
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
	queueDelay.Observe(time.Since(createdAt).Seconds())
	if previousState != "queued" {
		leaseRecoveries.Inc()
	}
	return &leasedRun{ID: runID, TenantID: string(tenantID), CurrentStep: currentStep, Input: input, Definition: definition, CredentialHandle: credentialHandle}, nil
}

func (s runtimeStore) execute(ctx context.Context, run *leasedRun) error {
	if !supportedWorkflowSchema(run.Definition.SchemaVersion) {
		return s.fail(ctx, run, "unsupported_workflow_schema")
	}
	for index := run.CurrentStep; index < len(run.Definition.Steps); index++ {
		var current step
		if err := json.Unmarshal(run.Definition.Steps[index], &current); err != nil {
			return s.fail(ctx, run, "invalid_workflow_step")
		}
		stepCtx, span := otel.Tracer("durable-agent-runtime.worker").Start(ctx, "run.step")
		span.SetAttributes(attribute.Int("workflow.step.index", index), attribute.String("workflow.step.kind", current.Kind))
		var stepErr error
		switch current.Kind {
		case "approval":
			stepErr = s.awaitApproval(stepCtx, run, index)
			span.End()
			return stepErr
		case "model":
			if current.Provider == "mock" {
				if err := s.recordStep(stepCtx, run, index, "model", "succeeded", "mock-model"); err != nil {
					stepErr = err
					break
				}
				break
			}
			if current.Provider != "openai_compatible" {
				stepErr = s.fail(stepCtx, run, "provider_not_configured")
				break
			}
			if err := s.executeProviderModel(stepCtx, run, index, current); err != nil {
				stepErr = err
			}
		case "transform":
			if current.Operation != "extract_json" && current.Operation != "template" {
				stepErr = s.fail(stepCtx, run, "unsupported_transform")
				break
			}
			if err := s.recordStep(stepCtx, run, index, "transform", "succeeded", current.Operation); err != nil {
				stepErr = err
			}
		case "tool":
			if err := s.executeTool(stepCtx, run, index, current); err != nil {
				stepErr = err
			}
		default:
			stepErr = s.fail(stepCtx, run, "unsupported_step")
		}
		span.End()
		if stepErr != nil {
			return stepErr
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
	if err == nil {
		runTerminals.WithLabelValues("succeeded").Inc()
	}
	return err
}

func (s runtimeStore) executeProviderModel(ctx context.Context, run *leasedRun, index int, current step) error {
	if run.CredentialHandle == nil || os.Getenv("DAR_CONTROL_PLANE_INTERNAL_URL") == "" || os.Getenv("DAR_INTERNAL_TOKEN") == "" {
		return s.uncertain(ctx, run, "provider_credential_unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(os.Getenv("DAR_CONTROL_PLANE_INTERNAL_URL"), "/")+"/internal/credentials/"+*run.CredentialHandle+"/consume", nil)
	if err != nil {
		return s.uncertain(ctx, run, "provider_credential_unavailable")
	}
	req.Header.Set("x-runtime-internal-token", os.Getenv("DAR_INTERNAL_TOKEN"))
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil || response.StatusCode != http.StatusOK {
		if response != nil {
			response.Body.Close()
		}
		return s.uncertain(ctx, run, "provider_credential_unavailable")
	}
	defer response.Body.Close()
	var credential struct {
		Credential string `json:"credential"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 2048)).Decode(&credential) != nil || credential.Credential == "" {
		return s.uncertain(ctx, run, "provider_credential_unavailable")
	}
	baseURL, model := os.Getenv("OPENAI_COMPATIBLE_BASE_URL"), os.Getenv("OPENAI_COMPATIBLE_MODEL")
	if baseURL == "" || model == "" {
		return s.uncertain(ctx, run, "provider_not_configured")
	}
	if err := callOpenAICompatible(ctx, baseURL, model, credential.Credential, current.PromptTemplate); err != nil {
		return s.uncertain(ctx, run, "provider_model_unavailable")
	}
	return s.recordStep(ctx, run, index, "model", "succeeded", "openai-compatible-model")
}

func callOpenAICompatible(ctx context.Context, baseURL, model, credential, prompt string) error {
	if parsed, err := url.Parse(baseURL); err != nil || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && (parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "localhost"))) {
		return errors.New("provider URL is not allowed")
	}
	body, _ := json.Marshal(map[string]any{"model": model, "messages": []map[string]string{{"role": "user", "content": prompt}}})
	modelRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(baseURL, "/")+"/v1/chat/completions", strings.NewReader(string(body)))
	modelRequest.Header.Set("Authorization", "Bearer "+credential)
	modelRequest.Header.Set("Content-Type", "application/json")
	modelResponse, err := (&http.Client{Timeout: 20 * time.Second}).Do(modelRequest)
	if err != nil || modelResponse.StatusCode >= 400 {
		if modelResponse != nil {
			modelResponse.Body.Close()
		}
		return errors.New("provider request unavailable")
	}
	defer modelResponse.Body.Close()
	if err = validateProviderResponse(modelResponse.Body); err != nil {
		return err
	}
	return nil
}

func supportedWorkflowSchema(version string) bool {
	// Definitions written before v0.2 did not contain schemaVersion. They are
	// deliberately interpreted as schema 1 so queued v0.1 work remains valid.
	return version == "" || version == "1"
}

func validateProviderResponse(reader io.Reader) error {
	body, err := io.ReadAll(io.LimitReader(reader, 1_000_001))
	if err != nil || len(body) > 1_000_000 {
		return errors.New("provider response unavailable")
	}
	var response struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if json.Unmarshal(body, &response) != nil || len(response.Choices) == 0 || strings.TrimSpace(response.Choices[0].Message.Content) == "" {
		return errors.New("malformed provider response")
	}
	return nil
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
			effectReplays.Inc()
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
	workerFailures.WithLabelValues(code).Inc()
	_, err := s.pool.Exec(ctx, `update workflow_runs set state = 'failed', lease_owner = null, lease_expires_at = null, last_error_code = $1,
      terminal_evidence = jsonb_build_object('errorCode', $1) where id = $2`, code, run.ID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `insert into run_events (tenant_id, run_id, event_type, detail) values ($1::uuid, $2::uuid, 'failed', $3)`, run.TenantID, run.ID, code)
	if err == nil {
		runTerminals.WithLabelValues("failed").Inc()
	}
	return err
}

func (s runtimeStore) uncertain(ctx context.Context, run *leasedRun, code string) error {
	workerFailures.WithLabelValues(code).Inc()
	_, err := s.pool.Exec(ctx, `update workflow_runs set state = 'uncertain', lease_owner = null, lease_expires_at = null, last_error_code = $1,
      terminal_evidence = jsonb_build_object('errorCode', $1) where id = $2`, code, run.ID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `insert into run_events (tenant_id, run_id, event_type, detail) values ($1::uuid, $2::uuid, 'uncertain', $3)`, run.TenantID, run.ID, code)
	if err == nil {
		runTerminals.WithLabelValues("uncertain").Inc()
	}
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
