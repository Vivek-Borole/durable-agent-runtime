// process-recovery creates evidence for an actual worker-process interruption.
// It uses an isolated JetStream stream and synthetic database fixture only.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

type recoveryReport struct {
	Scenario           string    `json:"scenario"`
	FirstWorkerKilled  bool      `json:"firstWorkerKilled"`
	LeaseExpired       bool      `json:"leaseExpired"`
	ReplacementClaimed bool      `json:"replacementClaimed"`
	Attempts           int       `json:"attempts"`
	CommittedEffects   int       `json:"committedEffects"`
	FinalState         string    `json:"finalState"`
	Passed             bool      `json:"passed"`
	CompletedAt        time.Time `json:"completedAt"`
}

func main() {
	adminURL, workerURL, binary := os.Getenv("DAR_BENCHMARK_POSTGRES_URL"), os.Getenv("DAR_WORKER_POSTGRES_URL"), os.Getenv("DAR_WORKER_BINARY")
	if adminURL == "" || workerURL == "" || binary == "" {
		panic("DAR_BENCHMARK_POSTGRES_URL, DAR_WORKER_POSTGRES_URL, and DAR_WORKER_BINARY are required")
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	nc, err := nats.Connect(natsURL)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	stamp := time.Now().UTC().UnixNano()
	stream, subject, consumer := fmt.Sprintf("DAR_REC_%d", stamp), fmt.Sprintf("dar.recovery.%d", stamp), fmt.Sprintf("dar-recovery-%d", stamp)
	if _, err = js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject}, Storage: nats.FileStorage}); err != nil {
		panic(err)
	}
	defer js.DeleteStream(stream)
	var tenantID, workflowID, runID string
	if err = pool.QueryRow(ctx, "insert into tenants (slug) values ($1) returning id", fmt.Sprintf("process-recovery-%d", stamp)).Scan(&tenantID); err != nil {
		panic(err)
	}
	defer pool.Exec(ctx, "delete from tenants where id=$1", tenantID)
	definition := `{"name":"process-recovery","version":"v1","budgetCents":1,"allowedHosts":[],"steps":[{"kind":"tool","tool":"mock_data_read","sideEffect":false}]}`
	if err = pool.QueryRow(ctx, "insert into workflow_definitions (tenant_id,name,version,definition,budget_cents) values ($1,'process-recovery','v1',$2::jsonb,1) returning id", tenantID, definition).Scan(&workflowID); err != nil {
		panic(err)
	}
	if err = pool.QueryRow(ctx, "insert into workflow_runs (tenant_id,workflow_id,idempotency_key,input,budget_cents) values ($1,$2,'process-recovery-0001','{}'::jsonb,1) returning id", tenantID, workflowID).Scan(&runID); err != nil {
		panic(err)
	}

	// The one-time trigger holds the worker inside the durable step-result write.
	// Killing it here proves no partially committed result survives process loss.
	if _, err = pool.Exec(ctx, `create or replace function dar_process_recovery_hold() returns trigger language plpgsql as $$ begin perform pg_sleep(12); return new; end $$; create trigger dar_process_recovery_hold before insert on run_step_results for each row execute function dar_process_recovery_hold();`); err != nil {
		panic(err)
	}
	defer pool.Exec(ctx, "drop trigger if exists dar_process_recovery_hold on run_step_results; drop function if exists dar_process_recovery_hold()")

	first := worker(binary, workerURL, natsURL, stream, subject, consumer)
	if err = first.Start(); err != nil {
		panic(err)
	}
	time.Sleep(750 * time.Millisecond)
	if _, err = js.Publish(subject, []byte(fmt.Sprintf(`{"runId":"%s"}`, runID))); err != nil {
		panic(err)
	}
	if err = waitState(ctx, pool, runID, "running", 8*time.Second); err != nil {
		panic(err)
	}
	// Let the worker enter the sleeping trigger, then abruptly terminate it.
	time.Sleep(800 * time.Millisecond)
	if err = first.Process.Kill(); err != nil {
		panic(err)
	}
	_ = first.Wait()
	if _, err = pool.Exec(ctx, "drop trigger dar_process_recovery_hold on run_step_results; drop function dar_process_recovery_hold()"); err != nil {
		panic(err)
	}

	// The worker's lease is deliberately 30 seconds. After it expires, a
	// duplicate delivery models JetStream redelivery without lowering normal
	// production acknowledgement timing.
	time.Sleep(31 * time.Second)
	second := worker(binary, workerURL, natsURL, stream, subject, consumer)
	if err = second.Start(); err != nil {
		panic(err)
	}
	defer func() { _ = second.Process.Kill(); _ = second.Wait() }()
	time.Sleep(750 * time.Millisecond)
	if _, err = js.Publish(subject, []byte(fmt.Sprintf(`{"runId":"%s"}`, runID))); err != nil {
		panic(err)
	}
	if err = waitState(ctx, pool, runID, "succeeded", 12*time.Second); err != nil {
		panic(err)
	}

	var attempts, effects int
	var state string
	if err = pool.QueryRow(ctx, "select attempt_count,state::text from workflow_runs where id=$1", runID).Scan(&attempts, &state); err != nil {
		panic(err)
	}
	if err = pool.QueryRow(ctx, "select count(*) from effect_commits where run_id=$1", runID).Scan(&effects); err != nil {
		panic(err)
	}
	report := recoveryReport{Scenario: "worker killed during a held mock-step write; expired lease plus duplicate delivery recovered", FirstWorkerKilled: true, LeaseExpired: true, ReplacementClaimed: attempts >= 2, Attempts: attempts, CommittedEffects: effects, FinalState: state, Passed: attempts >= 2 && effects == 0 && state == "succeeded", CompletedAt: time.Now().UTC()}
	body, _ := json.MarshalIndent(report, "", "  ")
	if err = os.MkdirAll("docs/evidence", 0o755); err != nil {
		panic(err)
	}
	if err = os.WriteFile(filepath.Join("docs/evidence", "process-recovery-report.json"), body, 0o644); err != nil {
		panic(err)
	}
	fmt.Println(string(body))
	if !report.Passed {
		os.Exit(1)
	}
}

func worker(binary, db, natsURL, stream, subject, consumer string) *exec.Cmd {
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(), "DAR_WORKER_POSTGRES_URL="+db, "NATS_URL="+natsURL, "DAR_STREAM="+stream, "DAR_QUEUE_SUBJECT="+subject, "DAR_CONSUMER="+consumer, "DAR_WORKER_CONCURRENCY=1")
	return cmd
}

func waitState(ctx context.Context, pool *pgxpool.Pool, runID, wanted string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var state string
		if err := pool.QueryRow(ctx, "select state::text from workflow_runs where id=$1", runID).Scan(&state); err != nil {
			return err
		}
		if state == wanted {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for state %q", wanted)
}
