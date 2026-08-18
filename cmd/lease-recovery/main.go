// lease-recovery produces a reproducible evidence artifact for the durable
// boundary used after a worker disappears. It simulates process loss by
// letting worker A's lease expire before worker B takes over; it does not
// claim to be a process-kill soak test.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type recoveryReport struct {
	Scenario            string    `json:"scenario"`
	FirstLeaseOwner     string    `json:"firstLeaseOwner"`
	RecoveredLeaseOwner string    `json:"recoveredLeaseOwner"`
	Attempts            int       `json:"attempts"`
	CommittedEffects    int       `json:"committedEffects"`
	FinalState          string    `json:"finalState"`
	Passed              bool      `json:"passed"`
	CompletedAt         time.Time `json:"completedAt"`
}

func main() {
	output := flag.String("output", "docs/evidence/lease-recovery-report.json", "JSON report path")
	flag.Parse()
	url := os.Getenv("DAR_BENCHMARK_POSTGRES_URL")
	if url == "" {
		panic("DAR_BENCHMARK_POSTGRES_URL is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	stamp := time.Now().UTC().UnixNano()
	var tenantID, workflowID, runID string
	if err = pool.QueryRow(ctx, "insert into tenants (slug) values ($1) returning id", fmt.Sprintf("recovery-%d", stamp)).Scan(&tenantID); err != nil {
		panic(err)
	}
	defer pool.Exec(ctx, "delete from tenants where id = $1", tenantID)
	definition := `{"name":"recovery-fixture","version":"v1","budgetCents":1,"allowedHosts":[],"steps":[{"kind":"tool","tool":"mock_ticket_write","sideEffect":true}]}`
	if err = pool.QueryRow(ctx, "insert into workflow_definitions (tenant_id,name,version,definition,budget_cents) values ($1,'recovery-fixture','v1',$2::jsonb,1) returning id", tenantID, definition).Scan(&workflowID); err != nil {
		panic(err)
	}
	if err = pool.QueryRow(ctx, "insert into workflow_runs (tenant_id,workflow_id,idempotency_key,input,budget_cents) values ($1,$2,'recovery-idempotency-0001','{}'::jsonb,1) returning id", tenantID, workflowID).Scan(&runID); err != nil {
		panic(err)
	}

	// Worker A acquired a lease, then disappeared before committing an effect.
	if _, err = pool.Exec(ctx, "update workflow_runs set state='running', lease_owner='worker-a', lease_expires_at=now()-interval '1 second', attempt_count=1 where id=$1", runID); err != nil {
		panic(err)
	}
	// Worker B owns the expired lease and makes the effect and terminal state
	// durable in a single transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)
	var recovered bool
	err = tx.QueryRow(ctx, `update workflow_runs set state='running', lease_owner='worker-b', lease_expires_at=now()+interval '30 seconds', attempt_count=attempt_count+1
		where id=$1 and state in ('running','leased') and lease_expires_at < now() returning true`, runID).Scan(&recovered)
	if err != nil || !recovered {
		panic("expired lease was not recoverable")
	}
	if _, err = tx.Exec(ctx, `insert into effect_commits (tenant_id,run_id,step_index,effect_key,outcome)
		values ($1,$2,0,'recovery-stable-effect','{"tool":"mock_ticket_write"}'::jsonb) on conflict (tenant_id,effect_key) do nothing`, tenantID, runID); err != nil {
		panic(err)
	}
	if _, err = tx.Exec(ctx, "update workflow_runs set state='succeeded', current_step=1, lease_owner=null, lease_expires_at=null, terminal_evidence=jsonb_build_object('kind','recovered-completion') where id=$1", runID); err != nil {
		panic(err)
	}
	if err = tx.Commit(ctx); err != nil {
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
	report := recoveryReport{Scenario: "simulated worker loss followed by expired-lease handoff", FirstLeaseOwner: "worker-a", RecoveredLeaseOwner: "worker-b", Attempts: attempts, CommittedEffects: effects, FinalState: state, Passed: attempts == 2 && effects == 1 && state == "succeeded", CompletedAt: time.Now().UTC()}
	if err = os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		panic(err)
	}
	body, _ := json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile(*output, body, 0o644); err != nil {
		panic(err)
	}
	fmt.Println(string(body))
	if !report.Passed {
		os.Exit(1)
	}
}
