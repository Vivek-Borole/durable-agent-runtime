package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type report struct {
	Attempts          int       `json:"attempts"`
	Workers           int       `json:"workers"`
	CommittedEffects  int64     `json:"committedEffects"`
	DurationMillis    int64     `json:"durationMillis"`
	Passed            bool      `json:"passed"`
	FailureConditions []string  `json:"failureConditions"`
	Machine           machine   `json:"machine"`
	CompletedAt       time.Time `json:"completedAt"`
}

type machine struct {
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	CPUs          int    `json:"cpus"`
	MemoryBytes   int64  `json:"memoryBytes"`
	GoVersion     string `json:"goVersion"`
	NodeVersion   string `json:"nodeVersion"`
	DockerVersion string `json:"dockerVersion"`
}

func main() {
	attempts := flag.Int("attempts", 100000, "number of concurrent redelivery attempts")
	workers := flag.Int("workers", 100, "bounded database worker count")
	output := flag.String("output", "docs/evidence/effect-fault-report.json", "JSON report path")
	flag.Parse()
	url := os.Getenv("DAR_WORKER_POSTGRES_URL")
	if url == "" {
		panic("DAR_WORKER_POSTGRES_URL is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	stamp := time.Now().UTC().UnixNano()
	tenantSlug := fmt.Sprintf("fault-%d", stamp)
	var tenantID, workflowID, runID string
	err = pool.QueryRow(ctx, "insert into tenants (slug) values ($1) returning id", tenantSlug).Scan(&tenantID)
	if err != nil {
		panic(err)
	}
	defer pool.Exec(ctx, "delete from tenants where id = $1", tenantID)
	definition := `{"name":"fault-fixture","version":"v1","budgetCents":1,"allowedHosts":[],"steps":[{"kind":"tool","tool":"mock_ticket_write","sideEffect":false}]}`
	err = pool.QueryRow(ctx, "insert into workflow_definitions (tenant_id,name,version,definition,budget_cents) values ($1,'fault-fixture','v1',$2::jsonb,1) returning id", tenantID, definition).Scan(&workflowID)
	if err != nil {
		panic(err)
	}
	err = pool.QueryRow(ctx, "insert into workflow_runs (tenant_id,workflow_id,idempotency_key,input,budget_cents) values ($1,$2,'fault-idempotency-0001','{}'::jsonb,1) returning id", tenantID, workflowID).Scan(&runID)
	if err != nil {
		panic(err)
	}

	started := time.Now()
	jobs := make(chan struct{})
	var inserted atomic.Int64
	var failed atomic.Int64
	var group sync.WaitGroup
	for range *workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for range jobs {
				command, queryErr := pool.Exec(ctx, `insert into effect_commits (tenant_id,run_id,step_index,effect_key,outcome)
                  values ($1,$2,0,'stable-mock-ticket-effect','{"tool":"mock_ticket_write"}'::jsonb)
                  on conflict (tenant_id,effect_key) do nothing`, tenantID, runID)
				if queryErr != nil {
					failed.Add(1)
					continue
				}
				inserted.Add(command.RowsAffected())
			}
		}()
	}
	for range *attempts {
		jobs <- struct{}{}
	}
	close(jobs)
	group.Wait()
	var persisted int64
	if err = pool.QueryRow(ctx, "select count(*) from effect_commits where tenant_id = $1 and run_id = $2", tenantID, runID).Scan(&persisted); err != nil {
		panic(err)
	}

	report := report{
		Attempts: *attempts, Workers: *workers, CommittedEffects: persisted,
		DurationMillis: time.Since(started).Milliseconds(), Passed: persisted == 1 && inserted.Load() == 1 && failed.Load() == 0,
		FailureConditions: []string{"concurrent duplicate delivery of one stable effect key", "PostgreSQL unique constraint as the commit boundary"},
		Machine:           currentMachine(), CompletedAt: time.Now().UTC(),
	}
	if err = os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		panic(err)
	}
	encoded, _ := json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile(*output, encoded, 0o644); err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
	if !report.Passed {
		os.Exit(1)
	}
}

func currentMachine() machine {
	memory, _ := strconv.ParseInt(commandOutput("sysctl", "-n", "hw.memsize"), 10, 64)
	return machine{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), MemoryBytes: memory, GoVersion: runtime.Version(), NodeVersion: commandOutput("node", "--version"), DockerVersion: commandOutput("docker", "--version")}
}

func commandOutput(name string, args ...string) string {
	output, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}
