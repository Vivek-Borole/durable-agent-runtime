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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

type benchmarkReport struct {
	ResidentActiveMockRuns int       `json:"residentActiveMockRuns"`
	WarmupRuns             int       `json:"warmupRuns"`
	BacklogRuns            int       `json:"backlogRuns"`
	WorkerProcesses        int       `json:"workerProcesses"`
	SucceededRuns          int       `json:"succeededRuns"`
	P50Millis              float64   `json:"p50Millis"`
	P95Millis              float64   `json:"p95Millis"`
	P99Millis              float64   `json:"p99Millis"`
	PassP95                bool      `json:"passP95Under500Millis"`
	DurationMillis         int64     `json:"durationMillis"`
	Machine                machine   `json:"machine"`
	CompletedAt            time.Time `json:"completedAt"`
	FailureConditions      []string  `json:"failureConditions"`
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
	warmup := flag.Int("warmup", 1000, "synthetic warmup workflow runs")
	active := flag.Int("active", 1000, "resident synthetic workflows held at awaiting approval")
	backlog := flag.Int("backlog", 10000, "synthetic measured workflow runs")
	workers := flag.Int("workers", 1, "real worker processes; each has bounded parallel delivery")
	output := flag.String("output", "docs/evidence/scheduling-benchmark-report.json", "JSON report path")
	flag.Parse()
	dbURL, natsURL := os.Getenv("DAR_WORKER_POSTGRES_URL"), os.Getenv("NATS_URL")
	if dbURL == "" {
		panic("DAR_WORKER_POSTGRES_URL is required")
	}
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
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
	stamp := time.Now().UnixNano()
	streamName := fmt.Sprintf("DAR_BENCH_%d", stamp)
	queueSubject := fmt.Sprintf("dar.bench.%d", stamp)
	consumerName := fmt.Sprintf("dar-bench-%d", stamp)
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Subjects: []string{queueSubject}, Storage: nats.FileStorage})
	if err != nil {
		panic(err)
	}
	defer js.DeleteStream(streamName)

	binary := os.Getenv("DAR_WORKER_BINARY")
	if binary == "" {
		binary = "/tmp/durable-agent-runtime-worker"
	}
	children := make([]*exec.Cmd, 0, *workers)
	for range *workers {
		child := exec.Command(binary)
		child.Env = append(os.Environ(), "DAR_WORKER_POSTGRES_URL="+dbURL, "NATS_URL="+natsURL, "DAR_STREAM="+streamName, "DAR_QUEUE_SUBJECT="+queueSubject, "DAR_CONSUMER="+consumerName)
		child.Stdout, child.Stderr = os.Stderr, os.Stderr
		if err = child.Start(); err != nil {
			panic(err)
		}
		children = append(children, child)
	}
	defer func() {
		for _, child := range children {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	time.Sleep(600 * time.Millisecond)

	tenantID, workflowID := fixture(ctx, pool)
	defer pool.Exec(ctx, "delete from tenants where id = $1", tenantID)
	if err = createResidentActive(ctx, pool, tenantID, workflowID, *active); err != nil {
		panic(err)
	}
	if _, err = createAndPublish(ctx, pool, js, queueSubject, tenantID, workflowID, *warmup); err != nil {
		panic(err)
	}
	if err = waitFor(ctx, pool, tenantID, *warmup, 60*time.Second); err != nil {
		panic(err)
	}
	started := time.Now()
	ids, err := createAndPublish(ctx, pool, js, queueSubject, tenantID, workflowID, *backlog)
	if err != nil {
		panic(err)
	}
	if err = waitFor(ctx, pool, tenantID, *warmup+*backlog, 90*time.Second); err != nil {
		panic(err)
	}
	latencies, err := completionLatencies(ctx, pool, ids)
	if err != nil {
		panic(err)
	}
	report := benchmarkReport{ResidentActiveMockRuns: *active, WarmupRuns: *warmup, BacklogRuns: *backlog, WorkerProcesses: *workers, SucceededRuns: len(latencies), DurationMillis: time.Since(started).Milliseconds(), Machine: currentMachine(), CompletedAt: time.Now().UTC(), FailureConditions: []string{"synthetic mock-data-read workflows", "1,000 resident workflows held at awaiting approval", "JetStream at-least-once delivery", "PostgreSQL queued-to-started event timestamps used as completion boundary"}}
	report.P50Millis, report.P95Millis, report.P99Millis = percentile(latencies, .50), percentile(latencies, .95), percentile(latencies, .99)
	report.PassP95 = report.P95Millis <= 500 && len(latencies) == *backlog
	if err = os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		panic(err)
	}
	encoded, _ := json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile(*output, encoded, 0o644); err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
	if !report.PassP95 {
		os.Exit(1)
	}
}

func fixture(ctx context.Context, pool *pgxpool.Pool) (string, string) {
	stamp := time.Now().UnixNano()
	var tenant, workflow string
	if err := pool.QueryRow(ctx, "insert into tenants (slug) values ($1) returning id", fmt.Sprintf("bench-%d", stamp)).Scan(&tenant); err != nil {
		panic(err)
	}
	definition := `{"name":"benchmark-fixture","version":"v1","budgetCents":1,"allowedHosts":[],"steps":[{"kind":"tool","tool":"mock_data_read","sideEffect":false}]}`
	if err := pool.QueryRow(ctx, "insert into workflow_definitions (tenant_id,name,version,definition,budget_cents) values ($1,'benchmark-fixture','v1',$2::jsonb,1) returning id", tenant, definition).Scan(&workflow); err != nil {
		panic(err)
	}
	return tenant, workflow
}
func createResidentActive(ctx context.Context, pool *pgxpool.Pool, tenant, workflow string, count int) error {
	for i := range count {
		if _, err := pool.Exec(ctx, `insert into workflow_runs (tenant_id,workflow_id,idempotency_key,input,budget_cents,state,current_step)
          values ($1,$2,$3,'{}'::jsonb,1,'awaiting_approval',0)`, tenant, workflow, fmt.Sprintf("benchmark-active-%d", i)); err != nil {
			return err
		}
	}
	return nil
}
func createAndPublish(ctx context.Context, pool *pgxpool.Pool, js nats.JetStreamContext, subject, tenant, workflow string, count int) ([]string, error) {
	ids := make([]string, 0, count)
	for i := range count {
		var id string
		if err := pool.QueryRow(ctx, "insert into workflow_runs (tenant_id,workflow_id,idempotency_key,input,budget_cents) values ($1,$2,$3,'{}'::jsonb,1) returning id::text", tenant, workflow, fmt.Sprintf("benchmark-%d-%d", time.Now().UnixNano(), i)).Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		if _, err := pool.Exec(ctx, `insert into run_events (tenant_id,run_id,event_type,detail) values ($1::uuid,$2::uuid,'queued','Benchmark message published')`, tenant, id); err != nil {
			return nil, err
		}
		if _, err := js.Publish(subject, []byte(fmt.Sprintf(`{"runId":"%s"}`, id))); err != nil {
			return nil, err
		}
	}
	return ids, nil
}
func waitFor(ctx context.Context, pool *pgxpool.Pool, tenant string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(ctx, "select count(*) from workflow_runs where tenant_id=$1 and state='succeeded'", tenant).Scan(&n); err != nil {
			return err
		}
		if n >= want {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d successful runs", want)
}
func completionLatencies(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]float64, error) {
	rows, err := pool.Query(ctx, `select extract(epoch from (started.created_at-queued.created_at))*1000
      from workflow_runs r
      join run_events queued on queued.run_id=r.id and queued.event_type='queued'
      join run_events started on started.run_id=r.id and started.event_type='started'
      where r.id=any($1::uuid[])`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []float64{}
	for rows.Next() {
		var value float64
		if err = rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
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
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[int(float64(len(values)-1)*p)]
}
