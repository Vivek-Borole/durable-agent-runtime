package effects

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoryLedgerCommitsOnlyOnceUnderConcurrentRedelivery(t *testing.T) {
	ledger := NewMemoryLedger()
	commit := Commit{Key: Key{TenantID: "tenant-a", RunID: "run-a", StepIndex: 2, EffectKey: "mock-ticket-1"}, Outcome: []byte(`{"ticket":"t1"}`)}
	var inserted atomic.Int32
	var group sync.WaitGroup
	for range 1000 {
		group.Add(1)
		go func() {
			defer group.Done()
			created, err := ledger.Commit(context.Background(), commit)
			if err != nil {
				t.Errorf("commit: %v", err)
			}
			if created {
				inserted.Add(1)
			}
		}()
	}
	group.Wait()
	if inserted.Load() != 1 {
		t.Fatalf("wanted exactly one commit, got %d", inserted.Load())
	}
}
