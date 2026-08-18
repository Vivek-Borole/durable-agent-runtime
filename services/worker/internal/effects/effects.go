package effects

import (
	"context"
	"sync"
)

// Key is stable across redelivery and retry. It scopes an external side effect
// to one tenant and never relies on transient task-delivery identifiers.
type Key struct {
	TenantID  string
	RunID     string
	StepIndex int
	EffectKey string
}

type Commit struct {
	Key     Key
	Outcome []byte
}

type Ledger interface {
	Commit(context.Context, Commit) (inserted bool, err error)
}

type MemoryLedger struct {
	mu      sync.Mutex
	commits map[Key][]byte
}

func NewMemoryLedger() *MemoryLedger {
	return &MemoryLedger{commits: make(map[Key][]byte)}
}

func (l *MemoryLedger) Commit(_ context.Context, commit Commit) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.commits[commit.Key]; exists {
		return false, nil
	}
	l.commits[commit.Key] = append([]byte(nil), commit.Outcome...)
	return true, nil
}

