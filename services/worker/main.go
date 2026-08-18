package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// The production worker will consume leased JetStream tasks. Keeping the process
// independently buildable now makes the Go/TypeScript service boundary explicit.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("durable agent worker started", "delivery", "at-least-once", "effects", "idempotent")
	<-ctx.Done()
	slog.Info("durable agent worker stopped")
}

