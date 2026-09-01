package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkerPackageBuilds(t *testing.T) {
	if err := validateURL("https://api.example.test/v1/fixture", []string{"api.example.test"}); err != nil {
		t.Fatalf("expected allowlisted HTTPS URL: %v", err)
	}
	if err := validateURL("http://api.example.test/v1/fixture", []string{"api.example.test"}); err == nil {
		t.Fatal("expected HTTP URL rejection")
	}
	if err := validateURL("https://attacker.test", []string{"api.example.test"}); err == nil {
		t.Fatal("expected non-allowlisted host rejection")
	}
}

func TestOpenAICompatibleProviderContract(t *testing.T) {
	const credential = "synthetic-provider-key-never-persisted"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" || request.Header.Get("Authorization") != "Bearer "+credential {
			http.Error(response, "bad contract", http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"choices":[{"message":{"content":"synthetic response"}}]}`))
	}))
	defer server.Close()
	if err := callOpenAICompatible(context.Background(), server.URL, "synthetic-model", credential, "synthetic prompt"); err != nil {
		t.Fatalf("provider contract failed: %v", err)
	}

	malformed := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) { _, _ = response.Write([]byte(`{"choices":[]}`)) }))
	defer malformed.Close()
	if err := callOpenAICompatible(context.Background(), malformed.URL, "synthetic-model", credential, "synthetic prompt"); err == nil {
		t.Fatal("malformed provider response must fail closed")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := callOpenAICompatible(cancelled, server.URL, "synthetic-model", credential, "synthetic prompt"); err == nil {
		t.Fatal("cancelled provider request must not succeed")
	}
}

func TestWorkflowSchemaCompatibility(t *testing.T) {
	if !supportedWorkflowSchema("") || !supportedWorkflowSchema("1") {
		t.Fatal("v0.1 and schema 1 workflows must remain executable")
	}
	if supportedWorkflowSchema("2") || supportedWorkflowSchema("future") {
		t.Fatal("unknown workflow schemas must fail closed")
	}
}

func TestProviderResponseValidation(t *testing.T) {
	if err := validateProviderResponse(strings.NewReader(`{"choices":[{"message":{"content":"synthetic result"}}]}`)); err != nil {
		t.Fatalf("expected compatible provider response: %v", err)
	}
	for _, payload := range []string{"{}", `{"choices":[]}`, `{"choices":[{"message":{"content":""}}]}`, "not-json"} {
		if err := validateProviderResponse(strings.NewReader(payload)); err == nil {
			t.Fatalf("expected malformed provider response rejection: %q", payload)
		}
	}
}

func TestWorkerDrainTimeoutBounds(t *testing.T) {
	t.Setenv("DAR_WORKER_DRAIN_TIMEOUT", "3s")
	if got := workerDrainTimeout(); got != 3*time.Second {
		t.Fatalf("unexpected drain timeout: %v", got)
	}
	t.Setenv("DAR_WORKER_DRAIN_TIMEOUT", "500ms")
	if got := workerDrainTimeout(); got != 25*time.Second {
		t.Fatalf("unsafe drain timeout should fall back: %v", got)
	}
}

func TestQueueRunIDValidation(t *testing.T) {
	if !isUUID("00000000-0000-4000-8000-000000000001") {
		t.Fatal("expected valid UUID")
	}
	for _, value := range []string{"", "not-a-uuid", "00000000-0000-4000-8000-00000000000z", "00000000_0000-4000-8000-000000000001"} {
		if isUUID(value) {
			t.Fatalf("expected invalid queue run ID: %q", value)
		}
	}
}
