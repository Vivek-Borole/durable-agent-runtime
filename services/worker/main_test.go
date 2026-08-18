package main

import "testing"

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
