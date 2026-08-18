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
