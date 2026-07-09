package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestDefaultControllerID(t *testing.T) {
	controllerID := defaultControllerID()
	if strings.TrimSpace(controllerID) == "" {
		t.Fatal("expected default controller id to be non-empty")
	}
	if !strings.Contains(controllerID, fmt.Sprintf("%d", currentProcessID())) {
		t.Fatalf("controller id %q should include process id", controllerID)
	}
}

func currentProcessID() int {
	return os.Getpid()
}
