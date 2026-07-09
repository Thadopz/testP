package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestRunServerOnListenerServesHandlerAndStopsWithContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunServerOnListener(ctx, listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("metrics ok"))
		}))
	}()

	response, err := http.Get("http://" + listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if !strings.Contains(string(bodyBytes), "metrics ok") {
		t.Fatalf("response body mismatch: got %q", string(bodyBytes))
	}

	cancel()
	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunServerOnListener error mismatch: got %v, want context.Canceled", err)
	}
}
