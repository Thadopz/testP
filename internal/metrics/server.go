package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

func RunServer(ctx context.Context, addr string, handler http.Handler) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return RunServerOnListener(ctx, listener, handler)
}

func RunServerOnListener(ctx context.Context, listener net.Listener, handler http.Handler) error {
	if handler == nil {
		listener.Close()
		return fmt.Errorf("metrics handler is required")
	}

	server := &http.Server{
		Handler: handler,
	}
	errCh := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		err := <-errCh
		if err != nil {
			return err
		}
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
