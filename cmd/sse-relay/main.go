// Command sse-relay runs the HTTP server: flags for the knobs documented in
// the README, RELAY_TOKEN from the environment, and a shutdown sequence that
// finishes every stream before the listener stops accepting drains.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rjjackson1/sse-relay/internal/hub"
	"github.com/rjjackson1/sse-relay/internal/relay"
)

// version is overridden at build time with:
//
//	go build -ldflags "-X main.version=v1.2.3"
//
// Left at "dev" for plain `go build` or `go run`.
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sse-relay:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr            = flag.String("addr", ":8080", "listen address")
		buffer          = flag.Int("buffer", hub.DefaultCapacity, "events kept per stream for replay")
		heartbeat       = flag.Duration("heartbeat", 15*time.Second, "delay between heartbeat comment frames")
		retry           = flag.Duration("retry", 2*time.Second, "reconnect delay advertised in the retry: field")
		shutdownTimeout = flag.Duration("shutdown-timeout", 10*time.Second, "grace period for in-flight requests on shutdown")
		showVersion     = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println("sse-relay", version)
		return nil
	}

	h := hub.New(*buffer)
	srv := relay.NewServer(h, relay.Config{
		Heartbeat: *heartbeat,
		RetryHint: *retry,
		Token:     os.Getenv("RELAY_TOKEN"),
	})

	httpSrv := &http.Server{
		Addr:    *addr,
		Handler: srv,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("sse-relay listening on %s", *addr)
		serveErr <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	log.Print("shutting down: finishing open streams")
	// Finish every stream first, so subscribers see event: done instead of a
	// connection that just dies mid frame, then let the server drain.
	h.CloseAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
