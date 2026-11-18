package main

import (
	"context"
	"flag"
	"fmt"
	"lamina/internal/engine"
	"lamina/internal/snapshot"
	"lamina/internal/transport"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("lamina stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("data", "var/lamina", "data directory exclusively owned by this process")
	addr := flag.String("listen", "127.0.0.1:7386", "HTTP bind address")
	restore := flag.String("restore", "", "validated backup to install into a new data directory before serving")
	commitDwell := flag.Duration("commit-dwell", 2*time.Millisecond, "max wait for commits to share one fsync (bounded group-commit latency)")
	commitGroup := flag.Int("commit-group", 64, "max records sharing one fsync group")
	commitQueue := flag.Int("commit-queue", 256, "max queued commits held in memory; full queue rejects with 429")
	flag.Parse()
	if *commitDwell <= 0 || *commitGroup <= 0 || *commitQueue <= 0 {
		return fmt.Errorf("commit-dwell, commit-group and commit-queue must be positive")
	}
	if *commitGroup > 1024 || *commitQueue > 4096 {
		return fmt.Errorf("commit-group <= 1024 and commit-queue <= 4096")
	}
	if *restore != "" {
		info, err := os.Stat(*restore)
		if err != nil {
			return err
		}
		if info.Size() > snapshot.MaxImage {
			return fmt.Errorf("backup exceeds limit")
		}
		b, err := os.ReadFile(*restore)
		if err != nil {
			return err
		}
		if err = engine.Restore(*dir, b); err != nil {
			return err
		}
	}
	db, err := engine.OpenWith(*dir, engine.CommitOptions{MaxDwell: *commitDwell, MaxGroup: *commitGroup, QueueDepth: *commitQueue})
	if err != nil {
		return err
	}
	defer db.Close()
	server := &http.Server{Addr: *addr, Handler: transport.New(db), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 45 * time.Second, MaxHeaderBytes: 32 << 10}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stopped)
	finished := make(chan error, 1)
	go func() { slog.Info("lamina ready", "address", *addr); finished <- server.ListenAndServe() }()
	select {
	case err = <-finished:
		if err != http.ErrServerClosed {
			return err
		}
	case <-stopped:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Shutdown(ctx); err != nil {
			server.Close()
			return err
		}
	}
	return nil
}
