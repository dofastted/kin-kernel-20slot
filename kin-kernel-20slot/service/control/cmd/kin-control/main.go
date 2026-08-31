package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kin.local/kin-control/internal/api"
	"kin.local/kin-control/internal/reconcile"
	"kin.local/kin-control/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := envString("KIN_CONTROL_ADDR", "0.0.0.0:9090")
	heartbeatTimeout := envDurationSeconds("KIN_HEARTBEAT_TIMEOUT_SECONDS", 20*time.Second)
	reconcileInterval := envDurationSeconds("KIN_RECONCILE_INTERVAL_SECONDS", 5*time.Second)
	snapshotTTL := envDurationSeconds("KIN_SNAPSHOT_TTL_SECONDS", time.Hour)
	internalToken := strings.TrimSpace(os.Getenv("KIN_CONTROL_INTERNAL_TOKEN"))
	if internalToken == "" {
		logger.Warn("KIN_CONTROL_INTERNAL_TOKEN is empty; /api/v1 stays unauthenticated for legacy demo stacks")
	}
	dbPath := strings.TrimSpace(os.Getenv("KIN_DB_PATH"))
	if dbPath == "" {
		dbPath = filepath.Join(envString("KIN_DATA_DIR", "data"), "kin.db")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o750); err != nil {
		logger.Error("control database directory initialization failed", "error", err)
		os.Exit(1)
	}
	controlStore, err := store.OpenSQLite(dbPath, os.Getenv("KIN_DB_SECRET"))
	if err != nil {
		logger.Error("control database initialization failed", "error", err)
		os.Exit(1)
	}
	defer controlStore.Close()
	reconciler := reconcile.New(controlStore.Observed(), heartbeatTimeout, logger)
	handler := api.New(controlStore, reconciler, snapshotTTL, logger, api.Options{InternalToken: internalToken}).Handler()

	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go reconciler.Run(ctx, reconcileInterval)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("control plane shutdown failed", "error", err)
		}
	}()

	logger.Info("kin-control listening", "address", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("control plane stopped", "error", err)
		os.Exit(1)
	}
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDurationSeconds(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}
