package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"kin.local/kin-control/internal/broker"
	"kin.local/kin-control/internal/model"
	"kin.local/kin-control/internal/reconcile"
	"kin.local/kin-control/internal/store"
)

type Server struct {
	store       *store.Memory
	reconciler  *reconcile.Reconciler
	snapshotTTL time.Duration
	logger      *slog.Logger
	mux         *http.ServeMux
	refresher   *broker.Refresher
}

func New(memoryStore *store.Memory, reconciler *reconcile.Reconciler, snapshotTTL time.Duration, logger *slog.Logger) *Server {
	server := &Server{
		store:       memoryStore,
		reconciler:  reconciler,
		snapshotTTL: snapshotTTL,
		logger:      logger,
		mux:         http.NewServeMux(),
		refresher:   &broker.Refresher{RequireSOCKS5: true},
	}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return requestLog(s.logger, s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /api/v1/kernels", s.listKernels)
	s.mux.HandleFunc("POST /api/v1/kernels", s.registerKernel)
	s.mux.HandleFunc("POST /api/v1/kernels/{id}/heartbeat", s.heartbeat)
	s.mux.HandleFunc("POST /api/v1/kernels/{id}/drain", s.drain)
	s.mux.HandleFunc("GET /api/v1/route-policies/{name}", s.getPolicy)
	s.mux.HandleFunc("PUT /api/v1/route-policies/{name}", s.putPolicy)
	s.mux.HandleFunc("GET /api/v1/snapshots/current", s.snapshot)
	s.mux.HandleFunc("POST /api/v1/reconcile", s.reconcileNow)
	s.mux.HandleFunc("POST /api/v1/credentials/exchange", s.exchange)
	s.mux.HandleFunc("POST /api/v1/credentials/refresh", s.refresh)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) listKernels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"kernels": s.store.ListKernels()})
}

func (s *Server) registerKernel(w http.ResponseWriter, r *http.Request) {
	var input model.KernelRegistration
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := validateRegistration(input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	kernel := s.store.UpsertKernel(input, time.Now())
	writeJSON(w, http.StatusOK, kernel)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Heartbeat(r.PathValue("id"), time.Now()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "kernel not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "heartbeat failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) drain(w http.ResponseWriter, r *http.Request) {
	if err := s.store.SetDraining(r.PathValue("id"), true); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "kernel not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "drain failed")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "draining"})
}

func (s *Server) getPolicy(w http.ResponseWriter, r *http.Request) {
	policy, err := s.store.GetPolicy(r.PathValue("name"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "route policy not found")
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) putPolicy(w http.ResponseWriter, r *http.Request) {
	var policy model.RoutePolicy
	if err := decodeJSON(w, r, &policy); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	name := r.PathValue("name")
	if policy.Name == "" {
		policy.Name = name
	}
	if policy.Name != name {
		writeError(w, http.StatusBadRequest, "invalid_request", "path and body policy names differ")
		return
	}
	if err := validatePolicy(policy); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.store.PutPolicy(policy))
}

func (s *Server) snapshot(w http.ResponseWriter, _ *http.Request) {
	now := time.Now().UTC()
	writeJSON(w, http.StatusOK, model.Snapshot{
		Revision:  s.store.Revision(),
		IssuedAt:  now,
		ExpiresAt: now.Add(s.snapshotTTL),
		Kernels:   s.store.ListKernels(),
		Policies:  s.store.ListPolicies(),
		Demo:      true,
	})
}

func (s *Server) reconcileNow(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.reconciler.Reconcile(time.Now()))
}

func (s *Server) exchange(w http.ResponseWriter, r *http.Request) {
	var req broker.ExchangeRequest
	if err := decodeJSONAllowUnknown(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if broker.SessionKeyPresent(req) {
		writeJSON(w, http.StatusGone, broker.RejectSessionKey())
		return
	}
	writeError(w, http.StatusGone, "rejected_interchange", "cookie/authorize interchange is not implemented; POST /api/v1/credentials/refresh with a /login refresh_token and socks5")
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var req broker.ExchangeRequest
	if err := decodeJSONAllowUnknown(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if broker.SessionKeyPresent(req) {
		writeJSON(w, http.StatusGone, broker.RejectSessionKey())
		return
	}
	result, err := s.refresher.Refresh(r.Context(), req.RefreshToken, req.Scopes, req.Socks5)
	if err != nil {
		writeError(w, http.StatusBadRequest, "refresh_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeJSONAllowUnknown(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func validateRegistration(input model.KernelRegistration) error {
	if !validName(input.ID) {
		return errors.New("id must use lowercase letters, digits, dots, dashes, or underscores")
	}
	if input.Address == "" || len(input.Address) > 512 {
		return errors.New("address is required and must be at most 512 characters")
	}
	if input.Capacity < 1 || input.Capacity > 100000 {
		return errors.New("capacity must be between 1 and 100000")
	}
	if !validName(input.Provider) {
		return errors.New("provider is invalid")
	}
	return nil
}

func validatePolicy(policy model.RoutePolicy) error {
	if !validName(policy.Name) || policy.Tenant == "" || policy.ModelPattern == "" {
		return errors.New("name, tenant, and model_pattern are required")
	}
	if !validName(policy.Provider) || !validName(policy.SlotGroup) {
		return errors.New("provider or slot_group is invalid")
	}
	if policy.MaxInflight < 1 || policy.MaxWaitingTool < 0 {
		return errors.New("max_inflight must be positive and max_waiting_tool non-negative")
	}
	return nil
}

func validName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || strings.ContainsRune("-._", char) {
			continue
		}
		return false
	}
	return true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"type": "error",
		"error": map[string]any{
			"code":      code,
			"message":   message,
			"retryable": status >= 500,
		},
	})
}

func requestLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}
