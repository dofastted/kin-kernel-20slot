package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"kin.local/kin-control/internal/broker"
	controlconfig "kin.local/kin-control/internal/config"
	"kin.local/kin-control/internal/model"
	"kin.local/kin-control/internal/reconcile"
	"kin.local/kin-control/internal/store"
)

type Server struct {
	store         *store.SQLite
	reconciler    *reconcile.Reconciler
	snapshotTTL   time.Duration
	logger        *slog.Logger
	config        *controlconfig.Service
	mux           *http.ServeMux
	refresher     *broker.Refresher
	internalToken string
}

type Options struct {
	InternalToken string
}

func New(controlStore *store.SQLite, reconciler *reconcile.Reconciler, snapshotTTL time.Duration, logger *slog.Logger, options Options) *Server {
	server := &Server{
		store:         controlStore,
		reconciler:    reconciler,
		snapshotTTL:   snapshotTTL,
		logger:        logger,
		mux:           http.NewServeMux(),
		refresher:     &broker.Refresher{RequireSOCKS5: true},
		config:        controlconfig.NewService(controlStore),
		internalToken: options.InternalToken,
	}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return requestLog(s.logger, internalAuth(s.internalToken, s.mux))
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
	s.mux.HandleFunc("PUT /api/v1/runtime-profile", s.putRuntimeProfile)
	s.mux.HandleFunc("GET /api/v1/runtime-profile", s.getRuntimeProfile)
	s.mux.HandleFunc("GET /api/v1/config/routing", s.getRoutingConfig)
	s.mux.HandleFunc("PUT /api/v1/config/routing", s.putRoutingConfig)
	s.mux.HandleFunc("GET /api/v1/config/model-policy", s.getModelPolicyConfig)
	s.mux.HandleFunc("PUT /api/v1/config/model-policy", s.putModelPolicyConfig)
	s.mux.HandleFunc("GET /api/v1/slots/{id}", s.getSlotConfig)
	s.mux.HandleFunc("PATCH /api/v1/slots/{id}", s.patchSlotConfig)
	s.mux.HandleFunc("POST /api/v1/slots/policy", s.putSlotPolicy)
	s.mux.HandleFunc("GET /api/v1/config/proxy-pool", s.getProxyPoolConfig)
	s.mux.HandleFunc("PUT /api/v1/config/proxy-pool", s.putProxyPoolConfig)
	s.mux.HandleFunc("GET /api/v1/proxies", s.listProxies)
	s.mux.HandleFunc("POST /api/v1/proxies/import", s.importProxies)
	s.mux.HandleFunc("POST /api/v1/proxies/allocate", s.allocateProxy)
	s.mux.HandleFunc("POST /api/v1/proxies/probe", s.probeAllProxies)
	s.mux.HandleFunc("PUT /api/v1/proxies/{id}", s.putProxy)
	s.mux.HandleFunc("DELETE /api/v1/proxies/{id}", s.deleteProxy)
	s.mux.HandleFunc("POST /api/v1/proxies/{id}/reveal", s.revealProxy)
	s.mux.HandleFunc("POST /api/v1/proxies/{id}/bind", s.bindProxy)
	s.mux.HandleFunc("POST /api/v1/proxies/{id}/unbind", s.unbindProxy)
	s.mux.HandleFunc("POST /api/v1/proxies/{id}/probe", s.probeProxy)
	s.mux.HandleFunc("POST /api/v1/proxies/{id}/enable", s.enableProxy)
	s.mux.HandleFunc("POST /api/v1/proxies/{id}/disable", s.disableProxy)
	s.mux.HandleFunc("GET /api/v1/config/documents/{domain...}", s.getDocument)
	s.mux.HandleFunc("PUT /api/v1/config/documents/{domain...}", s.putDocument)
	s.mux.HandleFunc("GET /api/v1/migration/domains/{domain}", s.getDomainOwner)
	s.mux.HandleFunc("PUT /api/v1/migration/domains/{domain}", s.putDomainOwner)
	s.mux.HandleFunc("POST /api/v1/operations", s.enqueueOperation)
	s.mux.HandleFunc("POST /api/v1/operations/claim", s.claimOperation)
	s.mux.HandleFunc("GET /api/v1/operations/{id}", s.getOperation)
	s.mux.HandleFunc("POST /api/v1/operations/{id}/complete", s.completeOperation)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	health := s.store.Health(r.Context())
	writeJSON(w, http.StatusOK, health)
}

func (s *Server) listKernels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"kernels": s.store.Observed().ListKernels()})
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
	kernel := s.store.Observed().UpsertKernel(input, time.Now())
	writeJSON(w, http.StatusOK, kernel)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Observed().Heartbeat(r.PathValue("id"), time.Now()); err != nil {
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
	if err := s.store.Observed().SetDraining(r.PathValue("id"), true); err != nil {
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
		writeStoreError(w, err)
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
	saved, err := s.store.PutPolicy(policy)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	revision, err := s.store.CurrentRevision(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	policies, err := s.store.ListPolicies()
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, model.Snapshot{
		Revision:  revision,
		IssuedAt:  now,
		ExpiresAt: now.Add(s.snapshotTTL),
		Kernels:   s.store.Observed().ListKernels(),
		Policies:  policies,
		Demo:      false,
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

func (s *Server) putRuntimeProfile(w http.ResponseWriter, r *http.Request) {
	var profile model.RuntimeProfile
	if err := decodeJSON(w, r, &profile); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := validateRuntimeProfile(profile); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	hash, err := profile.ConfigHash()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to compute config_hash")
		return
	}
	if err := s.store.SetRuntimeProfile(profile); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": profile, "config_hash": hash})
}

func (s *Server) getRuntimeProfile(w http.ResponseWriter, _ *http.Request) {
	profile, ok, err := s.store.GetRuntimeProfile()
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "runtime profile not set")
		return
	}
	hash, err := profile.ConfigHash()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to compute config_hash")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": profile, "config_hash": hash})
}

type typedConfigPutRequest struct {
	ExpectedRevision uint64          `json:"expected_revision"`
	Data             json.RawMessage `json:"data"`
	Import           bool            `json:"import,omitempty"`
}

func (s *Server) getRoutingConfig(w http.ResponseWriter, r *http.Request) {
	value, err := s.config.GetRouting(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) putRoutingConfig(w http.ResponseWriter, r *http.Request) {
	var input typedConfigPutRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := s.config.PutRouting(r.Context(), controlconfig.PutInput{
		ExpectedRevision: input.ExpectedRevision, Data: input.Data,
		UpdatedBy: requestOperator(r), Import: input.Import,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) getModelPolicyConfig(w http.ResponseWriter, r *http.Request) {
	value, err := s.config.GetModelPolicy(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) putModelPolicyConfig(w http.ResponseWriter, r *http.Request) {
	var input typedConfigPutRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := s.config.PutModelPolicy(r.Context(), controlconfig.PutInput{
		ExpectedRevision: input.ExpectedRevision, Data: input.Data,
		UpdatedBy: requestOperator(r), Import: input.Import,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) getSlotConfig(w http.ResponseWriter, r *http.Request) {
	value, err := s.config.GetSlot(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) patchSlotConfig(w http.ResponseWriter, r *http.Request) {
	var input typedConfigPutRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := s.config.PutSlot(r.Context(), r.PathValue("id"), controlconfig.PutInput{
		ExpectedRevision: input.ExpectedRevision, Data: input.Data,
		UpdatedBy: requestOperator(r), Import: input.Import,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

type batchSlotPolicyRequest struct {
	IDs    []string        `json:"ids"`
	Data   json.RawMessage `json:"data"`
	Import bool            `json:"import,omitempty"`
}

func (s *Server) putSlotPolicy(w http.ResponseWriter, r *http.Request) {
	var input batchSlotPolicyRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := s.config.PutSlots(r.Context(), input.IDs, controlconfig.PutInput{
		Data: input.Data, UpdatedBy: requestOperator(r), Import: input.Import,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

type putDocumentRequest struct {
	SchemaVersion    int             `json:"schema_version"`
	ExpectedRevision uint64          `json:"expected_revision"`
	Data             json.RawMessage `json:"data"`
	ChangeKind       string          `json:"change_kind,omitempty"`
}

func (s *Server) getDocument(w http.ResponseWriter, r *http.Request) {
	document, err := s.store.GetDocument(r.Context(), r.PathValue("domain"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, document)
}

func (s *Server) putDocument(w http.ResponseWriter, r *http.Request) {
	var input putDocumentRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	document, err := s.store.PutDocument(r.Context(), store.PutDocumentInput{
		Domain:           r.PathValue("domain"),
		SchemaVersion:    input.SchemaVersion,
		ExpectedRevision: input.ExpectedRevision,
		Data:             input.Data,
		UpdatedBy:        requestOperator(r),
		ChangeKind:       input.ChangeKind,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, document)
}

type putDomainOwnerRequest struct {
	Owner            string `json:"owner"`
	State            string `json:"state"`
	SourceHash       string `json:"source_hash,omitempty"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

func (s *Server) getDomainOwner(w http.ResponseWriter, r *http.Request) {
	owner, err := s.store.GetDomainOwner(r.Context(), r.PathValue("domain"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, owner)
}

func (s *Server) putDomainOwner(w http.ResponseWriter, r *http.Request) {
	var input putDomainOwnerRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	owner, err := s.store.PutDomainOwner(r.Context(), store.PutDomainOwnerInput{
		Domain:           r.PathValue("domain"),
		Owner:            input.Owner,
		State:            input.State,
		SourceHash:       input.SourceHash,
		ExpectedRevision: input.ExpectedRevision,
		UpdatedBy:        requestOperator(r),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, owner)
}

type enqueueOperationRequest struct {
	Kind           string          `json:"kind"`
	Target         string          `json:"target"`
	Payload        json.RawMessage `json:"payload"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (s *Server) enqueueOperation(w http.ResponseWriter, r *http.Request) {
	var input enqueueOperationRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	operation, err := s.store.EnqueueOperation(r.Context(), store.EnqueueOperationInput{
		Kind:           input.Kind,
		Target:         input.Target,
		Payload:        input.Payload,
		MaxAttempts:    input.MaxAttempts,
		IdempotencyKey: input.IdempotencyKey,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, operation)
}

type claimOperationRequest struct {
	Worker       string `json:"worker"`
	LeaseSeconds int    `json:"lease_seconds"`
}

func (s *Server) claimOperation(w http.ResponseWriter, r *http.Request) {
	var input claimOperationRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	operation, err := s.store.ClaimOperation(r.Context(), input.Worker, time.Duration(input.LeaseSeconds)*time.Second)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	operation, err := s.store.GetOperation(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

type completeOperationRequest struct {
	Worker    string          `json:"worker"`
	Status    string          `json:"status"`
	Result    json.RawMessage `json:"result,omitempty"`
	LastError string          `json:"last_error,omitempty"`
}

func (s *Server) completeOperation(w http.ResponseWriter, r *http.Request) {
	var input completeOperationRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	operation, err := s.store.CompleteOperation(
		r.Context(), r.PathValue("id"), input.Worker, input.Status, input.Result, input.LastError,
	)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func requestOperator(r *http.Request) string {
	operator := strings.TrimSpace(r.Header.Get("x-kin-operator"))
	if operator == "" {
		return "control-api"
	}
	if len(operator) > 128 {
		return operator[:128]
	}
	return operator
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

func validateRuntimeProfile(profile model.RuntimeProfile) error {
	if profile.ExecutionMode == "" {
		return errors.New("execution_mode is required")
	}
	if err := validateExecutionMode(profile.ExecutionMode); err != nil {
		return err
	}
	if profile.SystemLayout == "" {
		return errors.New("system_layout is required")
	}
	if profile.Timezone == "" {
		return errors.New("timezone is required")
	}
	if profile.SlotCount < 1 || profile.SlotCount > 20 {
		return errors.New("slot_count must be between 1 and 20")
	}
	if profile.MaxBodyBytes < 1 {
		return errors.New("max_body_bytes must be positive")
	}
	if profile.MaxOutputTokens < 1 {
		return errors.New("max_output_tokens must be positive")
	}
	return nil
}

// executionMode is the only mode the kernel implements (patch-only
// consolidation). It stays a profile field because `config_hash` is computed
// over the whole RuntimeProfile and compared across console, kernel, and CLI.
const executionMode = "native_messages"

// validateExecutionMode rejects anything the kernel cannot run. mcp_slot and
// native_slot were deleted along with their code paths, so naming them must
// fail here rather than produce a kernel that refuses to boot.
func validateExecutionMode(mode string) error {
	if strings.ToLower(strings.TrimSpace(mode)) == executionMode {
		return nil
	}
	return fmt.Errorf("execution_mode %q must be %s", mode, executionMode)
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

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "control state not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "revision_conflict", err.Error())
	case errors.Is(err, store.ErrInvalid):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, store.ErrSecretUnavailable):
		writeError(w, http.StatusServiceUnavailable, "secret_unavailable", "database secret is not configured")
	case errors.Is(err, store.ErrCorrupt):
		writeError(w, http.StatusInternalServerError, "control_state_corrupt", "control state is corrupt")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "control state operation failed")
	}
}

func internalAuth(expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		provided := ""
		if value := r.Header.Get("authorization"); strings.HasPrefix(value, "Bearer ") {
			provided = strings.TrimPrefix(value, "Bearer ")
		}
		expectedHash := sha256.Sum256([]byte(expected))
		providedHash := sha256.Sum256([]byte(provided))
		if expected == "" || subtle.ConstantTimeCompare(expectedHash[:], providedHash[:]) != 1 {
			writeError(w, http.StatusUnauthorized, "internal_auth_failed", "valid internal bearer token required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requestLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}
