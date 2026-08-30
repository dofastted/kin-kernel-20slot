package api

import (
	"encoding/json"
	"net/http"

	controlconfig "kin.local/kin-control/internal/config"
	"kin.local/kin-control/internal/store"
)

func (s *Server) getProxyPoolConfig(w http.ResponseWriter, r *http.Request) {
	value, err := s.config.GetProxyPool(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) putProxyPoolConfig(w http.ResponseWriter, r *http.Request) {
	var input typedConfigPutRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := s.config.PutProxyPool(r.Context(), controlconfig.PutInput{
		ExpectedRevision: input.ExpectedRevision, Data: input.Data,
		UpdatedBy: requestOperator(r), Import: input.Import,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) listProxies(w http.ResponseWriter, r *http.Request) {
	value, err := s.config.ListProxies(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) allocateProxy(w http.ResponseWriter, r *http.Request) {
	var input struct {
		VMID string `json:"vm_id"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if input.VMID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "vm_id required")
		return
	}
	proxy, workers, err := s.config.AllocateProxy(r.Context(), input.VMID, requestOperator(r))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proxy": proxy, "workers": workers})
}

type importProxiesRequest struct {
	Items  []importProxyItem `json:"items"`
	Import bool              `json:"import,omitempty"`
}

type importProxyItem struct {
	ID       string  `json:"id,omitempty"`
	Host     string  `json:"host"`
	Port     int     `json:"port"`
	Username *string `json:"username,omitempty"`
	Password *string `json:"password,omitempty"`
	Enabled  *bool   `json:"enabled,omitempty"`
}

func (s *Server) importProxies(w http.ResponseWriter, r *http.Request) {
	var input importProxiesRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	records := make([]store.PutProxyInput, 0, len(input.Items))
	for _, item := range input.Items {
		enabled := true
		if item.Enabled != nil {
			enabled = *item.Enabled
		}
		records = append(records, store.PutProxyInput{
			ID: item.ID, Scheme: "socks5", Host: item.Host, Port: item.Port,
			Username: item.Username, Password: item.Password, Enabled: enabled, Import: input.Import,
		})
	}
	added, skipped, err := s.config.ImportProxies(r.Context(), records, requestOperator(r), input.Import)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"added": len(added), "skipped": len(skipped), "items": added, "skip_details": skipped})
}

type putProxyRequest struct {
	ExpectedRevision uint64          `json:"expected_revision"`
	Data             json.RawMessage `json:"data"`
	Import           bool            `json:"import,omitempty"`
}

type proxyPatch struct {
	Host      *string `json:"host,omitempty"`
	Port      *int    `json:"port,omitempty"`
	Username  *string `json:"username,omitempty"`
	Password  *string `json:"password,omitempty"`
	Enabled   *bool   `json:"enabled,omitempty"`
	ClearAuth bool    `json:"clear_auth,omitempty"`
}

func (s *Server) putProxy(w http.ResponseWriter, r *http.Request) {
	var input putProxyRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var patch proxyPatch
	if len(input.Data) > 0 {
		if err := json.Unmarshal(input.Data, &patch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	current, err := s.config.GetProxyRecord(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	host := current.Host
	port := current.Port
	enabled := current.Enabled
	if patch.Host != nil {
		host = *patch.Host
	}
	if patch.Port != nil {
		port = *patch.Port
	}
	if patch.Enabled != nil {
		enabled = *patch.Enabled
	}
	proxy, workers, err := s.config.PutProxy(r.Context(), store.PutProxyInput{
		ID: current.ID, ExpectedRevision: input.ExpectedRevision, Scheme: "socks5",
		Host: host, Port: port, Enabled: enabled, BoundVMIDs: current.BoundVMIDs,
		Username: patch.Username, Password: patch.Password, ClearAuth: patch.ClearAuth,
		UpdatedBy: requestOperator(r), Import: input.Import,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proxy": proxy, "workers": workers})
}

func (s *Server) deleteProxy(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ExpectedRevision uint64 `json:"expected_revision"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := s.config.DeleteProxy(r.Context(), r.PathValue("id"), input.ExpectedRevision); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) revealProxy(w http.ResponseWriter, r *http.Request) {
	uri, err := s.config.RevealProxy(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.Header().Set("cache-control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"id": r.PathValue("id"), "uri": uri})
}

type proxyBindRequest struct {
	ExpectedRevision uint64 `json:"expected_revision"`
	VMID             string `json:"vm_id"`
}

func (s *Server) bindProxy(w http.ResponseWriter, r *http.Request) {
	var input proxyBindRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	proxy, workers, err := s.config.BindProxy(r.Context(), r.PathValue("id"), input.VMID, input.ExpectedRevision, requestOperator(r))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proxy": proxy, "workers": workers})
}

func (s *Server) unbindProxy(w http.ResponseWriter, r *http.Request) {
	var input proxyBindRequest
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	proxy, workers, err := s.config.UnbindProxy(r.Context(), r.PathValue("id"), input.VMID, input.ExpectedRevision, requestOperator(r))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proxy": proxy, "workers": workers})
}

func (s *Server) probeProxy(w http.ResponseWriter, r *http.Request) {
	proxy, probe, err := s.config.ProbeProxy(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proxy": proxy, "probe": probe})
}

func (s *Server) probeAllProxies(w http.ResponseWriter, r *http.Request) {
	result, err := s.config.ProbeAllProxies(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) enableProxy(w http.ResponseWriter, r *http.Request) {
	s.setProxyEnabled(w, r, true)
}

func (s *Server) disableProxy(w http.ResponseWriter, r *http.Request) {
	s.setProxyEnabled(w, r, false)
}

func (s *Server) setProxyEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	current, err := s.config.GetProxyRecord(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	proxy, workers, err := s.config.PutProxy(r.Context(), store.PutProxyInput{
		ID: current.ID, ExpectedRevision: current.Revision, Scheme: current.Scheme,
		Host: current.Host, Port: current.Port, Enabled: enabled, BoundVMIDs: current.BoundVMIDs,
		UpdatedBy: requestOperator(r),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proxy": proxy, "workers": workers})
}
