package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type HTTPServer struct {
	Agent ports.NodeAgent
}

func (s HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Agent == nil {
		writeError(w, http.StatusInternalServerError, "node agent is not configured")
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/snapshot":
		s.snapshot(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/load":
		s.load(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/unload":
		s.unload(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/inspect":
		s.inspect(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/begin-request":
		s.beginRequest(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/end-request":
		s.endRequest(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s HTTPServer) snapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.Agent.Snapshot(r.Context())
	writeJSON(w, snap, err)
}

func (s HTTPServer) load(w http.ResponseWriter, r *http.Request) {
	var preset domain.Preset
	if err := json.NewDecoder(r.Body).Decode(&preset); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	inst, err := s.Agent.Load(r.Context(), preset)
	if errors.Is(err, domain.ErrNoFit) {
		writeError(w, http.StatusTooManyRequests, err.Error())
		return
	}
	writeJSON(w, inst, err)
}

func (s HTTPServer) unload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		InstanceID string `json:"instance_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "instance_id is required")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Agent.Unload(r.Context(), req.InstanceID))
}

func (s HTTPServer) inspect(w http.ResponseWriter, r *http.Request) {
	var preset domain.Preset
	if err := json.NewDecoder(r.Body).Decode(&preset); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	metadata, err := s.Agent.InspectModel(r.Context(), preset)
	writeJSON(w, metadata, err)
}

func (s HTTPServer) beginRequest(w http.ResponseWriter, r *http.Request) {
	instanceID, ok := decodeInstanceID(w, r)
	if !ok {
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Agent.BeginRequest(r.Context(), instanceID))
}

func (s HTTPServer) endRequest(w http.ResponseWriter, r *http.Request) {
	instanceID, ok := decodeInstanceID(w, r)
	if !ok {
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Agent.EndRequest(r.Context(), instanceID))
}

func decodeInstanceID(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req struct {
		InstanceID string `json:"instance_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	if req.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "instance_id is required")
		return "", false
	}
	return req.InstanceID, true
}

type HTTPClient struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{BaseURL: strings.TrimRight(baseURL, "/"), Client: http.DefaultClient}
}

func (c *HTTPClient) Snapshot(ctx context.Context) (domain.NodeSnapshot, error) {
	var snap domain.NodeSnapshot
	err := c.do(ctx, http.MethodGet, "/snapshot", nil, &snap)
	return snap, err
}

func (c *HTTPClient) Load(ctx context.Context, p domain.Preset) (domain.ModelInstance, error) {
	var inst domain.ModelInstance
	err := c.do(ctx, http.MethodPost, "/load", p, &inst)
	return inst, err
}

func (c *HTTPClient) Unload(ctx context.Context, instanceID string) error {
	return c.do(ctx, http.MethodPost, "/unload", map[string]string{"instance_id": instanceID}, nil)
}

func (c *HTTPClient) InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error) {
	var metadata domain.ModelMetadata
	err := c.do(ctx, http.MethodPost, "/inspect", p, &metadata)
	return metadata, err
}

func (c *HTTPClient) BeginRequest(ctx context.Context, instanceID string) error {
	return c.do(ctx, http.MethodPost, "/begin-request", map[string]string{"instance_id": instanceID}, nil)
}

func (c *HTTPClient) EndRequest(ctx context.Context, instanceID string) error {
	return c.do(ctx, http.MethodPost, "/end-request", map[string]string{"instance_id": instanceID}, nil)
}

func (c *HTTPClient) do(ctx context.Context, method, path string, in, out any) error {
	var body *bytes.Reader
	if in == nil {
		body = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e wireError
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return fmt.Errorf("node http %s: %s", path, resp.Status)
		}
		return fmt.Errorf("node http %s: %s", path, e.Error)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type wireError struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, v any, err error) {
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		panic(err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(wireError{Error: msg})
}

var _ ports.NodeAgent = (*HTTPClient)(nil)
