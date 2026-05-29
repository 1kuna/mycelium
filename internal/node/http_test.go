package node

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestHTTPNodeAgentRoundTrip(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	server := httptest.NewServer(HTTPServer{Agent: agent})
	defer server.Close()
	client := NewHTTPClient(server.URL)

	snap, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Node.ID != "node_test" {
		t.Fatalf("snapshot = %+v", snap)
	}
	preset := fixtures.MakePreset()
	inst, err := client.Load(context.Background(), preset)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if inst.ID == "" || inst.PresetID != preset.ID {
		t.Fatalf("instance = %+v", inst)
	}
	metadata, err := client.InspectModel(context.Background(), preset)
	if err != nil {
		t.Fatalf("InspectModel: %v", err)
	}
	if metadata.ModelRef != preset.ModelRef {
		t.Fatalf("metadata = %+v", metadata)
	}
	if err := client.BeginRequest(context.Background(), inst.ID); err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	if err := client.EndRequest(context.Background(), inst.ID); err != nil {
		t.Fatalf("EndRequest: %v", err)
	}
	if err := client.Unload(context.Background(), inst.ID); err != nil {
		t.Fatalf("Unload: %v", err)
	}
}

func TestHTTPServerRejectsBadRequests(t *testing.T) {
	server := httptest.NewServer(HTTPServer{Agent: &failingNodeAgent{}})
	defer server.Close()

	resp, err := http.Post(server.URL+"/load", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("bad load post: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad load status = %s", resp.Status)
	}
	_ = resp.Body.Close()

	client := NewHTTPClient(server.URL)
	if _, err := client.Snapshot(context.Background()); err == nil || !strings.Contains(err.Error(), "snapshot failed") {
		t.Fatalf("snapshot error = %v", err)
	}
	if err := client.Unload(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "instance_id") {
		t.Fatalf("unload error = %v", err)
	}
	if err := client.BeginRequest(context.Background(), "inst-a"); err == nil || !strings.Contains(err.Error(), "begin failed") {
		t.Fatalf("begin error = %v", err)
	}
	if err := client.EndRequest(context.Background(), "inst-a"); err == nil || !strings.Contains(err.Error(), "end failed") {
		t.Fatalf("end error = %v", err)
	}
	resp, err = http.Post(server.URL+"/begin-request", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("bad begin post: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad begin status = %s", resp.Status)
	}
	_ = resp.Body.Close()
	if _, err := client.InspectModel(context.Background(), fixtures.MakePreset()); err == nil || !strings.Contains(err.Error(), "inspect failed") {
		t.Fatalf("inspect error = %v", err)
	}
}

func TestHTTPServerHandlesMissingAgentAndNotFound(t *testing.T) {
	server := httptest.NewServer(HTTPServer{})
	defer server.Close()

	resp, err := http.Get(server.URL + "/snapshot")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("missing agent status = %s", resp.Status)
	}
	_ = resp.Body.Close()

	server.Config.Handler = HTTPServer{Agent: mocks.NewNodeAgent(fixtures.MakeNode())}
	resp, err = http.Get(server.URL + "/missing")
	if err != nil {
		t.Fatalf("missing: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("not found status = %s", resp.Status)
	}
	_ = resp.Body.Close()
}

func TestHTTPServerShedsNoFitAsTooManyRequests(t *testing.T) {
	server := httptest.NewServer(HTTPServer{Agent: &failingNodeAgent{loadErr: domain.ErrNoFit}})
	defer server.Close()

	resp, err := http.Post(server.URL+"/load", "application/json", strings.NewReader(`{"id":"preset"}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %s", resp.Status)
	}
}

type failingNodeAgent struct {
	loadErr error
}

func (f *failingNodeAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{}, errors.New("snapshot failed")
}

func (f *failingNodeAgent) Load(context.Context, domain.Preset) (domain.ModelInstance, error) {
	if f.loadErr != nil {
		return domain.ModelInstance{}, f.loadErr
	}
	return domain.ModelInstance{}, errors.New("load failed")
}

func (f *failingNodeAgent) Unload(context.Context, string) error {
	return errors.New("unload failed")
}

func (f *failingNodeAgent) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, errors.New("inspect failed")
}

func (f *failingNodeAgent) BeginRequest(context.Context, string) error {
	return errors.New("begin failed")
}

func (f *failingNodeAgent) EndRequest(context.Context, string) error {
	return errors.New("end failed")
}

var _ ports.NodeAgent = (*failingNodeAgent)(nil)
