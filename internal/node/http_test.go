package node

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
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

func TestHTTPInstanceProxyThroughAuthenticatedTunnel(t *testing.T) {
	var backendAuth string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/chat/completions" || r.URL.RawQuery != "probe=1" {
			t.Fatalf("backend url = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `{"proxied":true}`)
	}))
	defer backend.Close()
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithListenAddr(backend.URL), WithAllocator(lease.NewAllocator()))
	remote := httptest.NewServer(HTTPServer{Agent: agent, AuthToken: "rpc-secret"})
	defer remote.Close()
	tunnel := membership.NewLANTunnel()
	tunnel.AuthToken = "rpc-secret"
	loopback, err := tunnel.Open(context.Background(), domain.Node{ID: "node_test", Address: remote.URL})
	if err != nil {
		t.Fatalf("Open tunnel: %v", err)
	}
	defer tunnel.Close(context.Background(), "node_test")
	client := NewHTTPClient("http://" + loopback)
	client.AuthToken = "rpc-secret"

	inst, err := client.Load(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.HasPrefix(inst.Addr, "http://"+loopback+"/instances/") {
		t.Fatalf("proxied addr = %s", inst.Addr)
	}
	snap, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Instances) != 1 || snap.Instances[0].Addr != inst.Addr {
		t.Fatalf("snapshot instances = %+v addr=%s", snap.Instances, inst.Addr)
	}
	resp, err := http.Post(inst.Addr+"/v1/chat/completions?probe=1", "application/json", strings.NewReader(`{"model":"tiny"}`))
	if err != nil {
		t.Fatalf("proxy post: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read proxy body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "proxied") {
		t.Fatalf("proxy response = %s %s", resp.Status, body)
	}
	if backendAuth != "" {
		t.Fatalf("backend saw peer auth header %q", backendAuth)
	}
}

func TestHTTPPeerRPCAuth(t *testing.T) {
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), WithAllocator(lease.NewAllocator()))
	server := httptest.NewServer(HTTPServer{Agent: agent, AuthToken: "rpc-secret"})
	defer server.Close()

	if _, err := NewHTTPClient(server.URL).Snapshot(context.Background()); err == nil || !strings.Contains(err.Error(), "authorization failed") {
		t.Fatalf("unauthenticated snapshot err = %v", err)
	}
	wrong := NewHTTPClient(server.URL)
	wrong.AuthToken = "wrong"
	if _, err := wrong.Snapshot(context.Background()); err == nil || !strings.Contains(err.Error(), "authorization failed") {
		t.Fatalf("wrong token err = %v", err)
	}
	client := NewHTTPClient(server.URL)
	client.AuthToken = "rpc-secret"
	if snap, err := client.Snapshot(context.Background()); err != nil || snap.Node.ID == "" {
		t.Fatalf("authorized snapshot = %+v %v", snap, err)
	}
}

func TestHTTPAdmissionControllerRoundTrip(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	admission := NewAdmission(fixtures.MakeNode(fixtures.WithNodeID("node-http")), lease.NewAllocator(), clock)
	server := httptest.NewServer(HTTPServer{Admission: admission})
	defer server.Close()
	client := NewHTTPClient(server.URL)
	job := fixtures.MakeJob(fixtures.WithJobID("job-http"))
	claim := fixtures.MakeClaim(3, 4)

	offer, err := client.Offer(context.Background(), job, claim)
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}
	if offer.JobID != job.ID || offer.NodeID != "node-http" || offer.Claim != claim {
		t.Fatalf("offer = %+v", offer)
	}
	lease, err := client.Commit(context.Background(), offer.OfferID, offer.Fence)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if lease.JobID != job.ID || lease.NodeID != "node-http" || lease.Claim != claim {
		t.Fatalf("lease = %+v", lease)
	}
	if got, found, err := client.LeaseForJob(context.Background(), job.ID); err != nil || !found || got.ID != lease.ID {
		t.Fatalf("LeaseForJob = %+v %v %v", got, found, err)
	}
	if got, found, err := client.LeaseForJob(context.Background(), "missing"); err != nil || found || got.ID != "" {
		t.Fatalf("missing LeaseForJob = %+v %v %v", got, found, err)
	}
	if err := client.Release(context.Background(), lease.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, found, err := client.LeaseForJob(context.Background(), job.ID); err != nil || found {
		t.Fatalf("released LeaseForJob found=%v err=%v", found, err)
	}

	preemptOffer, err := client.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-preempt")), claim)
	if err != nil {
		t.Fatalf("preempt Offer: %v", err)
	}
	preemptLease, err := client.Commit(context.Background(), preemptOffer.OfferID, preemptOffer.Fence)
	if err != nil {
		t.Fatalf("preempt Commit: %v", err)
	}
	if err := client.Preempt(context.Background(), preemptLease.ID, "test"); err != nil {
		t.Fatalf("Preempt: %v", err)
	}
}

func TestHTTPAdmissionPreservesStaleFenceError(t *testing.T) {
	admission := NewAdmission(fixtures.MakeNode(), lease.NewAllocator(), mocks.NewFakeClock(time.Now()))
	server := httptest.NewServer(HTTPServer{Admission: admission})
	defer server.Close()
	client := NewHTTPClient(server.URL)

	first, err := client.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), fixtures.MakeClaim(1, 1))
	if err != nil {
		t.Fatalf("first Offer: %v", err)
	}
	second, err := client.Offer(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-b")), fixtures.MakeClaim(1, 1))
	if err != nil {
		t.Fatalf("second Offer: %v", err)
	}
	if _, err := client.Commit(context.Background(), first.OfferID, first.Fence); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if _, err := client.Commit(context.Background(), second.OfferID, second.Fence); !errors.Is(err, domain.ErrStaleFence) {
		t.Fatalf("stale Commit err = %v", err)
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

	resp, err = http.Post(server.URL+"/admission/offer", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("bad offer post: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("missing admission status = %s", resp.Status)
	}
	_ = resp.Body.Close()

	admissionServer := httptest.NewServer(HTTPServer{Admission: &failingAdmissionController{offerErr: domain.ErrNoFit}})
	defer admissionServer.Close()
	admissionClient := NewHTTPClient(admissionServer.URL)
	if _, err := admissionClient.Offer(context.Background(), fixtures.MakeJob(), fixtures.MakeClaim(1, 1)); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("offer no-fit err = %v", err)
	}
	resp, err = http.Post(admissionServer.URL+"/admission/commit", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatalf("bad commit post: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad commit status = %s", resp.Status)
	}
	_ = resp.Body.Close()
	if _, err := admissionClient.Commit(context.Background(), "", 1); err == nil || !strings.Contains(err.Error(), "offer_id") {
		t.Fatalf("empty commit err = %v", err)
	}
	if err := admissionClient.Release(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "lease_id") {
		t.Fatalf("empty release err = %v", err)
	}
	if err := admissionClient.Preempt(context.Background(), "", "test"); err == nil || !strings.Contains(err.Error(), "lease_id") {
		t.Fatalf("empty preempt err = %v", err)
	}
	resp, err = http.Get(admissionServer.URL + "/admission/lease")
	if err != nil {
		t.Fatalf("empty lease query: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("unsupported lease inspection status = %s", resp.Status)
	}
	_ = resp.Body.Close()
	leaseServer := httptest.NewServer(HTTPServer{Admission: NewAdmission(fixtures.MakeNode(), lease.NewAllocator(), mocks.NewFakeClock(time.Now()))})
	defer leaseServer.Close()
	resp, err = http.Get(leaseServer.URL + "/admission/lease")
	if err != nil {
		t.Fatalf("bad lease query: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad lease query status = %s", resp.Status)
	}
	_ = resp.Body.Close()
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
	client := NewHTTPClient(server.URL)
	if _, err := client.Load(context.Background(), fixtures.MakePreset()); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("client no-fit err = %v", err)
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

type failingAdmissionController struct {
	offerErr   error
	commitErr  error
	releaseErr error
	preemptErr error
}

func (f *failingAdmissionController) Offer(context.Context, domain.Job, domain.Claim) (domain.LeaseOffer, error) {
	if f.offerErr != nil {
		return domain.LeaseOffer{}, f.offerErr
	}
	return domain.LeaseOffer{OfferID: "offer-a", JobID: "job-a", NodeID: "node-a", Fence: 1}, nil
}

func (f *failingAdmissionController) Commit(context.Context, string, uint64) (domain.Lease, error) {
	if f.commitErr != nil {
		return domain.Lease{}, f.commitErr
	}
	return domain.Lease{ID: "lease-a"}, nil
}

func (f *failingAdmissionController) Release(context.Context, string) error {
	return f.releaseErr
}

func (f *failingAdmissionController) Preempt(context.Context, string, string) error {
	return f.preemptErr
}

var _ ports.AdmissionController = (*failingAdmissionController)(nil)
