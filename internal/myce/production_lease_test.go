package myce

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Two hosts; only "gpu-host" has the accelerator a production stage needs.
func productionLeaseFleet(t *testing.T) (*Queue, *fakeClock, Machine, Machine) {
	t.Helper()
	q, c, workers := fleetFixture(t, "queue-host", "gpu-host", "other-host")
	gpu, other := workers[0], workers[1]
	gpu.GPU = "cuda"
	gpu.CapabilityProfile.Accelerators[0].Type = "cuda"
	check(t, q, gpu, nil)
	return q, c, gpu, other
}

func cudaStage() ProductionStage {
	return ProductionStage{Needs: ResourceNeeds{MemoryGB: 8, Platform: Platform{Required: []string{"cuda"}}}}
}

func cudaProductionSpec() Spec {
	s := spec("production")
	s.Needs.Platform = Platform{Required: []string{"cuda"}}
	return s
}

func productionWorkOpen(err error) bool {
	var refusal *HTTPError
	return errors.As(err, &refusal) && refusal.Status == http.StatusConflict && refusal.Code == "production_work_open"
}

func TestProductionLeaseRefusesDrainOfOnlyEligibleHostUntilExpiry(t *testing.T) {
	q, c, _, _ := productionLeaseFleet(t)
	mux := newRequestMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	call := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	if w := call("POST", "/api/production-leases", `{"project":"render","key":"run-7","ttl":"0s"}`); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "production_lease_invalid") {
		t.Fatal("non-expiring lease accepted", w.Code, w.Body.String())
	}
	stage, _ := json.Marshal(cudaStage())
	if w := call("POST", "/api/production-leases", `{"project":"render","key":"run-7","ttl":"2h","stages":[`+string(stage)+`]}`); w.Code != http.StatusOK {
		t.Fatal("open lease", w.Code, w.Body.String())
	}
	var listed struct{ Leases []ProductionLease }
	if w := call("GET", "/api/production-leases", ""); json.Unmarshal(w.Body.Bytes(), &listed) != nil || len(listed.Leases) != 1 || !listed.Leases[0].ExpiresAt.Equal(c.Now().Add(2*time.Hour)) {
		t.Fatal("lease not listed", w.Body.String())
	}

	if _, err := q.ChangeHostMaintenance("gpu-host", HostMaintenanceRequest{Action: "drain"}); !productionWorkOpen(err) || !strings.Contains(err.Error(), "run-7") {
		t.Fatal("drain stranded the lease's only host", err)
	}
	if q.maintenanceBlocked("gpu-host") {
		t.Fatal("refused drain changed policy")
	}
	if status, _ := q.HostMaintenanceStatus("gpu-host"); !strings.Contains(status.ProductionBlocker, "project render") {
		t.Fatal("host status hides the blocker", status.ProductionBlocker)
	}
	// Hosts that cannot start the declared stage are unaffected.
	if _, err := q.ChangeHostMaintenance("other-host", HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	// Renewal moves the expiry; the lease still ends on its own.
	c.Advance(time.Hour)
	if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "render", Key: "run-7", TTL: "30m", Stages: []ProductionStage{cudaStage()}}); err != nil {
		t.Fatal(err)
	}
	c.Advance(29 * time.Minute)
	if _, err := q.ChangeHostMaintenance("gpu-host", HostMaintenanceRequest{Action: "drain"}); !productionWorkOpen(err) {
		t.Fatal("renewed lease expired early", err)
	}
	c.Advance(time.Minute)
	if _, err := q.ChangeHostMaintenance("gpu-host", HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal("expired lease still blocks", err)
	}
	fleetTick(t, q)
	if len(q.s.ProductionLeases) != 0 {
		t.Fatal("expired lease retained", q.s.ProductionLeases)
	}
	// Closing is idempotent.
	if w := call("DELETE", "/api/production-leases/render/run-7", ""); w.Code != http.StatusOK {
		t.Fatal("close", w.Code, w.Body.String())
	}
}

func TestProductionLeaseWithoutStagesKeepsOneProjectHostEnabled(t *testing.T) {
	q, _, _, _ := productionLeaseFleet(t)
	if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "render", Key: "batch", TTL: "1h"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance("gpu-host", HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal("another eligible host remained", err)
	}
	if _, err := q.ChangeHostMaintenance("other-host", HostMaintenanceRequest{Action: "drain"}); !productionWorkOpen(err) {
		t.Fatal("drain left the project no host", err)
	}
	// Disable remains the explicit out-of-service action.
	if _, err := q.ChangeHostMaintenance("other-host", HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	if err := q.CloseProductionLease("render", "batch"); err != nil || len(q.ProductionLeases()) != 0 {
		t.Fatal("close", err)
	}
}

// The 2026-11-07 failure: a drain of the only eligible host between pipeline
// stages made the next stage fail at submission. With the lease open, the
// fleet upgrade keeps that host enabled, the next stage runs, and the host
// upgrades only after the lease closes. Other hosts upgrade meanwhile.
func TestFleetUpgradeWaitsForProductionLeaseBetweenPipelineStages(t *testing.T) {
	q, c, gpu, _ := productionLeaseFleet(t)
	if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "test", Key: "pipeline", TTL: "6h", Stages: []ProductionStage{cudaStage()}}); err != nil {
		t.Fatal(err)
	}
	first := submit(t, q, cudaProductionSpec())
	a := check(t, q, gpu, nil).Job
	if a == nil || a.ID != first.ID {
		t.Fatal("stage 1 not assigned")
	}
	finish(t, q, a, true)
	check(t, q, gpu, nil)

	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if got := fleetActivate(t, q, c); got != "queue-host/@queue" {
		t.Fatal("queue should upgrade first", got)
	}
	fleetTick(t, q)
	if got := fleetActivate(t, q, c); got != "other-host/worker-other-host" {
		t.Fatal("idle unrelated host should upgrade while the lease holds", got, q.fleetUpgradeStatus())
	}
	fleetTick(t, q)
	var waiting *fleetUpgradeHost
	for _, h := range q.s.FleetUpgrade.Hosts {
		if h.Host == "gpu-host" {
			waiting = h
		}
	}
	if waiting == nil || waiting.Phase != "pending" || !strings.Contains(waiting.Wait, "production lease pipeline") || q.maintenanceBlocked("gpu-host") {
		t.Fatal("idle lease host was drained", waiting, q.fleetUpgradeStatus())
	}

	// Stage 2 is submitted mid-rollout and starts on the still-enabled host.
	c.Advance(10 * time.Minute)
	gpu.Fleet.ObservedAt = c.Now()
	second := submit(t, q, cudaProductionSpec())
	b := check(t, q, gpu, nil).Job
	if b == nil || b.ID != second.ID {
		t.Fatal("stage 2 did not start", job(t, q, second.ID))
	}
	finish(t, q, b, true)
	check(t, q, gpu, nil)
	fleetTick(t, q)
	if q.fleetActiveHost() != nil {
		t.Fatal("upgrade drained the host while the lease was open")
	}

	if err := q.CloseProductionLease("test", "pipeline"); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if got := fleetActivate(t, q, c); got != "gpu-host/worker-gpu-host" {
		t.Fatal("closed lease did not release the host", got, q.fleetUpgradeStatus())
	}
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "complete" || q.maintenanceBlocked("gpu-host") {
		t.Fatal("rollout incomplete", q.fleetUpgradeStatus())
	}
	for _, id := range []string{first.ID, second.ID} {
		if got := job(t, q, id); got.State != "succeeded" {
			t.Fatal("pipeline stage did not complete", got.State, got.Reason)
		}
	}
}

// Queued production is open work too: draining its only host used to fail it
// at once with "no eligible host is enabled".
func TestQueuedProductionRefusesDrainOfItsOnlyHost(t *testing.T) {
	q, _, _, _ := productionLeaseFleet(t)
	j := submit(t, q, cudaProductionSpec())
	if _, err := q.ChangeHostMaintenance("gpu-host", HostMaintenanceRequest{Action: "drain"}); !productionWorkOpen(err) || !strings.Contains(err.Error(), j.ID) {
		t.Fatal("drain stranded queued production", err)
	}
	fleetTick(t, q)
	if got := job(t, q, j.ID); got.State != "queued" {
		t.Fatal("queued production failed", got.State, got.Reason)
	}
	// An explicit disable still takes the host out of service and fails fast.
	if _, err := q.ChangeHostMaintenance("gpu-host", HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if got := job(t, q, j.ID); got.State != "failed" || !strings.Contains(got.Reason, "no eligible host is enabled") {
		t.Fatal("disable no longer fails fast", got.State, got.Reason)
	}
}

// Without a lease, a stage submitted while the running upgrade holds its only
// host waits for the host to return instead of failing. A stopped operation's
// retained drain is not temporary, so the ordinary fail-fast applies.
func TestFleetOwnedDrainMakesProductionWaitThenStoppedDrainFails(t *testing.T) {
	q, c, gpu, _ := productionLeaseFleet(t)
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		fleetTick(t, q)
		if h := q.fleetActiveHost(); h != nil && h.Host == "gpu-host" {
			break
		}
		fleetActivate(t, q, c)
	}
	h := q.fleetActiveHost()
	if h == nil || h.Host != "gpu-host" || !q.fleetDrainTemporary("gpu-host") {
		t.Fatal("gpu host not under the upgrade's drain", q.fleetUpgradeStatus())
	}
	waiting := submit(t, q, cudaProductionSpec())
	fleetTick(t, q)
	if got := job(t, q, waiting.ID); got.State != "queued" {
		t.Fatal("stage failed during a temporary upgrade drain", got.State, got.Reason)
	}
	for range 2 {
		fleetActivate(t, q, c)
		fleetTick(t, q)
	}
	if q.s.FleetUpgrade.State != "complete" || q.maintenanceBlocked("gpu-host") {
		t.Fatal("rollout incomplete", q.fleetUpgradeStatus())
	}
	gpu.Session = "new-" + gpu.Session
	gpu.Fleet = q.s.Machines[gpu.ID].Fleet
	if r := check(t, q, gpu, nil).Job; r == nil || r.ID != waiting.ID {
		t.Fatal("waiting stage did not start after the host returned", job(t, q, waiting.ID))
	}

	// A stopped operation keeps its drain; that is not a returning host.
	q2, c2, _, _ := productionLeaseFleet(t)
	if _, err := q2.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		fleetTick(t, q2)
		if h := q2.fleetActiveHost(); h != nil && h.Host == "gpu-host" {
			break
		}
		fleetActivate(t, q2, c2)
	}
	q2.fleetStop(q2.fleetActiveHost(), "service_health_failed")
	if q2.fleetDrainTemporary("gpu-host") {
		t.Fatal("stopped operation's drain treated as temporary")
	}
	stranded := submit(t, q2, cudaProductionSpec())
	fleetTick(t, q2)
	if got := job(t, q2, stranded.ID); got.State != "failed" || !strings.Contains(got.Reason, "no eligible host is enabled") {
		t.Fatal("retained drain did not fail fast", got.State, got.Reason)
	}
}
