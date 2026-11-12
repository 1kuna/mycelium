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
	if got := fleetActivate(t, q, c); got != "queue-host/@queue" {
		t.Fatal("the queue's host goes last", got)
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
	for range 3 {
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

// A critical update may interrupt production (MY-39): an open lease does not
// hold its host. The running stage gets the ordinary checkpoint stop, and a
// stage submitted during the upgrade's drain waits for the host to return.
func TestCriticalFleetUpgradeProceedsPastProductionLease(t *testing.T) {
	q, c, gpu, _ := productionLeaseFleet(t)
	if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "test", Key: "pipeline", TTL: "6h", Stages: []ProductionStage{cudaStage()}}); err != nil {
		t.Fatal(err)
	}
	s := cudaProductionSpec()
	s.StartWindow = "10m" // Covers stop grace and service-health verification.
	first := submit(t, q, s)
	run := check(t, q, gpu, nil).Job
	if run == nil || run.ID != first.ID {
		t.Fatal("stage 1 not assigned")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Urgency: "critical"}); err != nil {
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
	if h == nil || h.Host != "gpu-host" || h.CriticalStopAt == nil || !q.fleetDrainTemporary("gpu-host") {
		t.Fatal("critical update waited on the lease", q.fleetUpgradeStatus())
	}
	if got := job(t, q, first.ID); got.State != "stopping" {
		t.Fatal("running stage not asked to checkpoint and stop", got.State)
	}
	stopped := result(run, false)
	stopped.Interrupted = true
	if err := q.Complete(stopped); err != nil {
		t.Fatal(err)
	}
	next := submit(t, q, s)
	fleetTick(t, q)
	for _, id := range []string{first.ID, next.ID} {
		if got := job(t, q, id); got.State != "queued" {
			t.Fatal("stage failed during the critical drain", id, got.State, got.Reason)
		}
	}
	for q.s.FleetUpgrade.State != "complete" {
		fleetActivate(t, q, c)
		fleetTick(t, q)
	}
	if q.maintenanceBlocked("gpu-host") {
		t.Fatal("critical update kept its drain")
	}
}

// Default status names each open lease once: project/key, expiry and the hosts
// able to start a stage (which planned drains keep one of enabled).
func TestProductionLeaseInCompactStatus(t *testing.T) {
	q, c, _, _ := productionLeaseFleet(t)
	if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "render", Key: "run-7", TTL: "2h", Stages: []ProductionStage{cudaStage()}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.OpenProductionLease(ProductionLeaseRequest{Project: "notes", Key: "any", TTL: "1h"}); err != nil {
		t.Fatal(err)
	}
	read := func() CompactStatus {
		q.statusCache.data, q.statusCache.compact = "", ""
		r := httptest.NewRequest("GET", "/api/status", nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
		var view CompactStatus
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &view) != nil {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		return view
	}
	view := read()
	want := []StatusLease{
		{Project: "notes", Key: "any", ExpiresAt: c.Now().Add(time.Hour), Hosts: []string{"gpu-host", "other-host"}},
		{Project: "render", Key: "run-7", ExpiresAt: c.Now().Add(2 * time.Hour), Hosts: []string{"gpu-host"}},
	}
	if len(view.Leases) != len(want) {
		t.Fatalf("leases: %+v", view.Leases)
	}
	for i, l := range view.Leases {
		if l.Project != want[i].Project || l.Key != want[i].Key || !l.ExpiresAt.Equal(want[i].ExpiresAt) || strings.Join(l.Hosts, ",") != strings.Join(want[i].Hosts, ",") {
			t.Fatalf("lease %d: %+v want %+v", i, l, want[i])
		}
	}
	text := strings.Join(view.Lines(false), "\n")
	if !strings.Contains(text, "production lease render/run-7: expires "+want[1].ExpiresAt.UTC().Format(time.RFC3339)+"; holds gpu-host\n") {
		t.Fatal(text)
	}
	c.Advance(90 * time.Minute) // the notes lease has expired
	if err := q.CloseProductionLease("render", "run-7"); err != nil {
		t.Fatal(err)
	}
	if view = read(); len(view.Leases) != 0 || strings.Contains(strings.Join(view.Lines(false), "\n"), "production lease") {
		t.Fatalf("closed or expired lease still shown: %+v", view.Leases)
	}
}

// #1374: a production stage declared with after: is open production work
// before its upstream succeeds, so a drain of its last eligible host refuses
// without a lease, between producer waves too. Upstream failure fails the stage
// and releases the host.
func TestBlockedAfterProductionStageRefusesDrainUntilUpstreamFails(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	spare := machine("worker-spare", "fake", "spare-host")
	check(t, q, spare, nil)
	parent := submit(t, q, s)
	c := wholeParentConsumer(s, parent.ID, false)
	c.Priority = "production"
	consumer := submit(t, q, c)
	if consumer.Spec.StartBy != nil || len(q.ProductionLeases()) != 0 {
		t.Fatal("fixture should have no start deadline and no lease", consumer.Spec.StartBy)
	}
	host := hostID(&m)
	wave := check(t, q, m, nil).Job
	if wave == nil || wave.Batch == nil {
		t.Fatal("producer wave not running")
	}
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain"}); !productionWorkOpen(err) || !strings.Contains(err.Error(), consumer.ID) || !strings.Contains(err.Error(), "waiting on after: "+parent.ID) {
		t.Fatal("drain stranded the declared stage", err)
	}
	if status, _ := q.HostMaintenanceStatus(host); !strings.Contains(status.ProductionBlocker, consumer.ID) {
		t.Fatal("host status (and fleet upgrade wait) hides the stage", status.ProductionBlocker)
	}
	// Eligibility comes from the stage's needs: a host that cannot start it drains.
	if _, err := q.ChangeHostMaintenance(hostID(&spare), HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	finish(t, q, wave, true)
	clock.Advance(time.Second)
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain"}); !productionWorkOpen(err) {
		t.Fatal("drain between producer waves stranded the stage", err)
	}
	tail := check(t, q, m, nil).Job
	finish(t, q, tail, false)
	if got := job(t, q, consumer.ID); got.State != "failed" || got.DiagnosticCode != "upstream_failed" {
		t.Fatal("upstream failure did not settle the stage", got.State, got.DiagnosticCode)
	}
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal("failed upstream still protects the host", err)
	}
}

// Only production stages are protected, and the protection ends when the stage
// is cancelled or starts running.
func TestBlockedAfterStageProtectionScope(t *testing.T) {
	q, _, m, s := fanOutFixture(t, 3)
	s.Outputs = map[string]string{"out": "results"}
	host := hostID(&m)
	parent := submit(t, q, s)
	experiment := submit(t, q, pieceConsumer(s, parent.ID, "item-1"))
	if reason := q.productionDrainBlockerForTest(host); reason != "" {
		t.Fatal("non-production after: stage blocks maintenance", experiment.ID, reason)
	}
	c := pieceConsumer(s, parent.ID, "item-2")
	c.Priority = "production"
	cancelledStage := submit(t, q, c)
	if reason := q.productionDrainBlockerForTest(host); !strings.Contains(reason, cancelledStage.ID) {
		t.Fatal("production stage not counted", reason)
	}
	if _, err := q.CancelJob(cancelledStage.ID); err != nil {
		t.Fatal(err)
	}
	if reason := q.productionDrainBlockerForTest(host); reason != "" {
		t.Fatal("cancelled stage still blocks maintenance", reason)
	}
	c = pieceConsumer(s, parent.ID, "item-0")
	c.Priority = "production"
	running := submit(t, q, c)
	wave := check(t, q, m, nil).Job
	finish(t, q, wave, true)
	if got := job(t, q, running.ID); !dependencyReady(got) {
		t.Fatal("stage not released by its accepted piece", got.Reason)
	}
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != running.ID {
		t.Fatal("ready production stage not assigned", assigned)
	}
	if reason := q.productionDrainBlockerForTest(host); reason != "" {
		t.Fatal("running stage still blocks maintenance", reason)
	}
}

func (q *Queue) productionDrainBlockerForTest(host string) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.productionDrainBlocker(host)
}
