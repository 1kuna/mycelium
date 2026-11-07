package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func decisionFixture(t *testing.T) (*Queue, *fakeClock, Client) {
	t.Helper()
	q, clock := fixture(t)
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 8 << 20, nil }
	q.requestBudgets.lanes = 1
	server := httptest.NewServer(API{q, "test"}.Handler())
	t.Cleanup(server.Close)
	return q, clock, Client{URL: server.URL, Token: "test"}
}
func readDecisions(t *testing.T, client Client, id string) DecisionPage {
	t.Helper()
	var out DecisionPage
	if err := client.Call(context.Background(), "GET", "/api/jobs/"+id+"/decisions", nil, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestDecisionReceiptWithheldPredicateAndStatus(t *testing.T) {
	q, clock, client := decisionFixture(t)
	m := cpuMachine("diagnostic-worker-a", "physical", "cpu", 8, 32)
	attemptWire(t, client, Checkin{Machine: m})
	j := submit(t, q, cpuSpec(1, 1))
	// An independently reported legacy component on the same host disagrees
	// about the allowance. Fresh successful check-ins cannot grant admission.
	peer := machine("peer", "fake", "physical")
	peer.HostMemoryBudgetGB = 16
	peer.HostMemoryMeasured = true
	peer.HostAvailableMemoryGB = 32
	attemptWire(t, client, Checkin{Machine: peer})
	clock.Advance(time.Second)
	reply := attemptWire(t, client, Checkin{Machine: m})
	if reply.Job != nil {
		t.Fatal("policy fence admitted a job")
	}
	page := readDecisions(t, client, j.ID)
	var receipt *DecisionReceipt
	for i := range page.Receipts {
		if page.Receipts[i].Admission == "host_memory_policy" && page.Receipts[i].CheckinWorker == q.control.anonymous(m.ID) {
			receipt = &page.Receipts[i]
			break
		}
	}
	if receipt == nil || receipt.Admitted || receipt.Reply.Outcome != "reply_ready" || receipt.Reply.Assignments != 0 || receipt.Request == 0 || receipt.Revision == 0 || !receipt.ObservedThrough.Equal(clock.Now()) {
		t.Fatalf("missing matching predicate/reply: %+v", page)
	}
	out, err := client.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if out.Attempt != 0 || out.State != "queued" || !strings.Contains(out.Reason, "host allowance policy mismatch") || strings.Contains(out.Reason, "available for work") || out.Blocker == nil || !strings.Contains(out.Blocker.Evidence, "host allowance policy mismatch") {
		t.Fatalf("status hid admission: %+v", out)
	}
	wire, _ := json.Marshal(page)
	for _, secret := range []string{m.ID, m.Session, m.HostID, peer.ID, "echo", "done"} {
		if strings.Contains(string(wire), "\""+secret+"\"") {
			t.Fatalf("identity/payload leaked: %q", secret)
		}
	}
}
func TestDecisionReceiptAdmissibleCompetitor(t *testing.T) {
	q, clock, client := decisionFixture(t)
	m := cpuMachine("diagnostic-worker-a", "physical", "cpu", 8, 32)
	attemptWire(t, client, Checkin{Machine: m})
	winnerSpec := cpuSpec(1, 1)
	winnerSpec.Project = "behind"
	winner := submit(t, q, winnerSpec)
	clock.Advance(time.Second)
	loserSpec := cpuSpec(1, 1)
	loserSpec.Project = "ahead"
	loser := submit(t, q, loserSpec)
	q.s.FairUsage[q.fairKey(loser.Spec)] = FairUsage{Seconds: 100, At: clock.Now()}
	clock.Advance(time.Second)
	reply := attemptWire(t, client, Checkin{Machine: m})
	if reply.Job == nil || reply.Job.ID != winner.ID {
		t.Fatalf("existing order changed: %+v", reply)
	}
	page := readDecisions(t, client, loser.ID)
	if len(page.Receipts) != 1 {
		t.Fatalf("%+v", page)
	}
	r := page.Receipts[0]
	if !r.Admitted || r.Admission != "admitted" || r.Winner != winner.ID || r.WinnerClass != "experiment" || r.GlobalWinner != winner.ID || r.Fairness == nil || !r.Fairness.Before || r.Fairness.Reason != "fair_score" || r.Reply.Assignments != 1 || !r.Reply.WinnerOffered || r.Reply.WinnerAttempt != reply.Job.Attempt {
		t.Fatalf("lost selection/reply: %+v", r)
	}
	if job(t, q, loser.ID).Attempt != 0 {
		t.Fatal("loser acquired an attempt")
	}
}
func TestDecisionReceiptBusyCheckinsBoundWrites(t *testing.T) {
	q, clock, client := decisionFixture(t)
	m := cpuMachine("diagnostic-worker-a", "physical", "cpu", 8, 32)
	attemptWire(t, client, Checkin{Machine: m})
	j := submit(t, q, cpuSpec(1, 1))
	peer := machine("peer", "fake", "physical")
	peer.HostMemoryBudgetGB = 16
	peer.HostMemoryMeasured = true
	peer.HostAvailableMemoryGB = 32
	attemptWire(t, client, Checkin{Machine: peer})
	attemptWire(t, client, Checkin{Machine: m})
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	var initialWrites int
	if err := q.usageReader.QueryRow(`SELECT COUNT(*) FROM control_events WHERE json_extract(data,'$.kind')='decision'`).Scan(&initialWrites); err != nil {
		t.Fatal(err)
	}
	if initialWrites == 0 {
		t.Fatal("no durable initial decision")
	}
	initial := readDecisions(t, client, j.ID)
	for i := 0; i < 128; i++ {
		clock.Advance(time.Millisecond)
		attemptWire(t, client, Checkin{Machine: m})
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	var afterWrites int
	if err := q.usageReader.QueryRow(`SELECT COUNT(*) FROM control_events WHERE json_extract(data,'$.kind')='decision'`).Scan(&afterWrites); err != nil {
		t.Fatal(err)
	}
	if afterWrites > initialWrites {
		t.Fatalf("128 unchanged checkins added %d durable decisions", afterWrites-initialWrites)
	}
	t.Logf("128 unchanged checkins: durable decision rows %d -> %d", initialWrites, afterWrites)
	after := readDecisions(t, client, j.ID)
	if after.Changes != initial.Changes || after.RetainedBytes > after.CapacityBytes {
		t.Fatalf("heartbeat-proportional recording: before=%+v after=%+v", initial, after)
	}
	found := false
	for _, r := range after.Receipts {
		if r.Checkins >= 129 {
			found = true
			if !r.ObservedThrough.Equal(clock.Now()) {
				t.Fatal("repeat contact not retained")
			}
		}
	}
	if !found {
		t.Fatal("missing fresh repeat evidence")
	}
	// Capacity withdrawal sheds retained detail and counts lost coverage. It
	// cannot block or mutate an assignment decision.
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 0, nil }
	if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	tiny := q.decisionPage(j.ID)
	if tiny.RetainedBytes != 0 || len(tiny.Receipts) != 0 || tiny.Omitted == 0 {
		t.Fatalf("capacity not enforced: %+v", tiny)
	}
}
func TestDecisionReceiptRecoveryReply(t *testing.T) {
	q, clock, client := decisionFixture(t)
	m := m3Machine()
	attemptWire(t, client, recoveryCheckin(t, m))
	j := submit(t, q, exclusionSpec("", "0"))
	clock.Advance(time.Second)
	c := recoveryCheckin(t, m)
	c.Recovering = true
	reply := attemptWire(t, client, c)
	if len(reply.Attempts) != 0 {
		t.Fatal("recovery admitted work")
	}
	page := readDecisions(t, client, j.ID)
	if len(page.Receipts) == 0 {
		t.Fatal("missing recovery decision")
	}
	for _, r := range page.Receipts {
		if r.EarlyReturn != "recovering" || r.Admission != "not_evaluated" || r.Reply.Outcome != "reply_ready" || !r.Incoming.Recovering || r.Incoming.Protocol != "keyed" || !r.Incoming.Devices {
			t.Fatalf("%+v", r)
		}
	}
}

func TestDecisionReceiptPublicationOutsideQueueLock(t *testing.T) {
	q, _, _ := decisionFixture(t)
	m := cpuMachine("diagnostic-worker-a", "physical", "cpu", 8, 32)
	check(t, q, m, nil)
	submit(t, q, cpuSpec(1, 1))
	committed := make(chan struct{}, 1)
	q.observeUpdate = func(time.Duration, time.Duration) {
		select {
		case committed <- struct{}{}:
		default:
		}
	}
	q.decisions.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := q.Checkin(Checkin{Machine: m}); done <- err }()
	<-committed
	read := make(chan struct{})
	go func() { q.PriorityPolicy(); close(read) }()
	select {
	case <-read:
	case <-time.After(2 * time.Second):
		q.decisions.mu.Unlock()
		<-done
		t.Fatal("publication blocked a queue reader")
	}
	q.decisions.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDecisionReceiptDurableTargetedRead(t *testing.T) {
	q, clock, client := decisionFixture(t)
	m := cpuMachine("diagnostic-worker-a", "physical", "cpu", 8, 32)
	attemptWire(t, client, Checkin{Machine: m})
	winner := submit(t, q, cpuSpec(1, 1))
	clock.Advance(time.Second)
	loser := submit(t, q, cpuSpec(1, 1))
	reply := attemptWire(t, client, Checkin{Machine: m})
	if reply.Job == nil || reply.Job.ID != winner.ID {
		t.Fatal("fixture winner")
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	var dbSeq int
	var dbName, dbPath string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&dbSeq, &dbName, &dbPath); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	reopened.requestBudgets.memory = func(context.Context) (int64, error) { return 8 << 20, nil }
	reopened.requestBudgets.lanes = 1
	server := httptest.NewServer(API{reopened, "test"}.Handler())
	t.Cleanup(server.Close)
	page := readDecisions(t, Client{URL: server.URL, Token: "test"}, loser.ID)
	if len(page.Receipts) != 1 || page.Receipts[0].Winner != winner.ID || !page.Receipts[0].Reply.WinnerOffered || page.Receipts[0].Request == 0 {
		t.Fatalf("durable receipt unavailable: %+v", page)
	}
}

func TestDecisionReceiptRecorderlessSimulation(t *testing.T) {
	report := runSimTest(t, simTestFleet(1), simTestWork(simTestStream("sample", "experiment", 0, 2, 1)), 10)
	if report.Completed != 1 || report.Unfinished != 0 {
		t.Fatalf("simulation changed: %+v", report)
	}
}

func TestDecisionReceiptReorderedConcurrentDirectives(t *testing.T) {
	q, _, client := decisionFixture(t)
	m := m3Machine()
	attemptWire(t, client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol})
	j := submit(t, q, exclusionSpec("", "0"))
	a := AttemptReport{Execution: Execution{ID: "recovered-a", Attempt: 1}, Session: m.Session}
	b := AttemptReport{Execution: Execution{ID: "recovered-b", Attempt: 1}, Session: m.Session}
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{a, b}}
	first := attemptWire(t, client, c)
	if len(first.Attempts) != 2 || !first.Attempts[0].Kill || !first.Attempts[1].Kill {
		t.Fatal("missing concurrent cleanup directives", first.Attempts)
	}
	initial := readDecisions(t, client, j.ID)
	for i := 0; i < 16; i++ {
		c.Attempts[0], c.Attempts[1] = c.Attempts[1], c.Attempts[0]
		attemptWire(t, client, c)
	}
	after := readDecisions(t, client, j.ID)
	if initial.Changes != after.Changes || len(initial.Receipts) != len(after.Receipts) {
		t.Fatalf("report order added decisions: before=%+v after=%+v", initial, after)
	}
	for _, r := range after.Receipts {
		if r.Checkins != 17 || r.EarlyReturn != "cleanup" || r.Reply.Kills != 2 || r.Incoming.Reports != 2 || len(r.Reply.Directives) != 2 || r.Reply.DirectivesOmitted != 0 {
			t.Fatalf("lost repeated cleanup: %+v", r)
		}
		for i, id := range []string{"recovered-a", "recovered-b"} {
			d := r.Reply.Directives[i]
			if d.ID != id || d.Attempt != 1 || !d.Kill || d.Session != q.control.anonymous(m.Session) {
				t.Fatalf("lost addressed cleanup: %+v", d)
			}
		}
	}
}

func TestDecisionReceiptMemoryInputs(t *testing.T) {
	q, clock, client := decisionFixture(t)
	m := cpuMachine("memory-worker", "memory-host", "cpu", 8, 32)
	m.MemoryCeilingSlackGB = 0.25
	attemptWire(t, client, Checkin{Machine: m})
	j := submit(t, q, cpuSpec(2, 4))
	clock.Advance(time.Second)
	reply := attemptWire(t, client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Fresh: true})
	if len(reply.Attempts) != 1 || reply.Attempts[0].ID != j.ID {
		t.Fatal("expected actual keyed offer", reply.Attempts)
	}
	page := readDecisions(t, client, j.ID)
	if len(page.Receipts) != 1 {
		t.Fatalf("missing memory decision: %+v", page)
	}
	r := page.Receipts[0]
	for _, memory := range []DecisionMemory{r.Memory, r.ProjectedMemory, r.ImmediateMemory} {
		if !memory.Evaluated || !memory.Measured || memory.HostGB != 4 || memory.HostNeedGB != 4.25 || memory.SlackGB != 0.25 || memory.FreeHostGB != 32 || memory.AvailableHostGB != 32 || memory.ReservedHostGB != 0 {
			t.Fatalf("wrong exact memory inputs: %+v", memory)
		}
	}
	if r.Incoming.Protocol != "keyed" || !r.Incoming.Fresh || len(r.Reply.Directives) != 1 || r.Reply.Directives[0].Execution != reply.Attempts[0].Execution || !r.Reply.Directives[0].Offer {
		t.Fatalf("lost incoming/actual outgoing protocol: %+v", r)
	}
}

func TestDecisionReceiptHeadroomJitterBoundWrites(t *testing.T) {
	q, clock, client := decisionFixture(t)
	m := cpuMachine("memory-worker", "memory-host", "cpu", 8, 32)
	attemptWire(t, client, Checkin{Machine: m})
	j := submit(t, q, cpuSpec(2, 4))
	m.HostAvailableMemoryGB = 2
	attemptWire(t, client, Checkin{Machine: m})
	before := readDecisions(t, client, j.ID)
	if len(before.Receipts) != 1 || before.Receipts[0].Admitted || !before.Receipts[0].Memory.Evaluated {
		t.Fatalf("missing withheld memory decision: %+v", before)
	}
	for i := 0; i < 16; i++ {
		m.HostAvailableMemoryGB = 2 + float64(i)/1024
		clock.Advance(time.Second)
		attemptWire(t, client, Checkin{Machine: m})
	}
	after := readDecisions(t, client, j.ID)
	if before.Changes != after.Changes || after.Receipts[0].Memory != before.Receipts[0].Memory || after.Receipts[0].Checkins != 17 {
		t.Fatalf("jitter rewrote exact original evidence: before=%+v after=%+v", before, after)
	}
	m.HostAvailableMemoryGB = 32
	reply := attemptWire(t, client, Checkin{Machine: m})
	if reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatal("capacity recovery did not admit the same job")
	}
	ready := readDecisions(t, client, j.ID)
	if ready.Changes <= after.Changes || !ready.Receipts[0].Admitted || ready.Receipts[0].ImmediateMemory.FreeHostGB != 32 {
		t.Fatalf("changed gate lost new exact evidence: %+v", ready)
	}
}

func TestDecisionReceiptEarlyReturnFences(t *testing.T) {
	q, _, client := decisionFixture(t)
	if _, err := q.SetPriorityPolicy(0, defaultPriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	attemptWire(t, client, Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol})
	j := submit(t, q, exclusionSpec("", "0"))
	q.s.PreparationPlans = map[string]HostLaunch{"capture": {Machine: m.ID, Session: m.Session, Kind: "environment", Key: "recipe", Attempt: "preparation-attempt", Started: true}}
	q.s.Jobs[j.ID].PreemptionPlacement = &PreemptionPlacement{Machine: m.ID, Session: m.Session, Device: "0", Releases: []string{"release-attempt"}}
	m.EnvironmentReports = []EnvironmentReport{{Key: "recipe", Attempt: "preparation-attempt", State: "building"}}
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Recovering: true}
	attemptWire(t, client, c)
	page := readDecisions(t, client, j.ID)
	if len(page.Receipts) == 0 {
		t.Fatal("no early-return rows")
	}
	for _, r := range page.Receipts {
		if r.EarlyReturn != "recovering" || r.PolicyRevision != 1 || r.PreparationCount != 1 || r.Preparation != q.control.anonymous("preparation-attempt") || r.ReleaseCount != 1 || r.ReleaseFence == "" {
			t.Fatalf("early-return facts lost: %+v", r)
		}
	}
}

func TestDecisionReceiptFanOutOffer(t *testing.T) {
	q, _, m, s := fanOutFixture(t, 2)
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 8 << 20, nil }
	q.requestBudgets.lanes = 1
	parent := submit(t, q, s)
	reply := check(t, q, m, nil)
	if reply.Job == nil || reply.Job.Batch == nil || reply.Job.Batch.Parent != parent.ID || reply.Job.ID == parent.ID {
		t.Fatal("no child wave")
	}
	page, err := q.DecisionReceipts(context.Background(), parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range page.Receipts {
		if r.GlobalWinner != parent.ID {
			continue
		}
		found = true
		if !r.Reply.JobOffered || !r.Reply.WinnerOffered || !r.Reply.GlobalOffered || r.Reply.JobExecution != reply.Job.ID || r.Reply.JobAttempt != reply.Job.Attempt || decisionValue(r) == 3 {
			t.Fatalf("successful child misreported as omitted: %+v", r)
		}
	}
	if !found {
		t.Fatal("missing parent dispatch receipt")
	}
}

func TestDecisionReceiptReadCapacityExhaustion(t *testing.T) {
	q, _, _ := decisionFixture(t)
	for i := 0; i < 64; i++ {
		r := &DecisionReceipt{JobID: "queued", Target: ID(), CheckinWorker: ID()}
		q.control.append(ControlEvent{Kind: "decision", Operation: "checkin", Decision: r})
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 4 * decisionCost(&DecisionReceipt{}), nil }
	small, err := q.DecisionReceipts(context.Background(), "queued")
	if err != nil {
		t.Fatal(err)
	}
	if !small.Truncated || len(small.Receipts) > 2 {
		t.Fatalf("read exceeded capacity: %+v", small)
	}
	// Zero capacity must not allocate a deduplication key per durable row, or
	// even start a durable read. A closed reader makes an attempted scan fail.
	if err := q.usageReader.Close(); err != nil {
		t.Fatal(err)
	}
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 0, nil }
	empty, err := q.DecisionReceipts(context.Background(), "queued")
	if err != nil || !empty.Truncated || len(empty.Receipts) != 0 {
		t.Fatalf("exhausted read scanned storage: %+v %v", empty, err)
	}
}
