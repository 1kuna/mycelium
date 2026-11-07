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
		if r.EarlyReturn != "recovering" || r.Admission != "not_evaluated" || r.Reply.Outcome != "reply_ready" {
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
	// Drop only the ephemeral view, as a queue process restart does. The targeted
	// endpoint must recover a matching immutable decision through the WAL reader.
	q.decisions.mu.Lock()
	q.decisions.rows = nil
	q.decisions.bytes = 0
	q.decisions.mu.Unlock()
	page := readDecisions(t, client, loser.ID)
	if len(page.Receipts) != 1 || page.Receipts[0].Winner != winner.ID || !page.Receipts[0].Reply.WinnerOffered || page.Receipts[0].Request == 0 {
		t.Fatalf("durable receipt unavailable: %+v", page)
	}
}
