package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The fake daemon proves both members receive the same worker-owned parent
// and bounded swap; fake kernel counters prove the aggregate OOM survives
// counter removal as a job failure. No local Docker daemon is contacted.
func TestDockerEnvelopeSharedParent(t *testing.T) {
	_, clock := fixture(t)
	ceilings := fakeCeilings(t)
	ceilings.fs = envelopePeakFS{oom: true}
	parent, err := ceilings.create("attempt-docker", 4<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string]map[string]any{}
	d := &dockerAttempt{clock: clock, dir: t.TempDir(), memory: parent, state: dockerExecution{Identity: "test", CgroupParent: "/synthetic", MemoryLimitBytes: 4 << 30, Containers: map[string]dockerMember{}}}
	d.command = func(_ context.Context, args ...string) ([]byte, error) {
		switch args[0] {
		case "create":
			fields := map[string]string{}
			for i := 1; i < len(args)-1; i++ {
				if strings.HasPrefix(args[i], "--") {
					fields[args[i]] = args[i+1]
				}
			}
			if fields["--cgroup-parent"] != "/synthetic" || fields["--memory"] != "4294967296" || fields["--memory-swap"] != "4294967296" {
				t.Fatalf("uncontained member: %v", fields)
			}
			id := fmt.Sprintf("container%d", len(objects)+1)
			objects[id] = map[string]any{"Id": id, "Config": map[string]any{"Labels": map[string]string{"myce.attempt": "test"}}, "HostConfig": map[string]any{"RestartPolicy": map[string]string{"Name": "no"}, "CgroupParent": "/synthetic", "Memory": 4 << 30, "MemorySwap": 4 << 30, "NetworkMode": "none"}, "State": map[string]any{"Running": false, "Status": "exited", "ExitCode": 0}}
			return []byte(id), nil
		case "ps":
			ids := []string{}
			for id := range objects {
				ids = append(ids, id)
			}
			return []byte(strings.Join(ids, "\n")), nil
		case "inspect":
			values := []map[string]any{}
			for _, id := range args[1:] {
				values = append(values, objects[id])
			}
			return json.Marshal(values)
		case "start":
			return nil, nil
		}
		t.Fatalf("unexpected Docker operation: %v", args)
		return nil, nil
	}
	base := []string{"create", "--cgroup-parent", "/synthetic", "--network", "none"}
	for _, name := range []string{"helper", "main"} {
		if _, err := d.launchContainer(base, []string{"image"}, name, 0, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.releaseEnvelope(); err != nil {
		t.Fatal(err)
	}
	r := dockerOutcome(d.state, Result{}, nil)
	if !r.OutOfMemory || r.Interrupted || !strings.Contains(r.Error, "4 GiB memory ceiling") || r.MemoryPeak == nil || r.MemoryPeak.HostGB == nil || *r.MemoryPeak.HostGB != 2 {
		t.Fatalf("lost envelope OOM: %+v", r)
	}
	// Legacy capped groups remain eligible without shared-parent support.
	j := dockerJob()
	j.Spec.Run.DockerSidecars = []DockerSidecar{{Name: "helper", MemoryGB: 1}}
	j.Spec.Run.DockerMemoryGB = 1
	m := machine("docker", "fake")
	m.Capabilities = append(m.Capabilities, "docker")
	if !dockerRuntimeFits(&j, &m) {
		t.Fatal("legacy capped Docker refused")
	}
	j.Spec.Run.DockerMemoryGB = 0
	if dockerRuntimeFits(&j, &m) || dockerRuntimeWaitingReason(&j, map[string]*Machine{m.ID: &m}) != dockerEnvelopeUnavailable {
		t.Fatal("unsupported envelope Docker lacks refusal")
	}
}

// A recovered cgroup can expose event counts without resident accounting. Read
// zeros alone must not invalidate an existing learned envelope.
func TestDockerEnvelopeRecoveryEventsOnlyKeepsLearning(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	seed := startObservation(t, q, m, s)
	host := 1.0
	r := result(&seed, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	j := startObservation(t, q, m, s)
	a, err := fakeCeilings(t).create("recovered-events-only", 4<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	d := &dockerAttempt{memory: a, clock: clock, dir: t.TempDir(), state: dockerExecution{Identity: "recovered", CgroupParent: "/synthetic", MainFinished: true, MemoryLimitBytes: 4 << 30}}
	if err := d.releaseEnvelope(); err != nil {
		t.Fatal(err)
	}
	r = dockerOutcome(d.state, result(&j, true), nil)
	if r.MemoryPeak == nil || r.MemoryPeak.Events == nil || r.MemoryPeak.HostGB != nil || r.MemoryPeak.CgroupGB != nil {
		t.Fatal("regression needs events without resident measurements", r.MemoryPeak)
	}
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, s)
	if r.MemoryPeak.Constrained || next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 1.25 {
		t.Fatalf("events-only recovery invalidated learning: peak=%+v next=%+v", r.MemoryPeak, next.MemoryEnvelope)
	}
}
