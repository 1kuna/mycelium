package myce

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func dockerSharedSpec(priority string, gb float64) Spec {
	s := sharedSpec(priority, gb)
	s.Resumable = false
	s.Run = dockerJob().Spec.Run
	s.Run.DockerMemoryGB = 1
	return s
}

func dockerSharedMachine() Machine {
	m := sharedUnifiedMachine()
	m.ID = "docker-worker"
	m.Capabilities = append(m.Capabilities, "docker")
	m.CapabilityProfile.OS = "linux"
	return m
}

func dockerSharedPair(t *testing.T, q *Queue, m Machine) (*Job, *Job) {
	t.Helper()
	a := assignGPU(t, q, m, submit(t, q, dockerSharedSpec("experiment", 17)))
	b := assignedJob(machineCheck(t, q, m, a))
	if b != nil {
		t.Fatal("unexpected assignment")
	}
	second := submit(t, q, dockerSharedSpec("experiment", 13))
	b = assignedJob(machineCheck(t, q, m, a))
	if b == nil || b.ID != second.ID || !slices.Equal(a.AllocatedDevices, b.AllocatedDevices) {
		t.Fatal("Docker jobs did not share", job(t, q, second.ID).Reason)
	}
	for _, j := range []*Job{a, b} {
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
	}
	return a, b
}

func startSharedDocker(t *testing.T, m Machine, clock Clock, j *Job) (*process, *workerStore, string) {
	t.Helper()
	w, s, _, root := dockerFixture(t)
	w.Machine = sharedWorker(m, clock).Machine
	w.Clock = clock
	s.state.Session = j.Session
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, err := startDockerFixture(w, s, *j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.stop(clock.Now(), 0, true)
		if c, ok := clock.(*dockerTestClock); ok {
			c.advance(defaultGPUStopGrace + 5*time.Second)
		}
		p.docker.waitShutdown(clock)
		p.docker.close()
	})
	await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
	return p, s, root
}

func TestDockerSharedDeviceRunsTwoJobsThatFit(t *testing.T) {
	q, _ := fixture(t)
	clock := &dockerTestClock{now: q.clock.Now()}
	q.clock = clock
	m := dockerSharedMachine()
	a, b := dockerSharedPair(t, q, m)
	w := sharedWorker(m, clock)
	if err := localAttemptAdmission(w, []Job{*a}, *b, nil); err != nil {
		t.Fatal(err)
	}
	for _, j := range []*Job{a, b} {
		_, _, root := startSharedDocker(t, m, clock, j)
		limit, _ := os.ReadFile(filepath.Join(root, "--memory"))
		network, _ := os.ReadFile(filepath.Join(root, "--network"))
		if string(limit) != "1073741824" || string(network) != "none" {
			t.Fatalf("cap/network: %s/%s", limit, network)
		}
	}
	machineCheck(t, q, m, a, b)
	requireState(t, q, a.ID, "running")
	requireState(t, q, b.ID, "running")
}

func TestDockerSharedDeviceProductionStopsAllContainers(t *testing.T) {
	q, _ := fixture(t)
	clock := &dockerTestClock{now: q.clock.Now()}
	q.clock = clock
	m := dockerSharedMachine()
	a, b := dockerSharedPair(t, q, m)
	pa, _, rootA := startSharedDocker(t, m, clock, a)
	pb, _, rootB := startSharedDocker(t, m, clock, b)
	demand := submit(t, q, dockerSharedSpec("production", 25))
	reply := machineCheck(t, q, m, a, b)
	if assignedJob(reply) != nil {
		t.Fatal("started before release")
	}
	victims := []struct {
		job     *Job
		process *process
		root    string
	}{{a, pa, rootA}, {b, pb, rootB}}
	for _, victim := range victims {
		requireState(t, q, victim.job.ID, "stopping")
		if !slices.ContainsFunc(reply.Attempts, func(d AttemptDirective) bool {
			return d.ID == victim.job.ID && d.Attempt == victim.job.Attempt && d.Stop && d.PriorityStop != nil
		}) {
			t.Fatal("missing exact container stop directive", victim.job.ID, reply.Attempts)
		}
		victim.process.stop(clock.Now(), StopGrace, false)
	}
	if assignedJob(machineCheck(t, q, m, a, b)) != nil {
		t.Fatal("started during cleanup")
	}
	clock.advance(defaultGPUStopGrace + StopGrace)
	for i, victim := range victims {
		var r Result
		select {
		case f := <-victim.process.done:
			if f.err != nil {
				t.Fatal(f.err)
			}
			r = f.result
		case <-time.After(testWaitTimeout):
			t.Fatal("victim cleanup did not finish", victim.job.ID)
		}
		if !r.Interrupted {
			t.Fatal("preemption lost interruption", r)
		}
		calls, err := os.ReadFile(filepath.Join(victim.root, "calls"))
		if err != nil || !strings.Contains(string(calls), "kill --signal=") && !strings.Contains(string(calls), "stop --time") {
			t.Fatal("container was not signalled", victim.job.ID, string(calls), err)
		}
		if _, err := os.Stat(filepath.Join(victim.root, "running")); !os.IsNotExist(err) {
			t.Fatal("container still running after cleanup", victim.job.ID, err)
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if i == 0 && assignedJob(machineCheck(t, q, m, b)) != nil {
			t.Fatal("production started before every exact container release")
		}
	}
	got := assignedJob(machineCheck(t, q, m))
	if got == nil || got.ID != demand.ID {
		t.Fatal("production did not start after every exact release", job(t, q, demand.ID).Reason)
	}
	startSharedDocker(t, m, clock, got)
}

func TestDockerSharedDeviceUnknownMemoryKeepsCard(t *testing.T) {
	q, _ := fixture(t)
	m := dockerSharedMachine()
	s := plainGPUSpec()
	s.Run = dockerJob().Spec.Run
	s.Run.DockerMemoryGB = 1
	s.Needs.HostMemoryGB = 1
	a := assignGPU(t, q, m, submit(t, q, s))
	small := submit(t, q, dockerSharedSpec("experiment", 13))
	if assignedJob(machineCheck(t, q, m, a)) != nil {
		t.Fatal("joined unknown device demand")
	}
	requireState(t, q, small.ID, "queued")
}

func TestDockerSharedDeviceRestartKeepsCustody(t *testing.T) {
	q, clock := fixture(t)
	m := dockerSharedMachine()
	a, b := dockerSharedPair(t, q, m)
	machineCheck(t, q, m, a, b)
	q = memoryFactsRestart(t, q, clock)
	fakeCheckpointAvailability(t, q, true)
	third := submit(t, q, dockerSharedSpec("experiment", 17))
	if assignedJob(machineCheck(t, q, m, a, b)) != nil {
		t.Fatal("restart lost charge")
	}
	for _, old := range []*Job{a, b} {
		got := job(t, q, old.ID)
		if got.State != "running" || got.Attempt != old.Attempt || got.ReleaseConfirmed {
			t.Fatal("restart lost exact custody", got.ID, got.State)
		}
	}
	requireState(t, q, third.ID, "queued")
}
