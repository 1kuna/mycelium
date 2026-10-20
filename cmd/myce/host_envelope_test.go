package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	validate "github.com/santhosh-tekuri/jsonschema/v6"
	"mycelium/internal/myce"
)

func validateEnvelopeOutput(t *testing.T, data []byte) myce.HostEnvelope {
	t.Helper()
	compiler := validate.NewCompiler()
	schema, err := compiler.Compile("../../docs/schemas/host-envelope-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	value, err := validate.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err, string(data))
	}
	if err = schema.Validate(value); err != nil {
		t.Fatal(err, string(data))
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var e myce.HostEnvelope
	if err = decoder.Decode(&e); err != nil {
		t.Fatal(err)
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		t.Fatal("multiple outputs", err)
	}
	t.Logf("actual command output: %s", data)
	return e
}
func envelopeFixture(t *testing.T) (myce.HostIdentityResolver, myce.ServiceConfig) {
	t.Helper()
	r, account, c := localIdentityFixture(t)
	lookup := r.Lookup
	r.Lookup = func(name string) (myce.WorkerAccount, error) {
		if name == "" {
			return account, nil
		}
		return lookup(name)
	}
	r.EUID = os.Geteuid()
	return r, c
}
func invokeEnvelope(t *testing.T, r myce.HostIdentityResolver, c myce.ServiceConfig, flags ...string) (myce.HostEnvelope, error) {
	t.Helper()
	args := []string{flags[0], "--local", "--config", c.Config, "--json"}
	// This fixture has no privileged observer, so an accepted close cannot
	// confirm release. Keep that failure bounded; explicit timeouts override it.
	if flags[0] == "close" {
		args = append(args, "--timeout", "80ms")
	}
	args = append(args, flags[1:]...)
	var out bytes.Buffer
	err := hostLocalCommandWithIdentity(args, myce.MachineProbe{OS: "fixture"}, &out, r, nil)
	e := validateEnvelopeOutput(t, out.Bytes())
	t.Logf("host %s exit=%d", strings.Join(flags, " "), exitCode(err))
	return e, err
}
func TestHostEnvelopeCommandsAndFailures(t *testing.T) {
	r, c := envelopeFixture(t)
	for _, tc := range []struct {
		name  string
		flags []string
		code  int
	}{
		{"status", []string{"status"}, 0},
		{"missing-open-CAS", []string{"open"}, 2},
		{"open-mismatch", []string{"open", "--expect-revision", "99"}, 3},
		{"open-unsettled", []string{"open", "--expect-revision", "1"}, 1},
		{"close-mismatch", []string{"close", "--expect-revision", "0"}, 3},
		{"invalid-flag", []string{"status", "--unit", "myce-worker.service"}, 2},
		{"invalid-duration", []string{"status", "--timeout", "invalid"}, 2},
		{"missing-local", []string{"status", "--local=false"}, 2},
		{"close", []string{"close", "--grace", "0"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := invokeEnvelope(t, r, c, tc.flags...)
			if exitCode(err) != tc.code {
				t.Fatal(e, err, exitCode(err))
			}
			if tc.code == 3 && (e.Accepted != nil || len(e.Errors) == 0 || e.Errors[len(e.Errors)-1].Code != "revision_mismatch") {
				t.Fatal(e)
			}
		})
	}
	t.Run("production-executable-output", func(t *testing.T) {
		binary := filepath.Join(t.TempDir(), "myce")
		build := exec.Command("go", "build", "-o", binary, ".")
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build: %v %s", err, output)
		}
		for _, tc := range []struct {
			args []string
			code int
		}{
			{[]string{"status"}, 0},
			{[]string{"open", "--expect-revision", "99"}, 3},
			{[]string{"open"}, 2},
			{[]string{"close", "--expect-revision", "99"}, 3},
			{[]string{"status", "--unit", "myce-worker.service"}, 2},
			{[]string{"close", "--grace", "0", "--timeout", "80ms"}, 1},
		} {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			args := append([]string{"host", tc.args[0], "--local", "--config", c.Config, "--json"}, tc.args[1:]...)
			cmd := exec.CommandContext(ctx, binary, args...)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			output, err := cmd.Output()
			cancel()
			code := 0
			var exited *exec.ExitError
			if errors.As(err, &exited) {
				code = exited.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			validateEnvelopeOutput(t, output)
			t.Logf("production myce %s raw exit=%d", strings.Join(args[:2], " "), code)
			if code != tc.code {
				t.Fatal(code, tc.code, stderr.String())
			}
		}
	})

	// A failed dropped child emits a schema-valid error and no acceptance.
	root, _, config := localIdentityFixture(t)
	var out bytes.Buffer
	err := hostLocalCommandWithIdentity([]string{"close", "--local", "--worker-user", "worker", "--config", config.Config, "--json"}, myce.MachineProbe{}, &out, root, func(context.Context, myce.HostChildLaunch) ([]byte, error) {
		return nil, errors.New("private fixture cause")
	})
	e := validateEnvelopeOutput(t, out.Bytes())
	if err == nil || e.Accepted != nil || strings.Contains(out.String(), "private fixture cause") {
		t.Fatal(e, err)
	}
	// Human output is a rendering of the same typed snapshot.
	out.Reset()
	if err = hostLocalCommandWithIdentity([]string{"status", "--local", "--config", c.Config}, myce.MachineProbe{}, &out, r, nil); err != nil || !strings.Contains(out.String(), "Host closed:") {
		t.Fatal(err, out.String())
	}
}
func TestHostEnvelopeCASAndLastClose(t *testing.T) {
	r, c := envelopeFixture(t)
	before := snapshotHostFiles(t, c.HostStateDir)
	mismatch, err := invokeEnvelope(t, r, c, "close", "--expect-revision", "99")
	if exitCode(err) != 3 || mismatch.Desired.Revision == nil || *mismatch.Desired.Revision != 1 {
		t.Fatal(mismatch, err)
	}
	if !reflect.DeepEqual(before, snapshotHostFiles(t, c.HostStateDir)) {
		t.Fatal("mismatch wrote host files")
	}
	first, err := invokeEnvelope(t, r, c, "close", "--grace", "0")
	if !errors.Is(err, myce.ErrHostReleaseUnconfirmed) || first.Accepted == nil || *first.Accepted != 2 || first.LastClose.Revision != 2 {
		t.Fatal(first, err)
	}
	second, err := invokeEnvelope(t, r, c, "close", "--grace", "0", "--expect-revision", "2")
	if !errors.Is(err, myce.ErrHostReleaseUnconfirmed) || second.Accepted == nil || *second.Accepted != 3 || *first.Operation == *second.Operation {
		t.Fatal(second, err)
	}
	state, err := myce.ReadHostIntent(c.HostStateDir)
	if err != nil {
		t.Fatal(err)
	}
	// Unit fixture only: dev lifecycle proves real settlement separately.
	now := time.Now().UTC()
	state.LastClose.Settlement = "confirmed"
	state.LastClose.SettledAt = &now
	data, _ := json.Marshal(state)
	if err = os.WriteFile(filepath.Join(c.HostStateDir, "host-state.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	opened, err := invokeEnvelope(t, r, c, "open", "--expect-revision", "3")
	if err != nil || opened.Accepted == nil || *opened.Accepted != 4 || opened.Ready != nil && *opened.Ready || !reflect.DeepEqual(opened.LastClose, state.LastClose) {
		t.Fatal(opened, err)
	}
	if _, err = invokeEnvelope(t, r, c, "open", "--expect-revision", "4"); exitCode(err) != 3 {
		t.Fatal(err)
	}
}
func TestHostEnvelopeStatusReadOnlyAndFreshness(t *testing.T) {
	r, c := envelopeFixture(t)
	before := snapshotHostFiles(t, c.HostStateDir)
	e, err := invokeEnvelope(t, r, c, "status")
	if err != nil || e.Accepted != nil || e.Counts.Active != nil || e.QueueRevision != nil {
		t.Fatal(e, err)
	}
	if !reflect.DeepEqual(before, snapshotHostFiles(t, c.HostStateDir)) {
		t.Fatal("status wrote files")
	}
	// Root can read only through a dropped child. Even failure is an envelope.
	root, _, config := localIdentityFixture(t)
	root.ReadConfig = func(string) (myce.ServiceConfig, error) {
		t.Fatal("root read config")
		return myce.ServiceConfig{}, nil
	}
	root.ValidatePath = func(string, uint32, bool) error { t.Fatal("root inspected state"); return nil }
	var out bytes.Buffer
	calls := 0
	err = hostLocalCommandWithIdentity([]string{"status", "--local", "--worker-user", "worker", "--config", config.Config, "--json"}, myce.MachineProbe{}, &out, root, func(context.Context, myce.HostChildLaunch) ([]byte, error) { calls++; return nil, myce.ErrHostChild })
	validateEnvelopeOutput(t, out.Bytes())
	if calls != 1 || !errors.Is(err, myce.ErrHostChild) {
		t.Fatal(calls, err)
	}
}
func TestHostEnvelopeWaitDeadline(t *testing.T) {
	r, c := envelopeFixture(t)
	for _, flags := range [][]string{{"status", "--expect", "ready", "--wait", "--timeout", "80ms"}, {"close", "--wait-released", "--grace", "0", "--timeout", "80ms"}} {
		start := time.Now()
		e, err := invokeEnvelope(t, r, c, flags...)
		if err == nil || time.Since(start) > 300*time.Millisecond || e.Ready != nil && *e.Ready {
			t.Fatal(e, err, time.Since(start))
		}
	}
	root, _, config := localIdentityFixture(t)
	var out bytes.Buffer
	start := time.Now()
	err := hostLocalCommandWithIdentity([]string{"open", "--local", "--worker-user", "worker", "--config", config.Config, "--expect-revision", "1", "--timeout", "2100ms", "--json"}, myce.MachineProbe{}, &out, root, func(ctx context.Context, launch myce.HostChildLaunch) ([]byte, error) {
		deadline, _ := ctx.Deadline()
		remaining := time.Until(deadline)
		if launch.Request.Timeout <= 0 || launch.Request.Timeout > remaining-1900*time.Millisecond {
			t.Fatalf("child timeout %v remaining %v", launch.Request.Timeout, remaining)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	validateEnvelopeOutput(t, out.Bytes())
	if err == nil || time.Since(start) > 2300*time.Millisecond {
		t.Fatal(err, time.Since(start))
	}
}

func TestHostEnvelopeWaitDeadlineFakeClock(t *testing.T) {
	t.Run("in-process-wait-exact-deadline", func(t *testing.T) {
		r, c := envelopeFixture(t)
		clock := &waitClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
		start := clock.Now()
		pauses := 0
		var out bytes.Buffer
		err := hostEnvelopeCommandWithTiming([]string{"status", "--local", "--config", c.Config, "--expect", "ready", "--wait", "--timeout", "80ms", "--json"}, myce.MachineProbe{}, &out, r, nil, nil, nil, clock, func(_ context.Context, delay time.Duration) error {
			pauses++
			clock.advance(delay)
			return nil
		})
		validateEnvelopeOutput(t, out.Bytes())
		if !errors.Is(err, myce.ErrHostDeadline) || !clock.Now().Equal(start.Add(80*time.Millisecond)) || pauses != 4 {
			t.Fatalf("deadline error=%v elapsed=%v pauses=%d", err, clock.Now().Sub(start), pauses)
		}
	})
	for _, tc := range []struct {
		name          string
		resolve, read time.Duration
		wait          bool
		wantErr       bool
	}{
		{"before", 0, 3*time.Second - time.Nanosecond, false, false},
		{"at", 0, 3 * time.Second, false, true},
		{"after", 0, 3*time.Second + time.Nanosecond, false, true},
		{"resolution-consumes-budget", 2 * time.Second, time.Second, false, true},
		{"wait-does-not-renew", 0, 0, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _, c := localIdentityFixture(t)
			clock := &waitClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			start := clock.Now()
			lookup := r.Lookup
			r.Lookup = func(name string) (myce.WorkerAccount, error) {
				clock.advance(tc.resolve)
				return lookup(name)
			}
			reads, pauses := 0, 0
			launch := func(_ context.Context, s myce.HostChildLaunch) ([]byte, error) {
				result := myce.HostMutationResult{HostRevision: myce.HostRevision{Revision: 1, Operation: "fixture-operation"}}
				switch s.Request.Operation {
				case "status":
					reads++
					e := myce.UnknownHostEnvelope("status", clock)
					e.Desired.Revision = &result.Revision
					e.Operation = &result.Operation
					host, boot, state := "fixture-host", "fixture-boot", "open"
					e.Host, e.Boot, e.Desired.State = &host, &boot, &state
					e.QueueRevision = &result.Revision
					at := clock.Now()
					for _, source := range []string{"local_intent", "queue", "required_workers"} {
						e.Sources[source] = &at
					}
					ready := !tc.wait
					e.Ready = &ready
					result.Envelope = &e
				case "resolve":
					clock.advance(tc.read)
				case "inspect-exclusion":
					return nil, myce.ErrHostChild
				default:
					t.Fatal("wait mutated state", s.Request.Operation)
				}
				return json.Marshal(struct {
					Version   int                     `json:"version"`
					Operation string                  `json:"operation"`
					Result    myce.HostMutationResult `json:"result"`
				}{1, s.Request.Operation, result})
			}
			var out bytes.Buffer
			err := hostEnvelopeCommandWithTiming([]string{"status", "--local", "--worker-user", "worker", "--config", c.Config, "--expect", "ready", "--wait", "--timeout", "3s", "--json"}, myce.MachineProbe{}, &out, r, launch, nil, nil, clock, func(_ context.Context, delay time.Duration) error {
				pauses++
				clock.advance(delay)
				return nil
			})
			e := validateEnvelopeOutput(t, out.Bytes())
			if tc.wantErr != errors.Is(err, myce.ErrHostDeadline) || !tc.wantErr && err != nil {
				t.Fatalf("deadline error=%v want deadline=%v", err, tc.wantErr)
			}
			// A dropped child can stop earlier to reserve its required two-second
			// exit margin. Neither polling nor that margin renews the deadline.
			if tc.wait && (pauses == 0 || clock.Now().After(start.Add(3*time.Second)) || reads < 2 || reads > 120) {
				t.Fatalf("deadline renewed: reads=%d pauses=%d elapsed=%v", reads, pauses, clock.Now().Sub(start))
			}
			if tc.wantErr && (len(e.Errors) == 0 || e.Errors[len(e.Errors)-1].Code != "deadline") {
				t.Fatal("missing typed deadline", e.Errors)
			}
		})
	}
}

type envelopeService struct{}

func (envelopeService) Stop(context.Context, time.Duration) error { return nil }
func (envelopeService) Empty(context.Context) (bool, error)       { return true, nil }
func envelopeHardware(_ context.Context, s myce.HostIntent) myce.HostExclusionReport {
	boot, _ := myce.HostBootID()
	now := time.Now().UTC()
	safe := true
	zero := 0
	return myce.HostExclusionReport{HostRevision: s.HostRevision, Boot: boot, Verdict: "PASS", RestartSafe: &safe, GPUHolders: &zero, RunningContainers: &zero, PendingOperations: &zero, At: now, Units: []myce.ExclusionUnit{{Unit: "myce-worker.service", Populated: new(bool)}}, Sources: map[string]*time.Time{"local_intent": &now, "cgroups": &now, "docker": &now, "gpu_holders": &now}, Errors: []myce.ExclusionError{}}
}
func TestHostEnvelopeLocalReleaseWhileQueuePending(t *testing.T) {
	r, c := envelopeFixture(t)
	handler := func(ctx context.Context, v myce.HostInvocation, m myce.HostMutation) (myce.HostMutationResult, error) {
		if m.Operation != "close" {
			t.Fatal("unexpected mutation", m.Operation)
		}
		report, err := (myce.HostControl{Dir: v.StateDir, FreshClose: true, CloseExpected: m.CloseExpected, Clock: myce.RealClock{}, Service: envelopeService{}, Observe: envelopeHardware}).Disable(ctx, m.Reason, m.Grace)
		return myce.HostMutationResult{HostRevision: report.HostRevision, CleanupConfirmed: report.Confirmed}, err
	}
	var out bytes.Buffer
	err := hostEnvelopeCommandWithObservation([]string{"close", "--local", "--config", c.Config, "--wait-released", "--grace", "0", "--timeout", "1s", "--json"}, myce.MachineProbe{}, &out, r, nil, handler, func(ctx context.Context, v myce.HostInvocation) (myce.HostExclusionReport, error) {
		s, err := myce.ReadHostIntent(v.StateDir)
		return envelopeHardware(ctx, s), err
	})
	e := validateEnvelopeOutput(t, out.Bytes())
	if err != nil || e.Accepted == nil || e.LocalExclusion != "PASS" || e.Settlement != "pending" || e.QueueRevision != nil {
		t.Fatal(e, err)
	}
	// Saved worker-owned bytes cannot promote an ordinary unprivileged status.
	e, err = invokeEnvelope(t, r, c, "status")
	if err != nil || e.LocalExclusion != "UNKNOWN" || e.Counts.GPUHolders != nil {
		t.Fatal(e, err)
	}
}

func TestHostEnvelopeRootCannotReuseWorkerExclusion(t *testing.T) {
	r, _, c := localIdentityFixture(t)
	revision := myce.HostRevision{Revision: 1, Operation: "fixture-operation"}
	forged := myce.UnknownHostEnvelope("status", myce.RealClock{})
	host, boot, state := "fixture-host", "fixture-boot", "closed"
	forged.Host = &host
	forged.Boot = &boot
	forged.Operation = &revision.Operation
	forged.Desired = myce.HostEnvelopeState{State: &state, Revision: &revision.Revision}
	forged.LocalExclusion = "PASS"
	var out bytes.Buffer
	err := hostLocalCommandWithIdentity([]string{"status", "--local", "--worker-user", "worker", "--config", c.Config, "--json"}, myce.MachineProbe{}, &out, r, func(_ context.Context, s myce.HostChildLaunch) ([]byte, error) {
		result := myce.HostMutationResult{HostRevision: revision}
		switch s.Request.Operation {
		case "status":
			result.Envelope = &forged
		case "resolve":
		case "inspect-exclusion":
			return nil, myce.ErrHostChild
		default:
			t.Fatal(s.Request.Operation)
		}
		return json.Marshal(struct {
			Version   int                     `json:"version"`
			Operation string                  `json:"operation"`
			Result    myce.HostMutationResult `json:"result"`
		}{1, s.Request.Operation, result})
	})
	e := validateEnvelopeOutput(t, out.Bytes())
	if err != nil || e.LocalExclusion != "UNKNOWN" || e.Counts.GPUHolders != nil || e.RestartSafe != nil {
		t.Fatal(e, err)
	}
}
