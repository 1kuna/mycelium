package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func exclusionFixture(t *testing.T) (HostExclusionInput, HostObservationContext, HostKernelReaders, fstest.MapFS, map[string]string) {
	t.Helper()
	data := fstest.MapFS{
		"proc/sys/kernel/random/boot_id": {Data: []byte("boot")},
		"proc/devices":                   {Data: []byte("Character devices:\n226 drm\n195 nvidia\n511 nvidia-uvm\nBlock devices:\n")},
		"proc/100/stat":                  {Data: []byte("100 (generic process) S " + strings.Repeat("0 ", 18) + "42")},
		"proc/100/cgroup":                {Data: []byte("0::/unrelated.service\n")},
		"proc/100/fd":                    {Mode: fs.ModeDir},
		"sys/fs/cgroup/user.slice/user-1000.slice/myce-worker.service/cgroup.events":        {Data: []byte("populated 0\n")},
		"sys/fs/cgroup/user.slice/user-1000.slice/myce-worker.service/cgroup.procs":         {},
		"sys/fs/cgroup/user.slice/user-1000.slice/myce-worker.service/engine/cgroup.events": {Data: []byte("populated 0\n")},
		"sys/fs/cgroup/user.slice/user-1000.slice/myce-worker.service/engine/cgroup.procs":  {},
	}
	devices := map[string]string{}
	k := HostKernelReaders{
		ReadFile: func(path string) ([]byte, error) { return fs.ReadFile(data, strings.TrimPrefix(path, "/")) },
		ReadDir:  func(path string) ([]os.DirEntry, error) { return fs.ReadDir(data, strings.TrimPrefix(path, "/")) },
		Readlink: func(path string) (string, error) {
			if strings.HasSuffix(path, "/exe") {
				return "/usr/bin/generic", nil
			}
			if strings.HasSuffix(path, "/ns/pid") {
				return "pid:[1]", nil
			}
			return "", os.ErrNotExist
		},
		Device: func(path string) (string, error) { return devices[path], nil },
	}
	in := HostExclusionInput{HostRevision: HostRevision{Revision: 7, Operation: "close"}, Host: "example", Boot: "boot", Desired: "closed", Workers: []HostWorkerIdentity{{Unit: "myce-worker.service", Machine: "worker", Session: "session"}}, Endpoints: []string{"unix:///example.sock"}}
	return in, HostObservationContext{Account: WorkerAccount{UID: 1000}, Privileged: true}, k, data, devices
}
func emptyDocker(context.Context, string, ...string) ([]byte, error) { return nil, nil }
func exclusionCodes(r HostExclusionReport) []string {
	var out []string
	for _, e := range r.Errors {
		out = append(out, e.Code)
	}
	return out
}
func TestExclusionKernelAndGPU(t *testing.T) {
	for _, mode := range []string{"empty_descendants", "drm_holder", "cuda_holder", "unrelated_display", "other_owner", "permission_denied", "holder_and_missing_source", "pid_churn", "namespace_churn", "populated_descendant", "unknown_endpoint", "open_gates", "pending_operation", "worker_without_unit", "nonroot", "reboot"} {
		t.Run(mode, func(t *testing.T) {
			in, o, k, data, devices := exclusionFixture(t)
			want := "PASS"
			wantCode := ""
			owner := "0::/user.slice/user-1000.slice/myce-worker.service/engine\n"
			switch mode {
			case "drm_holder", "cuda_holder", "holder_and_missing_source":
				data["proc/100/cgroup"].Data = []byte(owner)
				data["proc/100/fd/3"] = &fstest.MapFile{}
				devices["/proc/100/fd/3"] = "226:128"
				if mode == "cuda_holder" {
					devices["/proc/100/fd/3"] = "511:0"
				}
				want = "FAIL"
				wantCode = "owned_gpu_holder"
				if mode == "holder_and_missing_source" {
					read := k.ReadFile
					k.ReadFile = func(path string) ([]byte, error) {
						if strings.HasSuffix(path, "/cgroup.events") {
							return nil, os.ErrPermission
						}
						return read(path)
					}
				}
			case "unrelated_display", "other_owner":
				data["proc/100/fd/3"] = &fstest.MapFile{}
				devices["/proc/100/fd/3"] = "226:0"
				if mode == "other_owner" {
					data["proc/100/cgroup"].Data = []byte("0::/user.slice/user-2000.slice/myce-worker.service\n")
				}
			case "permission_denied":
				data["proc/100/stat"].Data = []byte("100 (generic process) Z " + strings.Repeat("0 ", 18) + "42")
				data["proc/100/cgroup"].Data = []byte(owner)
				read := k.ReadDir
				k.ReadDir = func(path string) ([]os.DirEntry, error) {
					if strings.HasSuffix(path, "/fd") {
						return nil, os.ErrPermission
					}
					return read(path)
				}
				want = "UNKNOWN"
				wantCode = "permission_denied"
			case "pid_churn":
				data["proc/100/cgroup"].Data = []byte(owner)
				read := k.ReadFile
				n := 0
				k.ReadFile = func(path string) ([]byte, error) {
					if path == "/proc/100/stat" {
						n++
						if n > 1 {
							return []byte("100 (generic) S " + strings.Repeat("0 ", 18) + "43"), nil
						}
					}
					return read(path)
				}
				want = "FAIL"
				wantCode = "pid_or_namespace_changed"
			case "namespace_churn":
				data["proc/100/cgroup"].Data = []byte(owner)
				read := k.Readlink
				n := 0
				k.Readlink = func(path string) (string, error) {
					if strings.HasSuffix(path, "/ns/pid") {
						n++
						if n > 1 {
							return "pid:[2]", nil
						}
					}
					return read(path)
				}
				want = "FAIL"
				wantCode = "pid_or_namespace_changed"
			case "populated_descendant":
				data["sys/fs/cgroup/user.slice/user-1000.slice/myce-worker.service/engine/cgroup.events"].Data = []byte("populated 1\n")
				want = "FAIL"
				wantCode = "owned_cgroup_populated"
			case "unknown_endpoint":
				in.Endpoints = append(in.Endpoints, "unavailable")
				want = "UNKNOWN"
				wantCode = "inventory_incomplete"
			case "open_gates":
				in.Desired = "open"
				want = "FAIL"
				wantCode = "launch_gates_open"
			case "pending_operation":
				in.Operations = []HostDaemonOperation{{Identity: "attempt", Pending: true, Released: true}}
				want = "UNKNOWN"
				wantCode = "daemon_operation_pending"
			case "worker_without_unit":
				in.Workers = append(in.Workers, HostWorkerIdentity{Machine: "other", Session: "session"})
				want = "UNKNOWN"
				wantCode = "registry_worker_without_unit"
			case "nonroot":
				o.Privileged = false
				want = "UNKNOWN"
				wantCode = "permission_denied"
			case "reboot":
				data["proc/sys/kernel/random/boot_id"].Data = []byte("next-boot")
				want = "UNKNOWN"
				wantCode = "boot_changed"
			}
			call := func(ctx context.Context, endpoint string, args ...string) ([]byte, error) {
				if endpoint == "unavailable" {
					return nil, os.ErrPermission
				}
				return emptyDocker(ctx, endpoint, args...)
			}
			r := ObserveHostExclusion(context.Background(), o, in, RealClock{}, k, call)
			if r.Verdict != want || wantCode != "" && !slices.Contains(exclusionCodes(r), wantCode) {
				t.Fatalf("%s: %+v", want, r)
			}
			if mode == "holder_and_missing_source" && !slices.Contains(exclusionCodes(r), "cgroup_unreadable") {
				t.Fatal("lost missing-source cause", r)
			}
			if want == "PASS" && (r.GPUHolders == nil || *r.GPUHolders != 0 || r.RestartSafe == nil || !*r.RestartSafe) {
				t.Fatal("incomplete PASS", r)
			}
		})
	}
}
func ownedDockerObject(status, policy string) dockerInspection {
	c := dockerInspection{ID: "container", Name: "/myce-attempt"}
	c.Config.Labels = map[string]string{"myce.attempt": "attempt"}
	live := status == "running" || status == "paused" || status == "restarting"
	c.State.Running = &live
	c.State.Status = status
	c.HostConfig.RestartPolicy = &dockerRestartPolicy{Name: policy}
	return c
}
func TestExclusionDockerStatesAndManualStart(t *testing.T) {
	for _, mode := range []string{"exited", "paused", "restarting", "manual_start", "unsafe_policy", "policy_missing", "orphan_sidecar"} {
		t.Run(mode, func(t *testing.T) {
			in, o, k, _, _ := exclusionFixture(t)
			status := "exited"
			policy := "no"
			want := "PASS"
			if mode == "paused" || mode == "restarting" {
				status = mode
				want = "FAIL"
			}
			if mode == "manual_start" || mode == "orphan_sidecar" {
				status = "running"
				want = "FAIL"
			}
			if mode == "unsafe_policy" {
				policy = "always"
				want = "FAIL"
			}
			c := ownedDockerObject(status, policy)
			if mode == "policy_missing" {
				c.HostConfig.RestartPolicy = nil
				want = "UNKNOWN"
			}
			call := func(_ context.Context, _ string, args ...string) ([]byte, error) {
				if args[0] == "ps" {
					return []byte("container"), nil
				}
				if args[0] != "inspect" {
					t.Fatal("observation mutated daemon", args)
				}
				return json.Marshal([]dockerInspection{c})
			}
			r := ObserveHostExclusion(context.Background(), o, in, RealClock{}, k, call)
			if r.Verdict != want {
				t.Fatal(want, r)
			}
		})
	}
}
func TestExclusionOldAndSimulatedEvidenceRejectedByProduction(t *testing.T) {
	q, clock := fixture(t)
	if _, dev := q.clock.(*devClock); dev {
		t.Fatal("normal production constructor enabled dev proof")
	}
	dir := t.TempDir()
	s := testHostOpen(t, dir, clock)
	boot, _ := HostBootID()
	s.LocalExclusion = &HostLocalExclusion{HostObservation: HostObservation{HostRevision: s.HostRevision, Boot: boot}, Confirmed: true}
	if localExclusionConfirmed(s, boot) {
		t.Fatal("S05 cleanup-only evidence accepted")
	}
	s.LocalExclusion.Simulated = true
	if localExclusionConfirmed(s, boot) {
		t.Fatal("production accepted simulated proof")
	}
	if !localExclusionConfirmed(s, boot, true) {
		t.Fatal("explicit dev host rejected simulated proof")
	}
	request := closeExchange(s.Host, clock)
	request.Simulated = true
	request.Sessions = []HostSession{{Machine: "worker", Session: "session"}}
	if _, err := q.ReconcileHost(request); err == nil || !strings.Contains(errors.Unwrap(err).Error(), "simulated_exclusion_unknown") {
		t.Fatal("production reconcile accepted simulation", err)
	}
	q.clock = &devClock{}
	if _, err := q.ReconcileHost(request); err != nil {
		t.Fatal("dev queue rejected its own simulation", err)
	}
	in, o, k, _, _ := exclusionFixture(t)
	in.Problems = []ExclusionError{{Component: "kernel", Code: "simulated_exclusion_unknown"}}
	if r := ObserveHostExclusion(context.Background(), o, in, clock, k, emptyDocker); r.Verdict != "UNKNOWN" {
		t.Fatal(r)
	}
}
func TestExclusionInputAndRecordingAreCustodyFenced(t *testing.T) {
	_, v := hostIdentityFixture(t)
	v.Privileged = false
	if err := closeTestHost(v.StateDir); err != nil {
		t.Fatal(err)
	}
	v.Config.DockerEndpoint = "unix:///example.sock"
	v.Config.Service = filepath.Join(v.Account.Home, "myce-worker.service")
	before := snapshotHostSecurityFiles(t, v.StateDir)
	in, err := readHostExclusionInput(v)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, snapshotHostSecurityFiles(t, v.StateDir)) {
		t.Fatal("inspect wrote state")
	}
	report := testHostExclusion(context.Background(), HostIntent{HostRevision: in.HostRevision, AcceptedAt: RealClock{}.Now()})
	report.Digest = exclusionDigest(*in)
	report.At = RealClock{}.Now()
	if err = recordHostExclusion(context.Background(), v, &report, RealClock{}); err != nil {
		t.Fatal(err)
	}
	saved, err := ReadHostIntent(v.StateDir)
	if err != nil || !localExclusionConfirmed(saved, in.Boot) {
		t.Fatal(saved, err)
	}
	if err = AdoptHostWorker(v.StateDir, HostWorker{Host: saved.Host, State: t.TempDir(), Name: "worker", Session: "new", Unit: "myce-worker.service"}); err != nil {
		t.Fatal(err)
	}
	if err = recordHostExclusion(context.Background(), v, &report, RealClock{}); !errors.Is(err, ErrHostRevision) {
		t.Fatal("stale snapshot recorded", err)
	}
	report.At = RealClock{}.Now().Add(-11 * time.Second)
	if err = recordHostExclusion(context.Background(), v, &report, RealClock{}); !errors.Is(err, ErrHostRevision) {
		t.Fatal("expired observation recorded", err)
	}
}
