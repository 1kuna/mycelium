package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ExclusionError is deliberately independent of raw OS/daemon errors and paths.
type ExclusionError struct {
	Component string `json:"component"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Contrary  bool   `json:"contrary,omitempty"`
}
type ExclusionUnit struct {
	Unit      string  `json:"unit"`
	Cgroup    *string `json:"cgroup"`
	Populated *bool   `json:"populated"`
}
type HostExclusionReport struct {
	HostRevision
	Boot              string                `json:"boot_id"`
	Verdict           string                `json:"local_exclusion"`
	RestartSafe       *bool                 `json:"restart_safe"`
	RunningContainers *int                  `json:"running_containers"`
	GPUHolders        *int                  `json:"owned_gpu_holders"`
	PendingOperations *int                  `json:"pending_daemon_operations"`
	Units             []ExclusionUnit       `json:"units"`
	Errors            []ExclusionError      `json:"errors"`
	At                time.Time             `json:"observed_at"`
	Sources           map[string]*time.Time `json:"source_observed_at"`
	Digest            string                `json:"custody_digest"`
}

func (r *HostExclusionReport) problem(component, code string, contrary bool) {
	r.Errors = append(r.Errors, ExclusionError{component, code, "Required exclusion evidence: " + code, contrary})
	if contrary {
		r.Verdict = "FAIL"
	} else if r.Verdict != "FAIL" {
		r.Verdict = "UNKNOWN"
	}
}
func (r *HostExclusionReport) source(name string, at time.Time) { t := at.UTC(); r.Sources[name] = &t }

// This is pipe data from the trusted dropped child, never read by root from disk.
// No logs, results, credentials or environment values cross this boundary.
type HostExclusionInput struct {
	HostRevision
	Host       string                `json:"host_id"`
	Boot       string                `json:"boot_id"`
	Desired    string                `json:"desired"`
	Workers    []HostWorkerIdentity  `json:"workers"`
	Endpoints  []string              `json:"docker_endpoints"`
	Operations []HostDaemonOperation `json:"daemon_operations"`
	Native     []HostNativeIdentity  `json:"native_processes"`
	Problems   []ExclusionError      `json:"problems"`
}
type HostNativeIdentity struct {
	Identity string `json:"identity"`
	PID      int    `json:"pid"`
	Birth    string `json:"birth,omitempty"`
}
type HostWorkerIdentity struct {
	Unit    string `json:"unit"`
	Machine string `json:"machine"`
	Session string `json:"session"`
}
type HostDaemonOperation struct {
	Endpoint  string `json:"endpoint"`
	Identity  string `json:"identity"`
	Pending   bool   `json:"pending"`
	Released  bool   `json:"released"`
	Operation string `json:"operation"`
	Container string `json:"container"`
}

func exclusionDigest(in HostExclusionInput) string {
	b, _ := json.Marshal(in)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func readHostExclusionInput(v HostInvocation) (*HostExclusionInput, error) {
	if v.Privileged || os.Geteuid() == 0 {
		return nil, ErrHostIdentity
	}
	s, err := ReadHostIntent(v.StateDir)
	if err != nil {
		return nil, err
	}
	boot, err := HostBootID()
	if err != nil {
		return nil, err
	}
	in := &HostExclusionInput{HostRevision: s.HostRevision, Host: s.Host, Boot: boot, Desired: s.Desired, Workers: []HostWorkerIdentity{}, Operations: []HostDaemonOperation{}, Endpoints: []string{}, Problems: []ExclusionError{}}
	problem := func(component, code string) {
		in.Problems = append(in.Problems, ExclusionError{Component: component, Code: code, Message: "Required exclusion evidence: " + code})
	}
	if s.LocalExclusion != nil && s.LocalExclusion.Simulated {
		problem("kernel", "simulated_exclusion_unknown")
	}
	workers, err := HostWorkers(v.StateDir)
	if err != nil {
		problem("custody", "registry_unreadable")
	}
	if v.Config.DockerEndpoint == "" {
		problem("docker", "endpoint_unconfigured")
	} else {
		in.Endpoints = append(in.Endpoints, v.Config.DockerEndpoint)
	}
	// Installed config is required even before the first worker registers.
	if !ValidWorkerUnit(filepath.Base(v.Config.Service)) {
		problem("cgroups", "installed_unit_unknown")
	}
	in.Workers = append(in.Workers, HostWorkerIdentity{Unit: filepath.Base(v.Config.Service)})
	for _, w := range workers {
		if w.Host != s.Host || w.Name == "" || w.Session == "" {
			problem("custody", "worker_identity_incomplete")
		}
		in.Workers = append(in.Workers, HostWorkerIdentity{w.Unit, w.Name, w.Session})
		if !ValidWorkerUnit(w.Unit) {
			problem("cgroups", "registry_worker_without_unit")
		}
		// Another installed worker may use a different daemon before its first
		// custody record exists. Its config is read only in this dropped child.
		endpoint, configErr := configuredWorkerDockerEndpoint(w, v.Account.UID)
		if configErr != nil {
			problem("docker", "worker_endpoint_unconfigured")
		} else if !slices.Contains(in.Endpoints, endpoint) {
			in.Endpoints = append(in.Endpoints, endpoint)
		}
		if validateHostTarget(w.State, v.Account.UID, true) != nil {
			problem("custody", "spool_unreadable")
			continue
		}
		// ReadDir reports permission failures; Glob would silently hide them.
		entries, e := os.ReadDir(w.State)
		if e != nil {
			problem("custody", "spool_unreadable")
			continue
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".docker.json") {
				continue
			}
			b, e := readHostRegular(filepath.Join(w.State, entry.Name()), v.Account.UID, 1<<20)
			var st dockerExecution
			if e != nil || json.Unmarshal(b, &st) != nil || st.Owner == nil || st.Owner.ID == "" || st.Owner.Machine == "" || st.Owner.Session == "" || st.Owner.Attempt <= 0 || st.Identity == "" || entry.Name() != st.Identity+".docker.json" || st.Endpoint == "" {
				problem("custody", "docker_custody_unreadable")
				continue
			}
			if !slices.Contains(in.Endpoints, st.Endpoint) {
				in.Endpoints = append(in.Endpoints, st.Endpoint)
			}
			in.Operations = append(in.Operations, HostDaemonOperation{st.Endpoint, st.Identity, st.Pending || st.OperatorConfirmed, st.Released && !st.OperatorConfirmed, st.Operation, st.Container})
		}
	}
	// Enumerate process custody in the dropped child, with explicit directory errors.
	addRecord := func(path string) {
		b, e := readHostRegular(path, v.Account.UID, 1<<20)
		var st workerState
		if e != nil || decodeWorkerState(b, &st) != nil {
			problem("custody", "native_custody_unreadable")
			return
		}
		if st.PID > 0 && st.Identity == "" {
			problem("custody", "native_identity_incomplete")
			return
		}
		if st.Identity != "" {
			in.Native = append(in.Native, HostNativeIdentity{st.Identity, st.PID, st.Birth})
		}
	}
	scan := func(dir string, optional bool) {
		entries, e := os.ReadDir(dir)
		if optional && errors.Is(e, os.ErrNotExist) {
			return
		}
		if e != nil {
			problem("custody", "native_inventory_unreadable")
			return
		}
		for _, entry := range entries {
			n := entry.Name()
			if n == "state.json" || strings.HasSuffix(n, ".json") && (strings.HasPrefix(n, "attempt-") || strings.HasPrefix(n, "engine-")) {
				addRecord(filepath.Join(dir, n))
			}
		}
	}
	scan(hostCommandDirectory(v.StateDir), true)
	for _, w := range workers {
		scan(w.State, false)
		if w.Environments != "" {
			e := filepath.WalkDir(w.Environments, func(path string, d os.DirEntry, e error) error {
				if e != nil {
					return e
				}
				if d.Type()&os.ModeSymlink != 0 {
					return ErrHostTarget
				}
				if !d.IsDir() && d.Name() == "build-state.json" {
					addRecord(path)
				}
				return nil
			})
			if e != nil {
				problem("custody", "build_inventory_unreadable")
			}
		}
	}
	slices.SortFunc(in.Native, func(a, b HostNativeIdentity) int { return strings.Compare(a.Identity, b.Identity) })
	slices.Sort(in.Endpoints)
	slices.SortFunc(in.Workers, func(a, b HostWorkerIdentity) int {
		return strings.Compare(a.Unit+"\x00"+a.Machine+"\x00"+a.Session, b.Unit+"\x00"+b.Machine+"\x00"+b.Session)
	})
	slices.SortFunc(in.Operations, func(a, b HostDaemonOperation) int {
		return strings.Compare(a.Endpoint+"\x00"+a.Identity, b.Endpoint+"\x00"+b.Identity)
	})
	return in, nil
}

// Both observation input and cleanup resolve configured endpoints inside the
// worker boundary, including workers whose spools have no Docker custody yet.
func configuredWorkerDockerEndpoint(w HostWorker, uid uint32) (string, error) {
	b, err := readHostRegular(w.Config, uid, 1<<20)
	var config ServiceConfig
	if err != nil || json.Unmarshal(b, &config) != nil || config.Role != "worker" || config.HostID != w.Host || config.State != w.State || config.DockerEndpoint == "" {
		return "", ErrHostIdentity
	}
	return config.DockerEndpoint, nil
}

// HostKernelReaders is a read-only OS boundary. Production passes only readers
// of kernel-owned trees. It intentionally has no worker-file or mutation API.
type HostKernelReaders struct {
	ReadFile func(string) ([]byte, error)
	ReadDir  func(string) ([]os.DirEntry, error)
	Readlink func(string) (string, error)
	Device   func(string) (string, error) // major:minor of a character device, or empty
}

func LocalHostKernelReaders() HostKernelReaders {
	return HostKernelReaders{os.ReadFile, os.ReadDir, os.Readlink, hostDeviceNumber}
}

// ObserveHostExclusion never executes systemctl. User-manager output is a
// discovery hint at most, and cannot supply any positive exclusion evidence.
func ObserveHostExclusion(ctx context.Context, o HostObservationContext, in HostExclusionInput, clock Clock, k HostKernelReaders, docker func(context.Context, string, ...string) ([]byte, error)) HostExclusionReport {
	if clock == nil {
		clock = RealClock{}
	}
	start := clock.Now()
	r := HostExclusionReport{HostRevision: in.HostRevision, Boot: in.Boot, Verdict: "PASS", At: start.UTC(), Digest: exclusionDigest(in), Errors: []ExclusionError{}, Units: []ExclusionUnit{}, Sources: map[string]*time.Time{"local_intent": nil, "cgroups": nil, "docker": nil, "gpu_holders": nil}}
	for _, e := range in.Problems {
		r.problem(e.Component, e.Code, e.Contrary)
	}
	if in.Desired != "closed" {
		r.problem("local_intent", "launch_gates_open", true)
	} else if in.Revision == 0 || in.Operation == "" || in.Host == "" || in.Boot == "" {
		r.problem("local_intent", "state_incomplete", false)
	} else {
		r.source("local_intent", start)
	}
	pending := 0
	for _, op := range in.Operations {
		if op.Pending {
			pending++
			r.problem("docker", "daemon_operation_pending", false)
		}
		if !op.Released {
			r.problem("docker", "custody_release_unconfirmed", false)
		}
	}
	r.PendingOperations = &pending
	if !o.Privileged {
		r.problem("gpu_holders", "permission_denied", false)
		r.problem("cgroups", "privileged_observation_required", false)
	}
	if k.ReadFile == nil || k.ReadDir == nil || k.Readlink == nil || k.Device == nil {
		r.problem("kernel", "readers_unavailable", false)
		return r
	}
	boot, err := k.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) != in.Boot {
		r.problem("kernel", "boot_changed", false)
	}
	// Scan the account's whole kernel cgroup tree rather than trusting a manager's
	// claimed ControlGroup (or a missing cgroup reported by the worker).
	root := fmt.Sprintf("/sys/fs/cgroup/user.slice/user-%d.slice", o.Account.UID)
	groups := map[string]bool{}
	units := map[string]bool{}
	for _, w := range in.Workers {
		if ValidWorkerUnit(w.Unit) {
			units[w.Unit] = true
		} else {
			r.problem("cgroups", "registry_worker_without_unit", false)
		}
	}
	if len(units) == 0 {
		r.problem("cgroups", "worker_inventory_incomplete", false)
	}
	cgroupOK := true
	var walk func(string, bool)
	walk = func(path string, owned bool) {
		if ctx.Err() != nil {
			cgroupOK = false
			return
		}
		name := filepath.Base(path)
		if ValidWorkerUnit(name) {
			owned = true
			groups[strings.TrimPrefix(path, "/sys/fs/cgroup")] = true
		}
		if owned {
			events, e1 := k.ReadFile(filepath.Join(path, "cgroup.events"))
			procs, e2 := k.ReadFile(filepath.Join(path, "cgroup.procs"))
			populated, valid := cgroupPopulated(events)
			if e1 != nil || e2 != nil || !valid {
				cgroupOK = false
				r.problem("cgroups", "cgroup_unreadable", false)
			}
			if valid && populated || e2 == nil && len(strings.Fields(string(procs))) > 0 {
				r.problem("cgroups", "owned_cgroup_populated", true)
			}
			if ValidWorkerUnit(name) {
				cg := strings.TrimPrefix(path, "/sys/fs/cgroup")
				var p *bool
				if e1 == nil && valid {
					p = &populated
				}
				r.Units = append(r.Units, ExclusionUnit{name, &cg, p})
				delete(units, name)
			}
		}
		entries, e := k.ReadDir(path)
		if e != nil {
			cgroupOK = false
			r.problem("cgroups", "cgroup_inventory_incomplete", false)
			return
		}
		for _, entry := range entries {
			if entry.Type()&os.ModeSymlink != 0 {
				cgroupOK = false
				r.problem("cgroups", "cgroup_inventory_incomplete", false)
			} else if entry.IsDir() {
				walk(filepath.Join(path, entry.Name()), owned)
			}
		}
	}
	walk(root, false)
	for unit := range units {
		empty := false
		if cgroupOK {
			r.Units = append(r.Units, ExclusionUnit{Unit: unit, Populated: &empty})
		} else {
			r.Units = append(r.Units, ExclusionUnit{Unit: unit})
		}
	}
	if cgroupOK {
		r.source("cgroups", clock.Now())
	}
	// Complete label inventory includes retained exited objects and orphan sidecars.
	// Every endpoint is explicit; caller environment never chooses one here.
	live := 0
	dockerOK := true
	restartSafe := in.Desired == "closed"
	containerPIDs := map[int]string{}
	if len(in.Endpoints) == 0 {
		dockerOK = false
		r.problem("docker", "endpoint_unconfigured", false)
	}
	for _, endpoint := range in.Endpoints {
		if docker == nil {
			dockerOK = false
			r.problem("docker", "daemon_unavailable", false)
			continue
		}
		cs, e := inspectOwnedDocker(ctx, endpoint, "", docker)
		if e != nil {
			dockerOK = false
			r.problem("docker", "inventory_incomplete", false)
		}
		for _, c := range cs {
			if dockerLive(c) {
				live++
				r.problem("docker", "owned_container_live", true)
			}
			if c.HostConfig.RestartPolicy == nil {
				restartSafe = false
				dockerOK = false
				r.problem("docker", "restart_policy_unknown", false)
			} else if c.HostConfig.RestartPolicy.Name != "no" {
				restartSafe = false
				r.problem("docker", "unsafe_restart_policy", true)
			}
			if c.State.Pid > 0 {
				containerPIDs[c.State.Pid] = c.ID
				raw, err := k.ReadFile(fmt.Sprintf("/proc/%d/cgroup", c.State.Pid))
				cg, ce := unifiedCgroup(raw)
				if err != nil || ce != nil || cg == "/" {
					r.problem("gpu_holders", "container_cgroup_unknown", false)
				} else {
					groups[cg] = true
				}
			}
		}
	}
	if dockerOK {
		r.RunningContainers = &live
		r.source("docker", clock.Now())
	}
	if cgroupOK && dockerOK && pending == 0 && in.Desired == "closed" {
		r.RestartSafe = &restartSafe
	}
	// Process identity is checked around each FD scan. Ownership comes from root's
	// cgroup read or the host PID of an exactly labelled container, never a name.
	holders, gpuOK := observeGPUHolders(ctx, k, groups, containerPIDs, in.Native, strings.TrimPrefix(root, "/sys/fs/cgroup"), &r)
	if gpuOK {
		r.GPUHolders = &holders
		r.source("gpu_holders", clock.Now())
	}
	// Recheck containers after the proc walk: a competing manual start is FAIL.
	finalLive := 0
	finalDockerOK := dockerOK
	for _, endpoint := range in.Endpoints {
		if docker == nil {
			continue
		}
		cs, e := inspectOwnedDocker(ctx, endpoint, "", docker)
		if e != nil {
			r.problem("docker", "inventory_incomplete", false)
			finalDockerOK = false
			r.RunningContainers = nil
			r.RestartSafe = nil
		}
		for _, c := range cs {
			if dockerLive(c) {
				finalLive++
				r.problem("docker", "owned_container_live", true)
			}
			if c.HostConfig.RestartPolicy == nil {
				r.problem("docker", "restart_policy_unknown", false)
				r.RestartSafe = nil
			} else if c.HostConfig.RestartPolicy.Name != "no" {
				r.problem("docker", "unsafe_restart_policy", true)
				safe := false
				r.RestartSafe = &safe
			}
		}
	}
	if finalDockerOK {
		r.RunningContainers = &finalLive
		r.source("docker", clock.Now())
	}
	// A second kernel walk catches late unit creation and descendant population.
	unitCount := len(r.Units)
	walk(root, false)
	r.Units = r.Units[:unitCount]
	if !cgroupOK {
		r.Sources["cgroups"] = nil
		r.RestartSafe = nil
	}
	endBoot, e := k.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil || strings.TrimSpace(string(endBoot)) != in.Boot || ctx.Err() != nil || clock.Now().Sub(start) > 10*time.Second {
		r.problem("kernel", "observation_changed_or_expired", false)
	}
	if r.RestartSafe == nil {
		r.problem("restart", "restart_safety_unconfirmed", false)
	}
	r.At = clock.Now().UTC()
	if !o.Privileged && r.Verdict == "PASS" {
		r.problem("kernel", "privileged_observation_required", false)
	}
	return r
}
func cgroupPopulated(b []byte) (bool, bool) {
	found := false
	value := false
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 0 && f[0] == "populated" {
			if found || len(f) != 2 || f[1] != "0" && f[1] != "1" {
				return false, false
			}
			found = true
			value = f[1] == "1"
		}
	}
	return value, found
}
func unifiedCgroup(b []byte) (string, error) {
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "0::/") {
			p := strings.TrimPrefix(line, "0::")
			if filepath.Clean(p) != p {
				break
			}
			return p, nil
		}
	}
	return "", errors.New("cgroup identity unavailable")
}
func ownedCgroup(path string, groups map[string]bool) bool {
	for g := range groups {
		if path == g || strings.HasPrefix(path, g+"/") {
			return true
		}
	}
	return false
}
func procBirth(b []byte) string {
	_, tail, ok := strings.Cut(string(b), ") ")
	if !ok {
		return ""
	}
	f := strings.Fields(tail)
	if len(f) < 20 {
		return ""
	}
	return f[19]
}

func observeGPUHolders(ctx context.Context, k HostKernelReaders, groups map[string]bool, containerPIDs map[int]string, native []HostNativeIdentity, accountRoot string, r *HostExclusionReport) (int, bool) {
	ok := true
	count := 0
	problem := func(code string) { ok = false; r.problem("gpu_holders", code, false) }
	// /proc/devices supplies driver major mappings even for containers whose
	// device nodes are absent from root's /dev mount. No process-name matching.
	drivers, e := k.ReadFile("/proc/devices")
	majors := map[string]bool{}
	if e != nil {
		problem("device_mapping_unreadable")
	} else {
		character := false
		headers := 0
		for _, line := range strings.Split(string(drivers), "\n") {
			if line == "Character devices:" {
				character = true
				headers++
				continue
			}
			if line == "Block devices:" {
				character = false
				headers++
			}
			f := strings.Fields(line)
			if character && len(f) == 2 && (f[1] == "drm" || strings.HasPrefix(f[1], "nvidia") || f[1] == "kfd" || f[1] == "accel") {
				if _, err := strconv.ParseUint(f[0], 10, 32); err != nil {
					problem("device_mapping_incomplete")
				} else {
					majors[f[0]] = true
				}
			}
		}
		if headers != 2 {
			problem("device_mapping_incomplete")
		}
	}
	entries, e := k.ReadDir("/proc")
	if e != nil {
		problem("proc_inventory_unreadable")
		return 0, false
	}
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || !entry.IsDir() {
			continue
		}
		if ctx.Err() != nil {
			problem("observation_expired")
			break
		}
		base := filepath.Join("/proc", entry.Name())
		stat, e1 := k.ReadFile(base + "/stat")
		cg, e2 := k.ReadFile(base + "/cgroup")
		if errors.Is(e1, os.ErrNotExist) && errors.Is(e2, os.ErrNotExist) {
			problem("pid_churn")
			continue
		}
		path, e3 := unifiedCgroup(cg)
		if e1 != nil || e2 != nil || e3 != nil || procBirth(stat) == "" {
			problem("process_identity_unreadable")
			continue
		}
		owned := ownedCgroup(path, groups) || containerPIDs[pid] != ""
		// Also recognize an owned unit created after the cgroup walk.
		for _, part := range strings.Split(path, "/") {
			if strings.HasPrefix(path, accountRoot+"/") && ValidWorkerUnit(part) {
				owned = true
			}
		}
		if len(native) > 0 {
			env, e1 := k.ReadFile(base + "/environ")
			argv, e2 := k.ReadFile(base + "/cmdline")
			if e1 != nil || e2 != nil {
				problem("native_identity_unreadable")
			}
			for _, n := range native {
				match := slices.Contains(strings.Split(string(env), "\x00"), "MYCE_PROCESS_ID="+n.Identity) || slices.Contains(strings.Split(string(argv), "\x00"), "myce-supervisor-"+n.Identity)
				if match || n.PID == pid && n.Birth != "" && n.Birth == procBirth(stat) {
					owned = true
				}
			}
		}
		if !owned {
			continue
		}
		// A live owned process outside the empty service tree is contrary evidence too.
		_, tail, _ := strings.Cut(string(stat), ") ")
		if !strings.HasPrefix(tail, "Z ") {
			r.problem("cgroups", "owned_process_live", true)
		}
		if _, e := k.Readlink(base + "/exe"); e != nil {
			problem("executable_unreadable")
		}
		ns, e := k.Readlink(base + "/ns/pid")
		if e != nil {
			problem("namespace_unreadable")
		}
		fds, e := k.ReadDir(base + "/fd")
		if e != nil {
			problem("permission_denied")
			continue
		}
		holder := false
		for _, fd := range fds {
			device, e := k.Device(base + "/fd/" + fd.Name())
			if e != nil {
				problem("fd_unreadable")
				continue
			}
			major, _, isdev := strings.Cut(device, ":")
			if isdev && majors[major] {
				holder = true
			}
		}
		if holder {
			count++
			r.problem("gpu_holders", "owned_gpu_holder", true)
		}
		after, e1 := k.ReadFile(base + "/stat")
		afterCG, e2 := k.ReadFile(base + "/cgroup")
		afterNS, e3 := k.Readlink(base + "/ns/pid")
		if e1 != nil || e2 != nil || e3 != nil || procBirth(after) != procBirth(stat) || string(afterCG) != string(cg) || ns != afterNS {
			problem("pid_or_namespace_changed")
		}
	}
	after, e := k.ReadDir("/proc")
	if e != nil {
		problem("proc_inventory_unreadable")
	} else {
		ids := func(es []os.DirEntry) []string {
			var out []string
			for _, e := range es {
				if _, err := strconv.Atoi(e.Name()); err == nil && e.IsDir() {
					out = append(out, e.Name())
				}
			}
			slices.Sort(out)
			return out
		}
		if !slices.Equal(ids(entries), ids(after)) {
			problem("proc_inventory_changed")
		}
	}
	return count, ok
}

// recordHostExclusion runs only inside hostMutationExecutor. Lock ordering is
// the same as close/open; no daemon, network or bulk I/O runs under intake.lock.
func recordHostExclusion(ctx context.Context, v HostInvocation, r *HostExclusionReport, clock Clock) error {
	if r == nil || r.Digest == "" || r.At.IsZero() || r.Verdict != "PASS" && r.Verdict != "FAIL" && r.Verdict != "UNKNOWN" {
		return ErrHostChild
	}
	if clock == nil {
		clock = RealClock{}
	}
	age := clock.Now().Sub(r.At)
	if age < 0 || age > 10*time.Second {
		return ErrHostRevision
	}
	if r.Verdict == "PASS" && !r.completePass() {
		return ErrHostChild
	}
	unlock, err := hostOperationLockContext(ctx, v.StateDir, "reconcile.lock")
	if err != nil {
		return err
	}
	defer unlock()
	in, err := readHostExclusionInput(v)
	if err != nil {
		return err
	}
	if exclusionDigest(*in) != r.Digest || in.Boot != r.Boot || in.HostRevision != r.HostRevision || in.Desired != "closed" {
		return ErrHostRevision
	}
	return mutateHostIntent(v.StateDir, r.HostRevision, func(s *HostIntent) error {
		errors := []string{}
		for _, e := range r.Errors {
			errors = append(errors, e.Code)
		}
		sessions := []HostSession{}
		if r.Verdict == "PASS" {
			for _, w := range in.Workers {
				if w.Machine != "" && w.Session != "" && ValidWorkerUnit(w.Unit) {
					sessions = append(sessions, HostSession{w.Machine, w.Session})
				}
			}
		}
		released := []HostObligation{}
		if s.LocalExclusion != nil {
			released = s.LocalExclusion.Released
		}
		s.LocalExclusion = &HostLocalExclusion{HostObservation: HostObservation{HostRevision: r.HostRevision, Boot: r.Boot, State: "closed", At: r.At}, Evidence: r, Confirmed: r.Verdict == "PASS", Errors: errors, Released: released, Sessions: sessions}
		if r.Verdict == "PASS" {
			s.Observed = &s.LocalExclusion.HostObservation
		} else if s.LastClose != nil {
			s.LastClose.Settlement = "pending"
			s.LastClose.SettledAt = nil
		}
		return nil
	})
}

// A boolean or a verdict string alone is never exclusion evidence.
func (r *HostExclusionReport) completePass() bool {
	if r == nil || r.Verdict != "PASS" || len(r.Errors) > 0 || r.At.IsZero() || r.RestartSafe == nil || !*r.RestartSafe || r.RunningContainers == nil || *r.RunningContainers != 0 || r.GPUHolders == nil || *r.GPUHolders != 0 || r.PendingOperations == nil || *r.PendingOperations != 0 || len(r.Units) == 0 {
		return false
	}
	for _, name := range []string{"local_intent", "cgroups", "docker", "gpu_holders"} {
		if r.Sources[name] == nil || r.Sources[name].IsZero() || r.Sources[name].After(r.At) || r.At.Sub(*r.Sources[name]) > 10*time.Second {
			return false
		}
	}
	for _, u := range r.Units {
		if !ValidWorkerUnit(u.Unit) || u.Populated == nil || *u.Populated {
			return false
		}
	}
	return true
}
