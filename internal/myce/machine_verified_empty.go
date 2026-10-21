package myce

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

// MachineDrainOptions selects an explicit alternative to production close receipts.
type MachineDrainOptions struct {
	Mode          string
	WorkerConfigs []string
	// Filesystem boundary for laptop tests; never an operator-supplied root.
	cgroupRoot string
}

// This is historical installation evidence, never a reusable release receipt or
// permission to admit work. It is committed atomically with source retirement.
type VerifiedEmptyDrainReceipt struct {
	Machine           string         `json:"machine"`
	Session           string         `json:"session"`
	Host              string         `json:"host"`
	VerifiedAt        time.Time      `json:"verified_at"`
	Unit              string         `json:"unit"`
	UnitState         string         `json:"unit_state"`
	UnitSubState      string         `json:"unit_sub_state"`
	MainPID           int            `json:"main_pid"`
	Cgroup            string         `json:"cgroup"`
	CgroupEmpty       bool           `json:"cgroup_empty"`
	CgroupAbsent      bool           `json:"cgroup_absent"`
	SpoolDigest       string         `json:"spool_digest"`
	RunningContainers int            `json:"running_containers"`
	DockerCheckedAt   time.Time      `json:"docker_checked_at"`
	QueueCounts       map[string]int `json:"queue_counts"`
}

func verifiedEmptySource(receipts []VerifiedEmptyDrainReceipt, m *Machine) bool {
	return m != nil && m.Session != "" && slices.ContainsFunc(receipts, func(r VerifiedEmptyDrainReceipt) bool {
		return r.Machine == m.ID && r.Session == m.Session && r.Host == hostID(m)
	})
}

// The offline DB lock and every existing spool lock stay held through policy
// publication. No scheduler is running here; all daemon/filesystem I/O is outside
// q.mu. A worker that races a restart cannot acquire its spool and launch work.
func (q *Queue) verifyEmptyDrain(ctx context.Context, mapping []MachineRename, opts MachineDrainOptions, command func(context.Context, string, ...string) ([]byte, error)) (receipts []VerifiedEmptyDrainReceipt, release func(), err error) {
	locks := []*os.File{}
	release = func() {
		for _, lock := range locks {
			_ = lock.Close()
		}
	}
	defer func() {
		if err != nil {
			release()
		}
	}()
	refuse := func(kind, detail string) error { return migrationError("machine_drain_"+kind, detail) }
	counts := map[string]int{"queued": 0, "running": 0, "stopping": 0, "preparing": 0, "host_releases": 0, "obligations": 0}
	for _, j := range q.s.Jobs {
		if _, tracked := counts[j.State]; tracked && j.State != "host_releases" && j.State != "obligations" {
			counts[j.State]++
		}
	}
	for _, s := range q.s.Submissions {
		if s.Job != nil && q.s.Jobs[s.JobID] == nil && slices.Contains([]string{"queued", "running", "stopping", "preparing"}, s.Job.State) {
			counts[s.Job.State]++
		}
	}
	if counts["queued"]+counts["running"]+counts["stopping"]+counts["preparing"] != 0 {
		return nil, release, refuse("queue_not_empty", "queue contains pending jobs; freeze intake and settle first")
	}
	hosts := map[string]bool{}
	wanted := map[string]MachineRename{}
	for _, r := range mapping {
		hosts[r.To] = true
		wanted[r.From] = r
	}
	for host := range hosts {
		counts["obligations"] += len(q.hostObligations(host))
	}
	for _, r := range q.s.HostReleases {
		if hosts[r.Host] || hosts[q.executionHost(r.Machine)] {
			counts["host_releases"]++
		}
	}
	if counts["host_releases"]+counts["obligations"] != 0 {
		return nil, release, refuse("obligations_pending", "mapped hosts retain release or execution custody")
	}
	if len(mapping) == 0 || len(opts.WorkerConfigs) != len(mapping) || command == nil {
		return nil, release, refuse("evidence_missing", "all mapped local worker configs and a command probe are required")
	}
	root := opts.cgroupRoot
	if root == "" {
		root = "/sys/fs/cgroup"
	}
	if _, e := os.ReadFile(filepath.Join(root, "cgroup.controllers")); e != nil {
		return nil, release, refuse("cgroup_unreadable", "cgroup v2 evidence unavailable")
	}
	seen, units, spools := map[string]bool{}, map[string]bool{}, map[string]bool{}
	var snapshot Snapshot
	for _, j := range q.s.Jobs {
		snapshot.Jobs = append(snapshot.Jobs, j)
	}
	for _, path := range opts.WorkerConfigs {
		s, e := ReadServiceConfig(path)
		if e != nil {
			return nil, release, refuse("evidence_missing", "source config unreadable")
		}
		r, known := wanted[s.Name]
		m := q.s.Machines[s.Name]
		unit := filepath.Base(s.Service)
		if !known || seen[s.Name] || units[unit] || spools[s.State] || s.Config != path || s.Role != "worker" || s.Detection.OS != "linux" || !ValidWorkerUnit(unit) || m == nil || m.Devices != nil || m.Replacement != nil || m.Session == "" || s.HostID != r.To || hostID(m) != r.To || !filepath.IsAbs(s.State) {
			return nil, release, refuse("evidence_missing", "source identity, local unit or spool does not match mapping")
		}
		seen[s.Name], units[unit], spools[s.State] = true, true, true
		lock, e := os.OpenFile(filepath.Join(s.State, "worker.lock"), os.O_RDWR, 0)
		if e != nil {
			return nil, release, refuse("spool_unsettled", "source spool lock unavailable")
		}
		locks = append(locks, lock)
		if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return nil, release, refuse("spool_unsettled", "source spool is in use")
		}
		properties, e := command(ctx, "systemctl", "--user", "show", unit, "--all", "-p", "Id,LoadState,ActiveState,SubState,MainPID,ControlGroup,Slice,FragmentPath")
		if e != nil {
			return nil, release, refuse("unit_unreadable", "user unit inspection failed")
		}
		p := map[string]string{}
		for _, line := range strings.Split(strings.TrimSpace(string(properties)), "\n") {
			k, v, ok := strings.Cut(line, "=")
			if ok {
				p[k] = v
			}
		}
		for _, k := range []string{"Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup", "Slice", "FragmentPath"} {
			if _, ok := p[k]; !ok {
				return nil, release, refuse("unit_unreadable", "user unit properties missing")
			}
		}
		if p["Id"] != unit || p["LoadState"] != "loaded" || p["FragmentPath"] != s.Service {
			return nil, release, refuse("unit_unreadable", "user unit ownership is not proven")
		}
		if p["ActiveState"] != "inactive" || p["MainPID"] != "0" {
			return nil, release, refuse("unit_active", "source user unit must be inactive with MainPID zero")
		}
		jobs, e := command(ctx, "systemctl", "--user", "list-jobs", "--no-legend", "--no-pager")
		if e != nil {
			return nil, release, refuse("unit_unreadable", "pending user unit jobs unreadable")
		}
		for _, line := range strings.Split(string(jobs), "\n") {
			if slices.Contains(strings.Fields(line), unit) {
				return nil, release, refuse("unit_active", "source unit has a pending manager job")
			}
		}
		cg := p["ControlGroup"]
		if cg == "" {
			slice := p["Slice"]
			if filepath.Base(slice) != slice || !strings.HasSuffix(slice, ".slice") {
				return nil, release, refuse("cgroup_unreadable", "source slice unknown")
			}
			parent, e := command(ctx, "systemctl", "--user", "show", slice, "-p", "ControlGroup", "--value")
			if e != nil || strings.TrimSpace(string(parent)) == "" {
				return nil, release, refuse("cgroup_unreadable", "source slice cgroup unavailable")
			}
			cg = strings.TrimSpace(string(parent)) + "/" + unit
		}
		absent, e := emptyMigrationCgroup(root, cg)
		if e != nil {
			return nil, release, e
		}
		marker, e := os.ReadFile(filepath.Join(s.State, ".myce-format"))
		if e != nil || string(marker) != "v3\n" {
			return nil, release, refuse("spool_unsettled", "source spool format missing or unreadable")
		}
		data, e := os.ReadFile(filepath.Join(s.State, "state.json"))
		var state workerState
		if e != nil || decodeWorkerState(data, &state) != nil || state.Session != m.Session {
			return nil, release, refuse("spool_unsettled", "source spool session missing or differs from queue")
		}
		if e = inspectMigrationSpool(s.State, snapshot); e != nil {
			return nil, release, refuse("spool_unsettled", "C4 spool: "+e.Error())
		}
		digest, e := migrationSpoolDigest(s.State)
		if e != nil {
			return nil, release, refuse("spool_unsettled", "source spool digest unavailable")
		}
		endpoint := s.DockerEndpoint
		if endpoint == "" {
			endpoint = "unix:///var/run/docker.sock"
		}
		for _, label := range []string{"myce.worker=" + s.Name, "myce.attempt"} {
			out, e := command(ctx, "docker", "--host", endpoint, "ps", "--no-trunc", "--filter", "label="+label, "--format", "{{.ID}}")
			if e != nil {
				return nil, release, refuse("docker_inaccessible", "cannot inspect the source Docker endpoint")
			}
			if strings.TrimSpace(string(out)) != "" {
				return nil, release, refuse("container_running", "running Mycelium-labelled container remains")
			}
		}
		now := q.clock.Now()
		receipts = append(receipts, VerifiedEmptyDrainReceipt{Machine: m.ID, Session: m.Session, Host: r.To, VerifiedAt: now, Unit: unit, UnitState: p["ActiveState"], UnitSubState: p["SubState"], Cgroup: cg, CgroupEmpty: true, CgroupAbsent: absent, SpoolDigest: digest, DockerCheckedAt: now, QueueCounts: counts})
	}
	slices.SortFunc(receipts, func(a, b VerifiedEmptyDrainReceipt) int { return strings.Compare(a.Machine, b.Machine) })
	return receipts, release, nil
}

func emptyMigrationCgroup(root, group string) (bool, error) {
	refuse := func(kind string) error {
		return migrationError("machine_drain_cgroup_"+kind, "source cgroup must be readable and empty")
	}
	if !filepath.IsAbs(group) || group == "/" || filepath.Clean(group) != group {
		return false, refuse("unreadable")
	}
	path := filepath.Join(root, group)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		// Absence is evidence only if the containing slice is accessible.
		if _, err := os.ReadDir(filepath.Dir(path)); err != nil {
			return false, refuse("unreadable")
		}
		return true, nil
	} else if err != nil {
		return false, refuse("unreadable")
	}
	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.Type()&os.ModeSymlink != 0 {
			return refuse("unreadable")
		}
		if !d.IsDir() {
			return nil
		}
		data, e := os.ReadFile(filepath.Join(p, "cgroup.procs"))
		if e != nil {
			return refuse("unreadable")
		}
		if strings.TrimSpace(string(data)) != "" {
			return refuse("populated")
		}
		return nil
	})
	return false, err
}

func migrationSpoolDigest(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, entry := range entries {
		name := entry.Name()
		if name != ".myce-format" && !strings.HasSuffix(name, ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", errors.New("nonregular spool evidence")
		}
		file, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return "", err
		}
		header, _ := json.Marshal([]any{name, info.Size()})
		_, _ = hash.Write(header)
		_, err = io.Copy(hash, file)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return "", errors.Join(err, closeErr)
		}
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
