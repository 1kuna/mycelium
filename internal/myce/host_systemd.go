package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// HostService owns native processes; Docker containment supplies ConfirmContainers separately.
type HostService interface {
	Stop(context.Context, time.Duration) error
	Empty(context.Context) (bool, error)
}

type SystemdHostService struct {
	Clock    Clock
	killAt   time.Time
	Unit     string
	Command  func(context.Context, string, ...string) ([]byte, error)
	ReadFile func(string) ([]byte, error)
	group    string
	stopped  bool
}

func (s *SystemdHostService) call(ctx context.Context, args ...string) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if s.Command == nil {
		return nil, errors.New("systemd runner unavailable")
	}
	return s.Command(ctx, "systemctl", append([]string{"--user"}, args...)...)
}
func (s *SystemdHostService) show(ctx context.Context) (map[string]string, error) {
	b, e := s.call(ctx, "show", s.Unit, "-p", "LoadState", "-p", "ActiveState", "-p", "ControlGroup", "-p", "KillMode", "-p", "SendSIGKILL", "-p", "TimeoutStopUSec")
	if e != nil {
		return nil, e
	}
	p := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			p[k] = v
		}
	}
	if p["LoadState"] != "loaded" || p["KillMode"] != "control-group" || p["SendSIGKILL"] != "yes" {
		return nil, errors.New("worker unit must be loaded with KillMode=control-group and SendSIGKILL=yes; rerun myce install worker")
	}
	return p, nil
}
func (s *SystemdHostService) Stop(ctx context.Context, grace time.Duration) error {
	if !ValidWorkerUnit(s.Unit) {
		return errors.New("refusing non-Mycelium worker unit")
	}
	p, e := s.show(ctx)
	if e != nil {
		return e
	}
	// systemctl properties may straddle a worker's exit; do not certify that snapshot.
	for p["ControlGroup"] == "" && p["ActiveState"] != "inactive" && p["ActiveState"] != "failed" {
		select {
		case <-ctx.Done():
			return errors.New("active worker has no cgroup: " + ctx.Err().Error())
		case <-time.After(10 * time.Millisecond):
		}
		p, e = s.show(ctx)
		if e != nil {
			return e
		}
	}
	s.group = p["ControlGroup"]
	if s.group != "" {
		if _, e = s.cgroupPath(); e != nil {
			return e
		}
	}
	if grace == 0 {
		if s.group != "" {
			if _, e = s.call(ctx, "kill", "--kill-whom=all", "--signal=SIGKILL", s.Unit); e != nil {
				return e
			}
		}
	} else {
		if timeout, e := time.ParseDuration(p["TimeoutStopUSec"]); e == nil && grace > timeout {
			return errors.New("requested grace exceeds the unit stop timeout; configure TimeoutStopSec before disabling")
		}
		if s.Clock == nil {
			s.Clock = RealClock{}
		}
		s.killAt = s.Clock.Now().Add(grace)
	}
	// Confirmation polls the retained cgroup, never merely stop-request admission.
	if _, e = s.call(ctx, "stop", "--no-block", s.Unit); e != nil {
		return e
	}
	s.stopped = true
	return nil
}
func (s *SystemdHostService) cgroupPath() (string, error) {
	clean := filepath.Clean(s.group)
	if clean != s.group || !strings.HasPrefix(clean, "/") || clean == "/" || !strings.HasSuffix(clean, "/"+s.Unit) {
		return "", fmt.Errorf("unsafe worker cgroup %q", s.group)
	}
	return filepath.Join("/sys/fs/cgroup", clean), nil
}
func (s *SystemdHostService) Empty(ctx context.Context) (bool, error) {
	if !s.stopped {
		return false, errors.New("worker stop not requested")
	}
	p, e := s.show(ctx)
	if e != nil {
		return false, e
	}
	if p["ControlGroup"] != "" && p["ControlGroup"] != s.group {
		return false, errors.New("worker cgroup changed during disable")
	}
	if !s.killAt.IsZero() && !s.Clock.Now().Before(s.killAt) && p["ControlGroup"] != "" {
		if _, e = s.call(ctx, "kill", "--kill-whom=all", "--signal=SIGKILL", s.Unit); e != nil {
			return false, e
		}
		s.killAt = time.Time{}
	}
	if p["ActiveState"] != "inactive" && p["ActiveState"] != "failed" {
		return false, nil
	}
	if s.group == "" {
		return p["ControlGroup"] == "", nil
	}
	root, e := s.cgroupPath()
	if e != nil {
		return false, e
	}
	read := s.ReadFile
	if read == nil {
		read = os.ReadFile
	}
	events, e := read(filepath.Join(root, "cgroup.events"))
	if errors.Is(e, os.ErrNotExist) {
		return p["ControlGroup"] == "", nil
	}
	if e != nil {
		return false, e
	}
	// populated includes descendant cgroups; cgroup.procs alone does not.
	populated := ""
	for _, line := range strings.Split(string(events), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == "populated" {
			populated = f[1]
		}
	}
	if populated != "0" && populated != "1" {
		return false, errors.New("missing cgroup populated state")
	}
	procs, e := read(filepath.Join(root, "cgroup.procs"))
	if e != nil {
		return false, e
	}
	return populated == "0" && strings.TrimSpace(string(procs)) == "", nil
}
