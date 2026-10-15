package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const DefaultContainerRetention = 24 * time.Hour

type dockerAcknowledgement struct {
	Worker string    `json:"worker"`
	At     time.Time `json:"at"`
}

func acknowledgeDocker(s *workerStore, worker string, now time.Time) error {
	path := filepath.Join(s.dir, s.state.Identity+".docker.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var st dockerExecution
	if err = json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Identity != s.state.Identity || !st.Released || st.Pending {
		return nil
	}
	// Preserve the first acknowledgement across replay after a crash.
	ackPath := filepath.Join(s.dir, st.Identity+".docker-ack.json")
	if _, err = os.Stat(ackPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return saveWorkerJSON(s.dir, st.Identity+".docker-ack.json", dockerAcknowledgement{Worker: worker, At: now})
}

// No force flag: Docker refuses removal if a stopped container starts after inspection.
func pruneStoppedDocker(ctx context.Context, dir, worker, active string, now time.Time, retention time.Duration, caller func(*dockerAttempt) func(...string) ([]byte, error)) error {
	paths, err := filepath.Glob(filepath.Join(dir, "*.docker-ack.json"))
	if err != nil {
		return err
	}
	var failures []error
	for _, path := range paths {
		if ctx.Err() != nil {
			return errors.Join(append(failures, ctx.Err())...)
		}
		identity := strings.TrimSuffix(filepath.Base(path), ".docker-ack.json")
		if identity == active || identity == "" || identity == "." || identity == ".." {
			continue
		}
		prune := func() error {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var ack dockerAcknowledgement
			if err = json.Unmarshal(b, &ack); err != nil {
				return err
			}
			if ack.Worker != worker || ack.At.IsZero() || now.Before(ack.At.Add(retention)) {
				return nil
			}
			b, err = os.ReadFile(filepath.Join(dir, identity+".docker.json"))
			if errors.Is(err, os.ErrNotExist) {
				return os.Remove(path)
			}
			if err != nil {
				return err
			}
			var st dockerExecution
			if err = json.Unmarshal(b, &st); err != nil {
				return err
			}
			if st.Identity != identity || st.Endpoint == "" || !st.Released || st.Pending {
				return nil
			}
			// A queued background scan can outlive the active identity it captured.
			if b, e := os.ReadFile(filepath.Join(dir, "state.json")); e == nil {
				var current workerState
				if e = decodeWorkerState(b, &current); e != nil {
					return e
				}
				if current.Identity == identity {
					return nil
				}
			} else if !errors.Is(e, os.ErrNotExist) {
				return e
			}
			d := &dockerAttempt{ctx: ctx, state: st, binary: "docker", timeout: 2 * time.Second, env: []string{}}
			for _, v := range os.Environ() {
				k, _, _ := strings.Cut(v, "=")
				if k != "MYCE_TOKEN" && k != "DOCKER_CONTEXT" {
					d.env = append(d.env, v)
				}
			}
			call := d.call
			if caller != nil {
				call = caller(d)
			}
			out, err := call("ps", "--all", "--quiet", "--no-trunc", "--filter", dockerAttemptFilter(identity), "--filter", "label=myce.worker="+worker)
			if err != nil {
				return err
			}
			ids := strings.Fields(string(out))
			if len(ids) > 0 {
				out, err = call(append([]string{"inspect"}, ids...)...)
				if err != nil {
					return err
				}
				var cs []dockerInspection
				if err = json.Unmarshal(out, &cs); err != nil {
					return err
				}
				if len(cs) != len(ids) {
					return errors.New("incomplete retention inventory")
				}
				seen := map[string]bool{}
				for _, c := range cs {

					if !slices.Contains(ids, c.ID) || seen[c.ID] || c.Config.Labels["myce.attempt"] != identity || c.Config.Labels["myce.worker"] != worker || c.State.Running == nil || *c.State.Running || (c.State.Status != "exited" && c.State.Status != "dead" && c.State.Status != "created") {
						return errors.New("retention ownership or stopped state unconfirmed")
					}
					seen[c.ID] = true
				}
				// Never discard sidecar evidence on upgrade or after a failed capture.
				needsEvidence := st.EvidenceSaved
				for _, m := range st.Containers {
					if m.Name != "" && m.Name != "main" {
						needsEvidence = true
					}
				}
				if needsEvidence {
					data, e := os.ReadFile(filepath.Join(dir, identity+".docker-evidence.json"))
					if e != nil {
						return errors.New("private Docker evidence unavailable; containers retained")
					}
					var evidence map[string]dockerMember
					if json.Unmarshal(data, &evidence) != nil {
						return errors.New("invalid private Docker evidence")
					}
					for _, c := range cs {
						m, ok := evidence[c.ID]
						if !ok || m.ExitCode == nil || *m.ExitCode != c.State.ExitCode {
							return errors.New("Docker exit evidence incomplete; containers retained")
						}
					}
				}
				if _, err = call(append([]string{"rm"}, ids...)...); err != nil {
					return err
				}
			}
			names := []string{identity + ".cid", identity + ".result.json"}
			for name := range st.Containers {
				if name == "" || strings.ContainsAny(name, "/\\") || name == ".." {
					return errors.New("invalid retained container name")
				}
				names = append(names, identity+"-"+name+".cid")
			}
			for _, name := range names {
				if err = os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			// Keep the acknowledgement until custody removal succeeds, so failures retry.
			if err = os.Remove(filepath.Join(dir, identity+".docker.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err = syncWorkerDir(dir); err != nil {
				return err
			}
			if err = os.Remove(path); err != nil {
				return err
			}
			return syncWorkerDir(dir)
		}
		if err := prune(); err != nil {
			failures = append(failures, fmt.Errorf("attempt %s: %w", identity, err))
		}
	}
	return errors.Join(failures...)
}
