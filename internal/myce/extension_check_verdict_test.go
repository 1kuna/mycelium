package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// A re-check without a verdict must not de-qualify a device (#619): a check
// cancelled by its caller while queued keeps the earlier qualified report,
// while a check that runs and fails still revokes it.
func TestExtensionRecheckWithoutVerdictKeepsQualification(t *testing.T) {
	for _, mode := range []string{"cancelled while queued", "never admitted", "runs and fails", "lost after starting"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, false)
			e.Kinds = nil
			delete(e.Variants[0].Baselines, "kind:sample")
			e = freezeComposition(t, e)
			root := t.TempDir()
			if err := InstallExtension(root, e); err != nil {
				t.Fatal(err)
			}
			fingerprint, err := ExtensionFingerprint(context.Background(), "fake", "0")
			if err != nil {
				t.Fatal(err)
			}
			m := machine("check-worker", "fake", "check-host")
			m.Device, m.ExtensionFingerprint = "0", fingerprint
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
			qualified := newExtensionReport(e, &m, "extension:sample")
			qualified.Setup, qualified.Qualified, qualified.Reason, qualified.CheckID = true, true, "", "earlier-check"
			if err := writeExtensionReports(root, e, "fake", []ExtensionReport{qualified}); err != nil {
				t.Fatal(err)
			}
			m.ExtensionReports = []ExtensionReport{qualified}
			m.HostAvailableMemoryGB, m.HostMemoryMeasured = .01, true // the check queues behind host RAM
			check(t, q, m, nil)
			ctx, cancelCaller := context.WithCancel(context.Background())
			defer cancelCaller()
			var submitted *Job
			response := func(v any) (*http.Response, error) {
				b, marshalErr := json.Marshal(v)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
			}
			client := Client{URL: "http://fixture", HTTP: &http.Client{Transport: clientTestTransport(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.URL.Path == "/api/inspection":
					return response(Snapshot{Machines: []*Machine{&m}})
				case req.URL.Path == "/api/extensions":
					if err := q.AddExtension(e); err != nil {
						t.Fatal(err)
					}
					return response(nil)
				case req.URL.Path == "/api/jobs":
					var s Spec
					if err := json.NewDecoder(req.Body).Decode(&s); err != nil {
						t.Fatal(err)
					}
					submitted = submit(t, q, s)
					return response(submitted)
				case req.URL.Path == "/api/jobs/cancel":
					ack, err := q.CancelSubmission("extension-check", submitted.Spec.SubmissionKey)
					if err != nil {
						t.Fatal(err)
					}
					return response(ack)
				case strings.HasSuffix(req.URL.Path, "/logs"):
					return response([]LogChunk{})
				case req.URL.Path == "/api/jobs/"+submitted.ID+"/ledger":
					page, err := q.JobLedger(submitted.ID, 0, -1, 256)
					if err != nil {
						t.Fatal(err)
					}
					return response(page)
				case req.URL.Path == "/api/jobs/"+submitted.ID:
					if got := job(t, q, submitted.ID); !terminal(got.State) {
						switch mode {
						case "cancelled while queued":
							cancelCaller() // the caller's own timeout
						case "never admitted":
							clock.Advance(time.Hour)
							if err := q.Tick(); err != nil {
								t.Fatal(err)
							}
							if waiting := job(t, q, submitted.ID); waiting.State != "queued" || waiting.StartedAt != nil || extensionCheckAdmissionReason(waiting.Reason) != "waiting for host RAM" {
								t.Fatal("capacity wait expired or lost its original RAM diagnostic", waiting)
							}
							// Generated startup windows pause while the route is
							// capacity-bound. Losing that route permits expiry
							// without creating an execution verdict.
							if err := q.update(func() error {
								q.s.Machines[m.ID].Gone = true
								return nil
							}); err != nil {
								t.Fatal(err)
							}
							if err := q.Tick(); err != nil {
								t.Fatal(err)
							}
						case "runs and fails":
							m.HostAvailableMemoryGB = 32
							running := check(t, q, m, nil).Job
							if running == nil {
								t.Fatal("check not assigned")
							}
							if err := q.Started(result(running, true)); err != nil {
								t.Fatal(err)
							}
							finish(t, q, running, false)
						case "lost after starting":
							// Each requeue clears StartedAt, so the failed
							// record alone no longer shows the check ran.
							m.HostAvailableMemoryGB = 32
							for range 3 {
								running := check(t, q, m, nil).Job
								if running == nil {
									t.Fatalf("check not assigned: %+v", job(t, q, submitted.ID))
								}
								if err := q.Started(result(running, true)); err != nil {
									t.Fatal(err)
								}
								lost := result(running, false)
								lost.Interrupted = true
								if err := q.Complete(lost); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					reply, err := q.jobSnapshot(submitted.ID)
					if err != nil {
						t.Fatal(err)
					}
					return response(reply)
				}
				t.Fatalf("unexpected request: %s", req.URL.Path)
				return nil, os.ErrInvalid
			})}}
			cache := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20}
			_, err = CheckExtension(ctx, root, e, "fake", "0", m.HostID, io.Discard, cache, client, 3*time.Minute)
			if err == nil {
				t.Fatal("check without a passing verdict returned no error")
			}
			got := job(t, q, submitted.ID)
			saved := readExtensionReports(root, e, "fake")
			if len(saved) != 1 {
				t.Fatalf("reports: %+v", saved)
			}
			if mode == "lost after starting" {
				if got.State != "failed" || saved[0].Qualified || !strings.HasPrefix(saved[0].Reason, "check failed") {
					t.Fatalf("check that started and failed kept qualification: job=%+v report=%+v", got, saved[0])
				}
				return
			}
			if mode == "runs and fails" {
				if got.StartedAt == nil || saved[0].Qualified || !strings.HasPrefix(saved[0].Reason, "check failed") {
					t.Fatalf("failed run kept qualification: job=%+v report=%+v", got, saved[0])
				}
				return
			}
			if got.StartedAt != nil || !terminal(got.State) {
				t.Fatalf("check started or was left live: %+v", got)
			}
			if !saved[0].Qualified || saved[0].CheckID != "earlier-check" || saved[0].Reason != "" {
				t.Fatalf("check without a verdict replaced the earlier qualification: %+v (err %v)", saved[0], err)
			}
		})
	}
}
