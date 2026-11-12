package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These are the public diagnostics emitted by launch/apply, including failures
// before a receipt exists. Exercise both live acknowledgements and outage replay.
func TestFleetFailureCodesSurviveReceiptAndQueue(t *testing.T) {
	codes := []string{
		"fleet_ack_failed", "fleet_authorization_refused", "fleet_instruction_stale", "fleet_ack_stale",
		"fleet_maintenance_changed",
		"fleet_artifact_download_failed", "fleet_artifact_transfer_failed", "fleet_artifact_refused",
		"fleet_staging_write_failed", "fleet_binary_unavailable", "fleet_candidate_invalid",
		"fleet_config_invalid", "fleet_service_invalid", "fleet_install_invalid", "fleet_state_invalid",
		"fleet_local_file_invalid", "fleet_credentials_unavailable", "fleet_queue_invalid",
		"fleet_darwin_identity_refused", "fleet_existing_service_unavailable", "fleet_health_failed",
		"fleet_helper_busy", "fleet_helper_launch_failed", "fleet_helper_launch_unclassified",
		"fleet_install_changed", "fleet_instruction_invalid", "fleet_preflight_failed",
		"fleet_storage_preflight_failed", "fleet_publish_failed", "fleet_publisher_mismatch",
		"fleet_publisher_unavailable", "fleet_signature_refused", "fleet_target_invalid",
		"fleet_receipt_write_failed", "fleet_recovery_runtime_changed", "fleet_restart_failed",
		"fleet_version_refused", "fleet_failure_code_missing", "fleet_failure_code_unrecognized",
	}
	f := newFleetInstallFixture(t, "darwin")
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	if err != nil {
		t.Fatal(err)
	}
	q, _, _ := fleetFixture(t, "queue", "worker")
	for _, code := range codes {
		t.Run(code, func(t *testing.T) {
			r.Phase, r.Failure = "failed", code
			if err := writeFleetReceipt(f.c, r); err != nil {
				t.Fatal(err)
			}
			replayed, err := ReadFleetUpdateFailure(f.c)
			if err != nil || replayed == nil || replayed.Failure != code || replayed.Operation != f.i.Operation || replayed.Revision != f.i.Revision || replayed.Session != f.i.Session {
				t.Fatalf("receipt lost code or attempt identity: %+v %v", replayed, err)
			}
			for _, reported := range []string{code, replayed.Failure} {
				if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
					t.Fatal(err)
				}
				fleetTick(t, q)
				i := fleetInstructionFor(t, q)
				if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: reported}); err != nil {
					t.Fatal(err)
				}
				h := fleetHostStatus(t, q, i.Host)
				want := code
				if code == "fleet_storage_preflight_failed" {
					want = "storage_preflight_failed"
				} else if code == "fleet_health_failed" {
					want = "service_health_failed"
				}
				if h.Failure != want || h.FailureCode != code || q.fleetUpgradeStatus().State != "stopped" {
					t.Fatalf("queue lost diagnostic: %+v", h)
				}
			}
		})
	}
}

// A momentarily stale authorization (a late proof) is asked again within the
// launch; a persistent one leaves no receipt and the next poll refetches.
func TestFleetFailureStaleAuthorizationRetriesThenRefetches(t *testing.T) {
	f := newFleetInstallFixture(t, "darwin")
	f.denyAt, f.denyCode = 1, "fleet_instruction_stale"
	if err := f.launch(); err != nil || f.launches != 1 || f.authorizations != 2 {
		t.Fatal("transient stale answer not retried", err, f.launches, f.authorizations)
	}

	f = newFleetInstallFixture(t, "darwin")
	f.denyAt, f.denyCode = -1, "fleet_instruction_stale"
	client, err := f.deps.client(f.c)
	if err != nil {
		t.Fatal(err)
	}
	collect := func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
		return &FleetRuntimeProof{Session: f.i.Session, ServiceHealthy: true}, nil
	}
	launch := func(ctx context.Context, c ServiceConfig, i FleetUpdateInstruction) error {
		return launchFleetUpdate(ctx, c, i, f.deps)
	}
	result := pollFleetRuntime(context.Background(), f.c, client, collect, launch, f.i.Host, f.i.Machine, f.i.Session, true, "", nil)
	if result.attempted != "" || result.failed != nil || f.launches != 0 || len(f.acks) != 0 {
		t.Fatal("stale authorization poisoned the operation or launched a helper", result, f.acks)
	}
	if _, err := os.Lstat(filepath.Join(f.c.State, fleetUpdateReceiptFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale authorization wrote a receipt", err)
	}
	f.denyAt = 0
	result = pollFleetRuntime(context.Background(), f.c, client, collect, launch, f.i.Host, f.i.Machine, f.i.Session, true, result.attempted, result.failed)
	if result.attempted != fleetInstructionKey(f.i) || f.launches != 1 || len(f.failureCodes) != 0 {
		t.Fatalf("fresh instruction did not launch: %+v %v", result, f.failureCodes)
	}
	_ = pollFleetRuntime(context.Background(), f.c, client, collect, launch, f.i.Host, f.i.Machine, f.i.Session, true, result.attempted, result.failed)
	if f.launches != 1 {
		t.Fatal("accepted instruction relaunched")
	}
}

func TestFleetFailureProofExpiryRefusesAuthorization(t *testing.T) {
	q, clock, _ := fleetFixture(t, "queue")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	clock.Advance(fleetRuntimeFreshFor)
	err := q.authorizeFleetUpdate(i.Operation, i.Host, i.Machine, i.Session)
	var rejection *HTTPError
	if !errors.As(err, &rejection) || rejection.Code != "fleet_instruction_stale" || q.s.FleetUpgrade.State != "running" {
		t.Fatal("expired proof should refuse without changing the operation", err)
	}
	q.fleetQueue.Proof.ObservedAt = clock.Now()
	if err := q.authorizeFleetUpdate(i.Operation, i.Host, i.Machine, i.Session); err != nil {
		t.Fatal("refreshed proof did not restore authorization", err)
	}
}

func TestFleetFailureRuntimePreservesCodesAndStaleBoundary(t *testing.T) {
	for _, tc := range []struct{ reported, want string }{
		{"fleet_config_invalid", "fleet_config_invalid"},
		{"fleet_publisher_mismatch", "fleet_publisher_mismatch"},
		{"fleet_ack_stale", "fleet_ack_stale"},
		// A stale refusal AFTER intent is an ordinary failure, not the private
		// pre-intent refetch signal. It must not automatically relaunch.
		{"fleet_instruction_stale", "fleet_instruction_stale"},
		{"private launch diagnostic", "fleet_helper_launch_unclassified"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			f := newFleetInstallFixture(t, "darwin")
			client, err := f.deps.client(f.c)
			if err != nil {
				t.Fatal(err)
			}
			collect := func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
				return &FleetRuntimeProof{Session: f.i.Session, ServiceHealthy: true}, nil
			}
			launches := 0
			launch := func(context.Context, ServiceConfig, FleetUpdateInstruction) error {
				launches++
				return errors.New(tc.reported)
			}
			result := pollFleetRuntime(context.Background(), f.c, client, collect, launch, f.i.Host, f.i.Machine, f.i.Session, true, "", nil)
			if result.attempted != fleetInstructionKey(f.i) || len(f.failureCodes) != 1 || f.failureCodes[0] != tc.want {
				t.Fatal("launch diagnostic lost", result, f.failureCodes)
			}
			_ = pollFleetRuntime(context.Background(), f.c, client, collect, launch, f.i.Host, f.i.Machine, f.i.Session, true, result.attempted, result.failed)
			if launches != 1 {
				t.Fatal("terminal launch failure automatically retried")
			}
		})
	}
}

func TestFleetFailureCredentialsOutageRetainsReceipt(t *testing.T) {
	f := newFleetInstallFixture(t, "darwin")
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	credentials, err := os.ReadFile(f.c.Credentials)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f.c.Credentials); err != nil {
		t.Fatal(err)
	}
	if err := f.apply(); err == nil || err.Error() != "fleet_credentials_unavailable" {
		t.Fatal("credentials outage lost code", err)
	}
	ack, err := ReadFleetUpdateFailure(f.c)
	if err != nil || ack == nil || ack.Failure != "fleet_credentials_unavailable" || f.fetches != 0 || f.restarts != 0 || len(f.failureCodes) != 0 {
		t.Fatal("offline failure not durable", ack, err)
	}
	if err := os.WriteFile(f.c.Credentials, credentials, 0600); err != nil {
		t.Fatal(err)
	}
	client, err := f.deps.client(f.c)
	if err != nil {
		t.Fatal(err)
	}
	collect := func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) { return nil, nil }
	launch := func(context.Context, ServiceConfig, FleetUpdateInstruction) error {
		t.Fatal("receipt recovery relaunched helper")
		return nil
	}
	_ = pollFleetRuntime(context.Background(), f.c, client, collect, launch, f.i.Host, f.i.Machine, f.i.Session, true, "", nil)
	if len(f.failureCodes) != 1 || f.failureCodes[0] != "fleet_credentials_unavailable" {
		t.Fatal("worker did not replay repaired-credentials failure", f.failureCodes)
	}
	f.assertBinary(f.old)
}

func TestFleetFailureFinalPublisherRecheckPreservesCode(t *testing.T) {
	for _, fault := range []string{"missing", "mismatch"} {
		t.Run(fault, func(t *testing.T) {
			f := newFleetInstallFixture(t, "darwin")
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			command := f.deps.command
			f.deps.command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				data, err := command(ctx, name, args...)
				if name == fleetStagingPath(f.c, f.i) {
					path := filepath.Join(f.c.State, "fleet-publisher.pub")
					if fault == "missing" {
						if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
					} else if err := os.WriteFile(path, []byte(strings.Repeat("x", len(f.i.PublisherKey))), 0600); err != nil {
						t.Fatal(err)
					}
				}
				return data, err
			}
			want := "fleet_publisher_unavailable"
			if fault == "mismatch" {
				want = "fleet_publisher_mismatch"
			}
			if err := f.apply(); err == nil || err.Error() != want {
				t.Fatal("final trust recheck collapsed failure", err)
			}
			ack, err := ReadFleetUpdateFailure(f.c)
			if err != nil || ack == nil || ack.Failure != want || len(f.failureCodes) != 1 || f.failureCodes[0] != want || f.restarts != 0 {
				t.Fatal("final trust failure lost", ack, err, f.failureCodes)
			}
			f.assertBinary(f.old)
		})
	}
}

// #846 reproduces transient stale proof after intent. Maintenance changes remain
// terminal at each boundary; only stale control responses may refetch.
func TestFleetFailureAfterIntentRemainsDurable(t *testing.T) {
	for _, denyAt := range []int{2, 3} {
		t.Run(map[int]string{2: "before-fetch", 3: "before-publication"}[denyAt], func(t *testing.T) {
			f := newFleetInstallFixture(t, "darwin")
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			f.denyAt, f.denyCode = denyAt, "fleet_maintenance_changed"
			if err := f.apply(); err == nil || err.Error() != "fleet_maintenance_changed" {
				t.Fatal("maintenance refusal lost code", err)
			}
			ack, err := ReadFleetUpdateFailure(f.c)
			if err != nil || ack == nil || ack.Failure != "fleet_maintenance_changed" || len(f.failureCodes) != 1 || f.failureCodes[0] != ack.Failure || f.restarts != 0 {
				t.Fatalf("helper failure not preserved: %+v %v", ack, err)
			}
			if err := f.apply(); err == nil || f.restarts != 0 || f.launches != 1 {
				t.Fatal("failed helper automatically retried activation", err)
			}
		})
	}
}

func TestFleetFailureDarwinInstalledIdentity(t *testing.T) {
	for _, fault := range []string{"old-adhoc", "different-requirement", "candidate-probe", "helper-launch"} {
		t.Run(fault, func(t *testing.T) {
			f := newFleetInstallFixture(t, "darwin")
			command := f.deps.command
			f.deps.command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if fault == "old-adhoc" && name == "codesign" && args[0] == "-dv" && args[len(args)-1] == f.c.Binary {
					return []byte("Identifier=io.github.1kuna.mycelium\nSignature=adhoc\n"), nil
				}
				if fault == "candidate-probe" && name == fleetStagingPath(f.c, f.i) || fault == "helper-launch" && name == "launchctl" && args[0] == "submit" {
					return nil, errors.New("private OS diagnostic")
				}
				return command(ctx, name, args...)
			}
			f.changeDR = fault == "different-requirement"
			err := f.launch()
			want := "fleet_helper_launch_failed"
			if fault != "helper-launch" {
				if err != nil {
					t.Fatal(err)
				}
				err = f.apply()
				want = "fleet_darwin_identity_refused"
				if fault == "candidate-probe" {
					want = "fleet_candidate_invalid"
				}
			}
			ack, readErr := ReadFleetUpdateFailure(f.c)
			if err == nil || err.Error() != want || readErr != nil || ack == nil || ack.Failure != want || len(f.failureCodes) != 1 || f.failureCodes[0] != want || f.restarts != 0 {
				t.Fatalf("lost Darwin failure: %v %+v %v %v", err, ack, readErr, f.failureCodes)
			}
			f.assertBinary(f.old)
		})
	}
}

func TestFleetFailureArtifactAndStaging(t *testing.T) {
	for _, tc := range []struct{ fault, want string }{
		{"download", "fleet_artifact_download_failed"},
		{"http-refusal", "fleet_artifact_refused"},
		{"transfer", "fleet_artifact_transfer_failed"},
		{"truncated", "fleet_artifact_refused"},
		{"hash", "fleet_artifact_refused"},
		{"oversize", "fleet_artifact_refused"},
		{"staging-parent-missing", "fleet_staging_write_failed"},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			c := ServiceConfig{Binary: filepath.Join(t.TempDir(), "myce")}
			i := FleetUpdateInstruction{Operation: "fixture-operation"}
			data := "artifact bytes"
			a := ReleaseArtifact{Size: int64(len(data)), SHA256: fleetBytesHash([]byte(data))}
			if tc.fault == "staging-parent-missing" {
				c.Binary = filepath.Join(filepath.Dir(c.Binary), "missing", "myce")
			}
			client := Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(*http.Request) (*http.Response, error) {
				if tc.fault == "download" {
					return nil, errors.New("private network diagnostic")
				}
				status := http.StatusOK
				body := io.NopCloser(strings.NewReader(data))
				switch tc.fault {
				case "http-refusal":
					status = http.StatusConflict
				case "transfer":
					body = io.NopCloser(fleetFailedArtifactReader{})
				case "truncated":
					body = io.NopCloser(strings.NewReader(data[:3]))
				case "hash":
					body = io.NopCloser(strings.NewReader(strings.Repeat("x", len(data))))
				case "oversize":
					body = io.NopCloser(strings.NewReader(data + "x"))
				}
				return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
			})}}
			path, err := fetchFleetArtifact(context.Background(), c, client, i, a)
			if err == nil || err.Error() != tc.want || path != "" {
				t.Fatal("artifact failure lost code", err, path)
			}
			if _, err := os.Lstat(fleetStagingPath(c, i)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed transfer left partial staging", err)
			}
		})
	}
}

type fleetFailedArtifactReader struct{}

func (fleetFailedArtifactReader) Read([]byte) (int, error) {
	return 0, errors.New("private stream diagnostic")
}

func TestFleetFailureSanitizesMissingLegacyAndUnknownCodes(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", "fleet_failure_code_missing"},
		{"fleet_private_secret_with_valid_code_shape", "fleet_failure_code_unrecognized"},
		{"fleet_restart_failed\nprivate diagnostic", "fleet_failure_code_unrecognized"},
	} {
		t.Run(tc.want+tc.input, func(t *testing.T) {
			q, _, _ := fleetFixture(t, "queue", "worker")
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			i := fleetInstructionFor(t, q)
			if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: tc.input}); err != nil {
				t.Fatal(err)
			}
			h := fleetHostStatus(t, q, i.Host)
			if h.Failure != tc.want || h.FailureCode != tc.want {
				t.Fatalf("unsafe or unactionable failure: %+v", h)
			}
			data, _ := json.Marshal(q.fleetUpgradeStatus())
			if strings.Contains(string(data), "private") {
				t.Fatal("untrusted diagnostic in public status")
			}
		})
	}
}

func fleetHostStatus(t *testing.T, q *Queue, host string) FleetHostStatus {
	t.Helper()
	for _, h := range q.fleetUpgradeStatus().Hosts {
		if h.Host == host {
			return h
		}
	}
	t.Fatal("no host", host)
	return FleetHostStatus{}
}
