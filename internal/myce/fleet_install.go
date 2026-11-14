package myce

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// FleetUpdateInstruction authorizes no local trust changes. PublisherKey must
// match the separately provisioned raw 32-byte State/fleet-publisher.pub key.
// The queue authorizes the exact operation, session and drained host before
// the launch and again immediately before the restart.
type FleetUpdateInstruction struct {
	StorageWarnings    bool              `json:"storage_warnings,omitempty"`
	StorageDiagnostics bool              `json:"storage_diagnostics,omitempty"`
	Operation          string            `json:"operation"`
	Revision           int               `json:"revision"`
	Host               string            `json:"host"`
	Machine            string            `json:"machine"`
	Session            string            `json:"session"`
	Release            EnrollmentRelease `json:"release"`
	PublisherKey       []byte            `json:"publisher_key"`
}

const fleetUpdateReceiptFile = "fleet-update.json"

type fleetInstallReceipt struct {
	VersionStderr  string                   `json:"version_stderr,omitempty"` // Local diagnostics only; never included in fleet acknowledgements.
	StorageWarning *StoragePreflightFailure `json:"storage_warning,omitempty"`
	Instruction    FleetUpdateInstruction   `json:"instruction"`
	ConfigSHA256   string                   `json:"config_sha256"`
	BeforeSHA256   string                   `json:"before_sha256"`
	BeforeVersion  string                   `json:"before_version"`
	BeforePID      int                      `json:"before_pid"`
	BeforeRuntime  FleetRuntimeProof        `json:"before_runtime"`
	Phase          string                   `json:"phase"`
	Failure        string                   `json:"failure,omitempty"`
	StorageFailure *StoragePreflightFailure `json:"storage_failure,omitempty"`
}

type fleetInstallDeps struct {
	versionCommand func(context.Context, string) ([]byte, []byte, error)
	storageFS      *onboardingStorageFS
	command        func(context.Context, string, ...string) ([]byte, error)
	client         func(ServiceConfig) (Client, error)
	health         EnrollmentWait
}

func fleetCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	// Bound an individual OS probe/manager command, not production drain time.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if name == cachedMountInventoryCommand {
		return cachedMountInventory(ctx)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = withoutMyceToken(os.Environ())
	cmd.WaitDelay = time.Second
	var out boundedProbeOutput
	out.limit = 1 << 20
	cmd.Stdout, cmd.Stderr = &out, &out // codesign inspection writes to stderr.
	err := cmd.Run()
	return out.Bytes(), err
}

func fleetClient(c ServiceConfig) (Client, error) {
	if c.JoiningFile != "" {
		i, err := ReadInvitation(c.JoiningFile)
		if err != nil {
			return Client{}, err
		}
		client, err := i.Client(c.Queue)
		return client.WithProgressWatch(), err
	}
	token, err := ReadServiceToken(c)
	if err != nil || token == "" {
		return Client{}, errors.New("fleet_credentials_unavailable")
	}
	u, err := url.Parse(c.Queue)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" || u.Scheme != "https" && u.Scheme != "http" {
		return Client{}, errors.New("fleet_queue_invalid")
	}
	// Queue progress, not a fixed timeout, bounds control calls under load (#1016).
	return Client{URL: c.Queue, Token: token, HTTP: &http.Client{}}.WithProgressWatch(), nil
}

func fleetNoRedirect(client Client) Client {
	hc := http.Client{}
	if client.HTTP != nil {
		hc = *client.HTTP
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client.HTTP = &hc
	return client
}

func fleetPrivateFile(path string, private bool) error {
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0022 != 0 || private && st.Mode().Perm()&0077 != 0 {
		return errors.New("fleet_local_file_invalid")
	}
	return nil
}

func fleetConfig(configPath string) (ServiceConfig, string, error) {
	if !filepath.IsAbs(configPath) || fleetPrivateFile(configPath, true) != nil {
		return ServiceConfig{}, "", errors.New("fleet_config_invalid")
	}
	c, err := ReadServiceConfig(configPath)
	if err != nil || c.Config != configPath || c.HostID == "" || c.Role != "worker" && c.Role != "queue" || !filepath.IsAbs(c.Binary) || !filepath.IsAbs(c.State) || !filepath.IsAbs(c.Service) {
		return c, "", errors.New("fleet_config_invalid")
	}
	if c.Detection.OS != "linux" && c.Detection.OS != "darwin" || c.Detection.OS == "linux" && (c.Role == "worker" && !ValidWorkerUnit(filepath.Base(c.Service)) || c.Role == "queue" && filepath.Base(c.Service) != "myce-queue.service") {
		return c, "", errors.New("fleet_service_invalid")
	}
	for _, path := range []string{c.Binary, c.Service} {
		if fleetPrivateFile(path, false) != nil {
			return c, "", errors.New("fleet_install_invalid")
		}
	}
	st, err := os.Lstat(c.State)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return c, "", errors.New("fleet_state_invalid")
	}
	hash, err := FileSHA256(configPath)
	return c, hash, err
}

func fleetArtifact(c ServiceConfig, i FleetUpdateInstruction) (ReleaseArtifact, error) {
	if i.Operation == "" || i.Revision <= 0 || i.Host != c.HostID || i.Machine == "" || i.Session == "" || c.Role == "worker" && i.Machine != c.Name {
		return ReleaseArtifact{}, errors.New("fleet_instruction_invalid")
	}
	path := filepath.Join(c.State, "fleet-publisher.pub")
	if fleetPrivateFile(path, false) != nil {
		return ReleaseArtifact{}, errors.New("fleet_publisher_unavailable")
	}
	st, err := os.Lstat(path)
	if err != nil || st.Size() != ed25519.PublicKeySize {
		return ReleaseArtifact{}, errors.New("fleet_publisher_unavailable")
	}
	key, err := os.ReadFile(path)
	if err != nil || len(key) != ed25519.PublicKeySize || !bytes.Equal(key, i.PublisherKey) {
		return ReleaseArtifact{}, errors.New("fleet_publisher_mismatch")
	}
	m, err := VerifyRelease(i.Release.SignedRelease, ed25519.PublicKey(key))
	if err != nil || !reflect.DeepEqual(m, i.Release.Manifest) {
		return ReleaseArtifact{}, errors.New("fleet_signature_refused")
	}
	a, err := m.Select(c.Detection.OS, c.Detection.Arch)
	if err != nil {
		return a, errors.New("fleet_target_invalid")
	}
	return a, nil
}

// fleetAuthorize asks the queue whether this exact operation and process may
// still proceed. A momentarily stale answer (a late proof) is retried for one
// proof-freshness window; any other refusal is final.
func fleetAuthorize(ctx context.Context, client Client, i FleetUpdateInstruction, wait func(context.Context, time.Duration) error) (FleetRuntimeProof, error) {
	if wait == nil {
		wait = enrollmentPause
	}
	v := url.Values{"operation": {i.Operation}, "revision": {strconv.Itoa(i.Revision)}, "host": {i.Host}, "machine": {i.Machine}, "session": {i.Session}}
	for tries := 0; ; tries++ {
		var proof FleetRuntimeProof
		err := client.Call(ctx, http.MethodGet, "/api/fleet/authorize?"+v.Encode(), nil, &proof)
		if err == nil && fleetProofReady(&proof) && proof.Session == i.Session {
			return proof, nil
		}
		err = fleetControlFailure(err, "fleet_authorization_refused")
		if !fleetStaleControl(err) || tries >= 4 || wait(ctx, fleetRuntimeFreshFor/4) != nil {
			return proof, err
		}
	}
}

func fleetAck(ctx context.Context, client Client, r fleetInstallReceipt) error {
	i := r.Instruction
	failure := ""
	if r.Phase == "failed" {
		failure = fleetFailureCode(r.Failure)
	}
	ack := FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: r.Phase, Failure: failure, StorageFailure: fleetReceiptStorageFailure(r), StorageWarning: fleetReceiptStorageWarning(r)}
	if err := client.Call(ctx, http.MethodPost, "/api/fleet/ack", ack, nil); err != nil {
		return fleetControlFailure(err, "fleet_ack_failed")
	}
	return nil
}

func writeFleetReceipt(c ServiceConfig, r fleetInstallReceipt) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return writeInstallFile(filepath.Join(c.State, fleetUpdateReceiptFile), data, 0600, installFiles{}, nil)
}

func readFleetReceipt(path string) (fleetInstallReceipt, error) {
	var r fleetInstallReceipt
	if err := fleetPrivateFile(path, true); err != nil {
		return r, err
	}
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer f.Close()
	err = json.NewDecoder(io.LimitReader(f, 1<<20)).Decode(&r)
	if err != nil || !validPin(r.ConfigSHA256) || !validPin(r.BeforeSHA256) {
		return r, errors.New("fleet_receipt_invalid")
	}
	switch r.Phase {
	case "prepared", "installed", "restarting", "failed":
		return r, nil
	}
	return r, errors.New("fleet_receipt_invalid")
}

func fleetHelperLabel(c ServiceConfig, i FleetUpdateInstruction) string {
	hash := sha256.Sum256([]byte(c.Config + "\x00" + i.Operation))
	return "myce-fleet-" + hex.EncodeToString(hash[:12])
}

// LaunchFleetUpdate checks the instruction against local trust and the
// running service, records it, and starts the SAME installed binary as a
// finite transient manager job outside the service's kill group, so the
// service restart cannot kill it. One operation launches once per host.
func LaunchFleetUpdate(ctx context.Context, c ServiceConfig, i FleetUpdateInstruction) error {
	return launchFleetUpdate(ctx, c, i, fleetInstallDeps{command: fleetCommand, versionCommand: fleetVersionCommand, client: fleetClient})
}

func launchFleetUpdate(ctx context.Context, requested ServiceConfig, i FleetUpdateInstruction, d fleetInstallDeps) error {
	c, configHash, err := fleetConfig(requested.Config)
	if err != nil || ValidateInstallIdentity(c, requested) != nil {
		return errors.New("fleet_config_invalid")
	}
	unlock, err := commandLock(c.State, "fleet-update.lock", false)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return &fleetInstructionRefetchError{} // A helper still runs; ask again next poll.
	}
	if err != nil {
		return errors.New("fleet_helper_busy")
	}
	defer unlock()
	path := filepath.Join(c.State, fleetUpdateReceiptFile)
	if r, err := readFleetReceipt(path); err == nil && r.Instruction.Operation == i.Operation {
		return nil // Already launched for this operation; its helper reports.
	}
	a, err := fleetArtifact(c, i)
	if err != nil {
		return err
	}
	client, err := d.client(c)
	if err != nil {
		return errors.New("fleet_credentials_unavailable")
	}
	client = fleetNoRedirect(client)
	defer client.HTTP.CloseIdleConnections()
	proof, err := fleetAuthorize(ctx, client, i, d.health.Wait)
	if err != nil {
		if fleetStaleControl(err) {
			return &fleetInstructionRefetchError{}
		}
		return err
	}
	if CheckReleaseUpgrade(i.Release.Manifest.Version, proof.Version, false, proof.SHA256 == a.SHA256) != nil {
		return errors.New("fleet_version_refused")
	}
	before, err := FileSHA256(c.Binary)
	if err != nil {
		return errors.New("fleet_binary_unavailable")
	}
	// The installed bytes are the running ones, or already the release (an
	// earlier helper published them and stopped before its restart).
	if before != proof.SHA256 && before != a.SHA256 {
		return errors.New("fleet_install_changed")
	}
	identity, err := EnrollmentServiceHealth(ctx, c, d.command)
	pidText, _, _ := strings.Cut(identity, "/")
	pid, _ := strconv.Atoi(pidText)
	if err != nil || pid <= 0 || proof.PID != pid {
		return errors.New("fleet_existing_service_unavailable")
	}
	r := fleetInstallReceipt{Instruction: i, ConfigSHA256: configHash, BeforeSHA256: before, BeforeVersion: proof.Version, BeforePID: pid, BeforeRuntime: proof, Phase: "prepared"}
	if err = writeFleetReceipt(c, r); err != nil {
		return errors.New("fleet_receipt_write_failed")
	}
	label := fleetHelperLabel(c, i)
	args := []string{c.Binary, "fleet-apply", "--config", c.Config, "--receipt", path}
	if c.Detection.OS == "linux" {
		args = append([]string{"--user", "--unit=" + label, "--collect", "--property=Type=exec", "--property=Restart=no", "--property=UMask=0077", "--property=StandardOutput=null", "--property=StandardError=null", "--property=UnsetEnvironment=MYCE_TOKEN", "--"}, args...)
		_, err = d.command(ctx, "systemd-run", args...)
	} else {
		args = append([]string{"submit", "-l", label, "-o", "/dev/null", "-e", "/dev/null", "--"}, args...)
		_, err = d.command(ctx, "launchctl", args...)
	}
	if err != nil {
		r.Phase, r.Failure = "failed", "fleet_helper_launch_failed"
		_ = writeFleetReceipt(c, r)
		_ = fleetAck(ctx, client, r)
		return errors.New(r.Failure)
	}
	return nil
}

// fleetVersionCommand keeps machine-readable stdout separate from diagnostics.
// Retain at most 4 KiB of stderr in the local receipt: enough for a startup
// diagnostic without letting a noisy candidate grow durable state unboundedly.
func fleetVersionCommand(ctx context.Context, path string) ([]byte, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "version", "--json")
	cmd.Env = withoutMyceToken(os.Environ())
	cmd.WaitDelay = time.Second
	var stdout, stderr boundedProbeOutput
	stdout.limit, stderr.limit = 1<<20, 4<<10
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func fleetVersion(ctx context.Context, path string, command func(context.Context, string) ([]byte, []byte, error)) (VersionInfo, string, error) {
	var info VersionInfo
	data, diagnostic, err := command(ctx, path)
	if err != nil || json.Unmarshal(data, &info) != nil {
		return info, string(diagnostic), errors.New("fleet_version_invalid")
	}
	return info, string(diagnostic), nil
}

// ApplyFleetUpdate is the hidden finite helper: fetch and verify the release,
// swap it in place of the installed binary, restart the existing service and
// wait for the manager to report the new process. It never restores old
// bytes, retries the restart or resumes maintenance; the queue checks health.
func ApplyFleetUpdate(ctx context.Context, configPath, receiptPath string) error {
	return applyFleetUpdate(ctx, configPath, receiptPath, fleetInstallDeps{command: fleetCommand, versionCommand: fleetVersionCommand, client: fleetClient})
}

func applyFleetUpdate(ctx context.Context, configPath, receiptPath string, d fleetInstallDeps) error {
	c, configHash, err := fleetConfig(configPath)
	if err != nil || receiptPath != filepath.Join(c.State, fleetUpdateReceiptFile) {
		return errors.New("fleet_config_invalid")
	}
	unlock, err := commandLock(c.State, "fleet-update.lock", true)
	if err != nil {
		return errors.New("fleet_helper_busy")
	}
	defer unlock()
	r, err := readFleetReceipt(receiptPath)
	if err != nil || r.ConfigSHA256 != configHash {
		return errors.New("fleet_receipt_invalid")
	}
	switch r.Phase {
	case "restarting":
		return nil // Already restarted; the queue checks the new process.
	case "failed":
		return errors.New(fleetFailureCode(r.Failure))
	}
	if c.Detection.OS == "darwin" {
		// launchctl submit creates no persistent definition or KeepAlive daemon.
		defer func() {
			_, _ = d.command(context.WithoutCancel(ctx), "launchctl", "remove", fleetHelperLabel(c, r.Instruction))
		}()
	}
	client, err := d.client(c)
	if err != nil {
		// The worker replays this receipt once credentials are repaired.
		r.Phase, r.Failure = "failed", "fleet_credentials_unavailable"
		_ = writeFleetReceipt(c, r)
		return errors.New(r.Failure)
	}
	client = fleetNoRedirect(client)
	defer client.HTTP.CloseIdleConnections()
	fail := func(code string) error {
		r.Phase, r.Failure = "failed", fleetFailureCode(code)
		saveErr := writeFleetReceipt(c, r)
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = fleetAck(ackCtx, client, r)
		if saveErr != nil {
			return errors.New("fleet_receipt_write_failed")
		}
		return errors.New(r.Failure)
	}
	a, err := fleetArtifact(c, r.Instruction)
	if err != nil {
		return fail(err.Error())
	}
	// Activation needs the installer's own preflight and storage checks.
	if c.ValidateInstall() != nil || c.ValidateInstallPolicies() != nil {
		return fail("fleet_preflight_failed")
	}
	r.StorageWarning, err = preflightFleetStorageResult(ctx, c, d.command, d.storageFS)
	if err != nil {
		r.StorageFailure = storageFailureDetail(err)
		return fail("fleet_storage_preflight_failed")
	}
	manager := installManager{c.Detection.OS, d.command}
	unit := installUnit{path: c.Service, label: "local.mycelium." + c.Role}
	state, err := manager.inspect(ctx, unit)
	if err != nil || !state.active || c.Detection.OS == "linux" && state.enabled == "not-found" || c.Detection.OS == "darwin" && !state.loaded {
		return fail("fleet_existing_service_unavailable")
	}
	before, err := FileSHA256(c.Binary)
	if err != nil {
		return fail("fleet_binary_unavailable")
	}
	// This operation, process and drain still hold. When the bytes are already
	// in place (an earlier helper stopped before its restart), this is the
	// last check before the restart.
	if _, err := fleetAuthorize(ctx, client, r.Instruction, d.health.Wait); err != nil {
		return fail(err.Error())
	}
	if before != a.SHA256 {
		if before != r.BeforeSHA256 {
			return fail("fleet_install_changed")
		}
		if err := swapFleetBinary(ctx, c, client, &r, a, d); err != nil {
			return fail(err.Error())
		}
		r.Phase = "installed"
		if writeFleetReceipt(c, r) != nil {
			return fail("fleet_receipt_write_failed")
		}
	}
	// Nothing else changed the installed settings or bytes.
	current, currentConfig, configErr := fleetConfig(configPath)
	currentHash, hashErr := FileSHA256(c.Binary)
	if configErr != nil || currentConfig != configHash || ValidateInstallIdentity(c, current) != nil || hashErr != nil || currentHash != a.SHA256 {
		return fail("fleet_install_changed")
	}
	identity, err := EnrollmentServiceHealth(ctx, c, d.command)
	if pid, _, _ := strings.Cut(identity, "/"); err != nil || pid != strconv.Itoa(r.BeforePID) {
		return fail("fleet_recovery_runtime_changed")
	}
	r.Phase = "restarting"
	if writeFleetReceipt(c, r) != nil {
		return fail("fleet_receipt_write_failed")
	}
	_ = fleetAck(ctx, client, r) // Informational: the queue verifies the new process itself.
	if manager.activity(ctx, unit, true, true) != nil {
		return fail("fleet_restart_failed")
	}
	if waitFleetManager(ctx, c, r.BeforePID, d) != nil {
		return fail("fleet_health_failed")
	}
	return nil
}

// swapFleetBinary downloads the release through the queue, checks its size,
// hash, signature identity and reported version, asks the queue once more,
// then publishes it over the installed binary with the installer's atomic
// same-directory write. The restart follows immediately.
func swapFleetBinary(ctx context.Context, c ServiceConfig, client Client, r *fleetInstallReceipt, a ReleaseArtifact, d fleetInstallDeps) error {
	staged, err := fetchFleetArtifact(ctx, c, client, r.Instruction, a)
	if err != nil {
		return err
	}
	defer os.Remove(staged)
	if c.Detection.OS == "darwin" && (!nativeMachO(c.Binary) || !nativeMachO(staged) || validateDarwinUpgradeIdentity(ctx, c.Binary, staged, d.command) != nil) {
		return errors.New("fleet_darwin_identity_refused")
	}
	info, diagnostic, err := fleetVersion(ctx, staged, d.versionCommand)
	r.VersionStderr = diagnostic
	m := r.Instruction.Release.Manifest
	if err != nil || info.Version != m.Version || info.OS != a.OS || info.Arch != a.Arch || info.StartGate != m.StartGate || info.AttemptProtocol != m.AttemptProtocol || a.NativeSensing && !info.NativeSensing {
		return errors.New("fleet_candidate_invalid")
	}
	if CheckReleaseUpgrade(m.Version, r.BeforeVersion, false) != nil {
		return errors.New("fleet_version_refused")
	}
	// Local publisher trust may have changed during the download.
	if _, err := fleetArtifact(c, r.Instruction); err != nil {
		return err
	}
	data, err := os.ReadFile(staged)
	if err != nil || fleetBytesHash(data) != a.SHA256 || int64(len(data)) != a.Size {
		return errors.New("fleet_artifact_refused")
	}
	if _, err := fleetAuthorize(ctx, client, r.Instruction, d.health.Wait); err != nil {
		return err
	}
	if current, err := FileSHA256(c.Binary); err != nil || current != r.BeforeSHA256 {
		return errors.New("fleet_install_changed")
	}
	st, err := os.Lstat(c.Binary)
	if err != nil || writeInstallFile(c.Binary, data, st.Mode().Perm(), installFiles{}, nil) != nil {
		return errors.New("fleet_publish_failed")
	}
	return nil
}

func fleetBytesHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func fetchFleetArtifact(ctx context.Context, c ServiceConfig, client Client, i FleetUpdateInstruction, a ReleaseArtifact) (string, error) {
	v := url.Values{"operation": {i.Operation}, "target": {a.File}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(client.URL, "/")+"/api/fleet/artifact?"+v.Encode(), nil)
	if err != nil {
		return "", errors.New("fleet_artifact_download_failed")
	}
	req.Header.Set("Authorization", "Bearer "+client.Token)
	// Large binaries need a transfer budget separate from small control calls.
	// This bounds a stuck transfer; no retries or production-work deadline.
	hc := *client.HTTP
	hc.Timeout = 10 * time.Minute
	res, err := hc.Do(req)
	if err != nil {
		return "", errors.New("fleet_artifact_download_failed")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", errors.New("fleet_artifact_refused")
	}
	// The helper lock makes this the only writer; a dead helper's partial is
	// rebuildable and replaced.
	f, err := os.OpenFile(fleetStagingPath(c, i), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return "", errors.New("fleet_staging_write_failed")
	}
	path := f.Name()
	keep := false
	defer func() {
		_ = f.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(res.Body, a.Size))
	if err != nil {
		return "", errors.New("fleet_artifact_transfer_failed")
	}
	if n != a.Size {
		return "", errors.New("fleet_artifact_refused")
	}
	var extra [1]byte
	if n, err := res.Body.Read(extra[:]); n != 0 || err != io.EOF || hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		return "", errors.New("fleet_artifact_refused")
	}
	if err := errors.Join(f.Chmod(0700), f.Sync(), f.Close()); err != nil {
		return "", errors.New("fleet_staging_write_failed")
	}
	keep = true
	return path, nil
}

func fleetStagingPath(c ServiceConfig, i FleetUpdateInstruction) string {
	return filepath.Join(filepath.Dir(c.Binary), "."+fleetHelperLabel(c, i)+".partial")
}

// waitFleetManager waits up to a minute, enrollment's readiness budget, for
// the manager to report a new healthy process. It never retries the restart;
// the queue then checks that process's runtime proof and stability.
func waitFleetManager(ctx context.Context, c ServiceConfig, beforePID int, d fleetInstallDeps) error {
	opt := d.health
	if opt.Clock == nil {
		opt.Clock = RealClock{}
	}
	if opt.Wait == nil {
		opt.Wait = enrollmentPause
	}
	if opt.Timeout <= 0 {
		opt.Timeout = time.Minute
	}
	deadline := opt.Clock.Now().Add(opt.Timeout)
	for {
		probe, cancel := context.WithTimeout(ctx, 5*time.Second)
		identity, err := EnrollmentServiceHealth(probe, c, d.command)
		cancel()
		pid, _, _ := strings.Cut(identity, "/")
		if err == nil && pid != strconv.Itoa(beforePID) {
			return nil
		}
		if ctx.Err() != nil || !opt.Clock.Now().Before(deadline) || opt.Wait(ctx, time.Second) != nil {
			return errors.New("fleet_health_failed")
		}
	}
}

// ReadFleetUpdateFailure lets a restarted runtime report a helper failure whose
// acknowledgement was lost during queue outage. It never consumes evidence or
// substitutes a new session/revision. Arbitrary receipt text is not exposed.
func ReadFleetUpdateFailure(c ServiceConfig) (*FleetUpgradeAck, error) {
	path := filepath.Join(c.State, fleetUpdateReceiptFile)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	r, err := readFleetReceipt(path)
	if err != nil {
		return nil, errors.New("fleet_receipt_invalid")
	}
	if r.Phase != "failed" {
		return nil, nil
	}
	i := r.Instruction
	if i.Operation == "" || i.Revision <= 0 || i.Host != c.HostID || i.Session == "" || c.Role == "worker" && i.Machine != c.Name || c.Role == "queue" && i.Machine != "@queue" {
		return nil, errors.New("fleet_receipt_invalid")
	}
	code := fleetFailureCode(r.Failure)
	return &FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: code, StorageFailure: fleetReceiptStorageFailure(r), StorageWarning: fleetReceiptStorageWarning(r)}, nil
}
