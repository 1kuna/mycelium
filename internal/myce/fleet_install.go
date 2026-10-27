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
// The queue additionally authorizes the exact operation/session and owned drain
// before download and again immediately before publication.
type FleetUpdateInstruction struct {
	Operation    string            `json:"operation"`
	Revision     int               `json:"revision"`
	Host         string            `json:"host"`
	Machine      string            `json:"machine"`
	Session      string            `json:"session"`
	Release      EnrollmentRelease `json:"release"`
	PublisherKey []byte            `json:"publisher_key"`
}

const fleetUpdateReceiptFile = "fleet-update.json"

type fleetInstallReceipt struct {
	Instruction   FleetUpdateInstruction `json:"instruction"`
	ConfigSHA256  string                 `json:"config_sha256"`
	BeforeSHA256  string                 `json:"before_sha256"`
	BeforeVersion string                 `json:"before_version"`
	BeforePID     int                    `json:"before_pid"`
	BeforeRuntime FleetRuntimeProof      `json:"before_runtime"`
	Phase         string                 `json:"phase"`
	Failure       string                 `json:"failure,omitempty"`
}

type fleetInstallDeps struct {
	command func(context.Context, string, ...string) ([]byte, error)
	client  func(ServiceConfig) (Client, error)
	health  EnrollmentWait
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
		return i.Client(c.Queue)
	}
	token, err := ReadServiceToken(c)
	if err != nil || token == "" {
		return Client{}, errors.New("fleet_credentials_unavailable")
	}
	u, err := url.Parse(c.Queue)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" || u.Scheme != "https" && u.Scheme != "http" {
		return Client{}, errors.New("fleet_queue_invalid")
	}
	return Client{URL: c.Queue, Token: token, HTTP: &http.Client{Timeout: 10 * time.Second}}, nil
}

func fleetNoRedirect(client Client) Client {
	hc := http.Client{Timeout: 10 * time.Second}
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

func fleetAuthorize(ctx context.Context, client Client, i FleetUpdateInstruction) error {
	_, err := fleetAuthorizedRuntime(ctx, client, i)
	return err
}

func fleetAuthorizedRuntime(ctx context.Context, client Client, i FleetUpdateInstruction) (FleetRuntimeProof, error) {
	var proof FleetRuntimeProof
	v := url.Values{"operation": {i.Operation}, "revision": {strconv.Itoa(i.Revision)}, "host": {i.Host}, "machine": {i.Machine}, "session": {i.Session}}
	if err := client.Call(ctx, http.MethodGet, "/api/fleet/authorize?"+v.Encode(), nil, &proof); err != nil || !fleetProofReady(&proof) || proof.Session != i.Session {
		return proof, errors.New("fleet_authorization_refused")
	}
	return proof, nil
}

func fleetAck(ctx context.Context, client Client, r fleetInstallReceipt) error {
	i := r.Instruction
	ack := struct {
		Operation string `json:"operation"`
		Revision  int    `json:"revision"`
		Host      string `json:"host"`
		Machine   string `json:"machine"`
		Session   string `json:"session"`
		Phase     string `json:"phase"`
		Failure   string `json:"failure,omitempty"`
	}{i.Operation, i.Revision, i.Host, i.Machine, i.Session, r.Phase, r.Failure}
	if client.Call(ctx, http.MethodPost, "/api/fleet/ack", ack, nil) != nil {
		return errors.New("fleet_ack_failed")
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

// LaunchFleetUpdate durably records intent before starting the SAME installed
// binary in a finite transient manager job outside the service's kill group.
// Repeated instructions never relaunch a recorded revision automatically.
// An explicit higher revision can recover only an absent helper and exact old
// manager/runtime identity; release, drain and session authority stay frozen.
func LaunchFleetUpdate(ctx context.Context, c ServiceConfig, i FleetUpdateInstruction) error {
	return launchFleetUpdate(ctx, c, i, fleetInstallDeps{command: fleetCommand, client: fleetClient})
}

func launchFleetUpdate(ctx context.Context, requested ServiceConfig, i FleetUpdateInstruction, d fleetInstallDeps) error {
	c, configHash, err := fleetConfig(requested.Config)
	if err != nil || ValidateInstallIdentity(c, requested) != nil {
		return errors.New("fleet_config_invalid")
	}
	unlock, err := commandLock(c.State, "fleet-update.lock", false)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return nil // The existing local apply/launch still owns this operation.
	}
	if err != nil {
		return errors.New("fleet_helper_busy")
	}
	defer unlock()
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
	proof, err := fleetAuthorizedRuntime(ctx, client, i)
	if err != nil {
		return err
	}
	if CheckReleaseUpgrade(i.Release.Manifest.Version, proof.Version, false, proof.SHA256 == a.SHA256) != nil {
		return errors.New("fleet_version_refused")
	}
	path := filepath.Join(c.State, fleetUpdateReceiptFile)
	var r fleetInstallReceipt
	resuming := false
	if _, err := os.Lstat(path); err == nil {
		r, err = readFleetReceipt(path)
		if err != nil {
			return errors.New("fleet_receipt_invalid")
		}
		if r.Instruction.Operation == i.Operation {
			frozen := r.Instruction
			frozen.Revision = i.Revision
			if !reflect.DeepEqual(frozen, i) || r.ConfigSHA256 != configHash || i.Revision < r.Instruction.Revision {
				return errors.New("fleet_receipt_conflict")
			}
			if i.Revision == r.Instruction.Revision {
				if r.Phase == "failed" {
					return errors.New("fleet_previous_failure")
				}
				return nil
			}
			// Only an explicit operator revision can recover a vanished helper.
			// A running/unknown transient is never duplicated or killed.
			absent, err := fleetHelperAbsent(ctx, c, r.Instruction, d.command)
			if err != nil {
				return errors.New("fleet_helper_absence_unproven")
			}
			if !absent {
				return nil // Proven active older revision continues unchanged.
			}
			if !sameFleetRuntime(proof, r.BeforeRuntime) {
				return errors.New("fleet_recovery_runtime_changed")
			}
			resuming = true
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("fleet_receipt_invalid")
	}
	before, err := FileSHA256(c.Binary)
	if err != nil {
		return errors.New("fleet_binary_unavailable")
	}
	identity, err := EnrollmentServiceHealth(ctx, c, d.command)
	pidText, _, _ := strings.Cut(identity, "/")
	pid, _ := strconv.Atoi(pidText)
	if err != nil || pid <= 0 || proof.PID != pid {
		return errors.New("fleet_existing_service_unavailable")
	}
	if resuming {
		if pid != r.BeforePID || before != r.BeforeSHA256 && before != a.SHA256 {
			return errors.New("fleet_recovery_runtime_changed")
		}
		// This exact operation's partial is rebuildable from its frozen release;
		// the independent absence probe above proved no helper still uses it.
		partial := fleetStagingPath(c, r.Instruction)
		if st, err := os.Lstat(partial); err == nil {
			if !st.Mode().IsRegular() || os.Remove(partial) != nil {
				return errors.New("fleet_staging_cleanup_failed")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("fleet_staging_cleanup_failed")
		}
		r.Instruction, r.Failure = i, ""
		r.Phase = "prepared"
		if before == a.SHA256 {
			// Queue proof and manager PID both still name the original process.
			// The higher revision explicitly authorizes recovery of restart intent.
			r.Phase = "installed"
		}
	} else {
		info, err := fleetVersion(ctx, c.Binary, d.command)
		if err != nil || CheckReleaseUpgrade(i.Release.Manifest.Version, info.Version, false, before == a.SHA256) != nil {
			return errors.New("fleet_version_refused")
		}
		r = fleetInstallReceipt{Instruction: i, ConfigSHA256: configHash, BeforeSHA256: before, BeforeVersion: info.Version, BeforePID: pid, BeforeRuntime: proof, Phase: "prepared"}
	}
	if err = writeFleetReceipt(c, r); err != nil {
		return errors.New("fleet_receipt_write_failed")
	}
	// The local lock and (for recovery) transient-absence proof precede this
	// durable claim. Merely receiving a newer instruction never revokes the
	// older active helper's ability to report failure.
	claim := r
	claim.Phase = "prepared"
	if err = fleetAck(ctx, client, claim); err != nil {
		return err
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

func sameFleetRuntime(a, b FleetRuntimeProof) bool {
	return a.PID == b.PID && a.Session == b.Session && a.Version == b.Version && a.SHA256 == b.SHA256 && a.StartGate == b.StartGate && a.AttemptProtocol == b.AttemptProtocol
}

func fleetHelperAbsent(ctx context.Context, c ServiceConfig, i FleetUpdateInstruction, command func(context.Context, string, ...string) ([]byte, error)) (bool, error) {
	label := fleetHelperLabel(c, i)
	if c.Detection.OS == "linux" {
		data, err := command(ctx, "systemctl", "--user", "show", label+".service", "--property=LoadState", "--value")
		if (err == nil || installExit(err, 4)) && strings.TrimSpace(string(data)) == "not-found" {
			return true, nil
		}
		if err == nil && strings.TrimSpace(string(data)) == "loaded" {
			if _, err := command(ctx, "systemctl", "--user", "is-active", "--quiet", label+".service"); err == nil {
				return false, nil
			}
		}
	} else {
		manager := installManager{c.Detection.OS, command}
		state, err := manager.inspect(ctx, installUnit{label: label})
		if err == nil && !state.loaded {
			return true, nil
		}
		if err == nil && state.active {
			return false, nil
		}
	}
	return false, errors.New("fleet_helper_absence_unproven")
}

func fleetVersion(ctx context.Context, path string, command func(context.Context, string, ...string) ([]byte, error)) (VersionInfo, error) {
	var info VersionInfo
	data, err := command(ctx, path, "version", "--json")
	if err != nil || json.Unmarshal(data, &info) != nil {
		return info, errors.New("fleet_version_invalid")
	}
	return info, nil
}

// ApplyFleetUpdate is the hidden finite helper entry point. It never restores
// old bytes, retries activation, resumes maintenance, or declares runtime proof.
// Restart intent is persisted before the manager call; restarting receipts never
// repeat that call. Installed receipts can safely finish their first restart.
func ApplyFleetUpdate(ctx context.Context, configPath, receiptPath string) error {
	return applyFleetUpdate(ctx, configPath, receiptPath, fleetInstallDeps{command: fleetCommand, client: fleetClient})
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
	client, err := d.client(c)
	if err != nil {
		return errors.New("fleet_credentials_unavailable")
	}
	client = fleetNoRedirect(client)
	defer client.HTTP.CloseIdleConnections()
	if c.Detection.OS == "darwin" {
		// launchctl submit creates no persistent definition or KeepAlive daemon.
		defer func() {
			_, _ = d.command(context.WithoutCancel(ctx), "launchctl", "remove", fleetHelperLabel(c, r.Instruction))
		}()
	}
	fail := func(code string) error {
		r.Phase, r.Failure = "failed", code
		saveErr := writeFleetReceipt(c, r)
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = fleetAck(ackCtx, client, r)
		if saveErr != nil {
			return errors.New("fleet_receipt_write_failed")
		}
		return errors.New(code)
	}
	if r.Phase == "failed" {
		_ = fleetAck(ctx, client, r)
		return errors.New("fleet_previous_failure")
	}
	a, err := fleetArtifact(c, r.Instruction)
	if err != nil {
		return fail("fleet_signature_refused")
	}
	if r.Phase == "restarting" {
		return fleetAck(ctx, client, r)
	}
	// Activation always needs the installer's existing preflight, including
	// when a colocated service already published the shared target binary.
	if c.ValidateInstall() != nil || c.ValidateInstallPolicies() != nil {
		return fail("fleet_preflight_failed")
	}
	roots, err := c.StateRoots()
	if err != nil || CheckStateRoots(roots...) != nil || prepareInstallNASMode(ctx, c, d.command, true) != nil {
		return fail("fleet_storage_preflight_failed")
	}
	manager := installManager{c.Detection.OS, d.command}
	unit := installUnit{path: c.Service, label: "local.mycelium." + c.Role}
	activate := func() error {
		if fleetAck(ctx, client, r) != nil {
			return fail("fleet_ack_failed")
		}
		if fleetAuthorize(ctx, client, r.Instruction) != nil {
			return fail("fleet_authorization_refused")
		}
		identity, err := EnrollmentServiceHealth(ctx, c, d.command)
		pid, _, _ := strings.Cut(identity, "/")
		if err != nil || pid != strconv.Itoa(r.BeforePID) {
			return fail("fleet_recovery_runtime_changed")
		}
		// Installed is proof that this helper has not issued a restart yet:
		// every manager call is preceded by durable restarting intent.
		r.Phase = "restarting"
		if writeFleetReceipt(c, r) != nil {
			return fail("fleet_receipt_write_failed")
		}
		if fleetAck(ctx, client, r) != nil {
			return fail("fleet_ack_failed")
		}
		if manager.activity(ctx, unit, true, true) != nil {
			return fail("fleet_restart_failed")
		}
		if waitFleetManager(ctx, c, r.BeforePID, d) != nil {
			return fail("fleet_health_failed")
		}
		return nil
	}
	if r.Phase == "installed" {
		hash, err := FileSHA256(c.Binary)
		if err != nil || hash != a.SHA256 {
			return fail("fleet_install_changed")
		}
		return activate()
	}
	if err = fleetAuthorize(ctx, client, r.Instruction); err != nil {
		return fail("fleet_authorization_refused")
	}
	before, err := FileSHA256(c.Binary)
	if err != nil {
		return fail("fleet_binary_unavailable")
	}
	if before == a.SHA256 {
		// Recover a crash between publication and receipt. No restart was
		// issued: its separate durable intent must precede the manager call.
		r.Phase = "installed"
		if writeFleetReceipt(c, r) != nil {
			return fail("fleet_receipt_write_failed")
		}
		return activate()
	}
	if before != r.BeforeSHA256 || CheckReleaseUpgrade(r.Instruction.Release.Manifest.Version, r.BeforeVersion, false, before == a.SHA256) != nil {
		return fail("fleet_version_refused")
	}
	state, err := manager.inspect(ctx, unit)
	if err != nil || !state.active || c.Detection.OS == "linux" && state.enabled == "not-found" || c.Detection.OS == "darwin" && !state.loaded {
		return fail("fleet_existing_service_unavailable")
	}
	staged, err := fetchFleetArtifact(ctx, c, client, r.Instruction, a)
	if err != nil {
		return fail("fleet_artifact_refused")
	}
	defer os.Remove(staged)
	if c.Detection.OS == "darwin" && (!nativeMachO(c.Binary) || !nativeMachO(staged) || validateDarwinUpgradeIdentity(ctx, c.Binary, staged, d.command) != nil) {
		return fail("fleet_darwin_identity_refused")
	}
	info, err := fleetVersion(ctx, staged, d.command)
	m := r.Instruction.Release.Manifest
	if err != nil || info.Version != m.Version || info.OS != a.OS || info.Arch != a.Arch || info.StartGate != m.StartGate || info.AttemptProtocol != m.AttemptProtocol || a.NativeSensing && !info.NativeSensing {
		return fail("fleet_candidate_invalid")
	}
	// Recheck local settings/bytes and queue authority after potentially slow I/O.
	current, currentConfig, err := fleetConfig(configPath)
	currentHash, hashErr := FileSHA256(c.Binary)
	if err != nil || currentConfig != configHash || ValidateInstallIdentity(c, current) != nil || hashErr != nil || currentHash != before {
		return fail("fleet_install_changed")
	}
	if _, err = fleetArtifact(c, r.Instruction); err != nil {
		return fail("fleet_signature_refused")
	}
	if fleetAuthorize(ctx, client, r.Instruction) != nil {
		return fail("fleet_authorization_refused")
	}
	// writeInstallFile is the installer's same-directory, fsynced atomic
	// publication primitive. No transaction/snapshot/restore path is involved.
	data, err := os.ReadFile(staged)
	if err != nil || fleetBytesHash(data) != a.SHA256 || int64(len(data)) != a.Size {
		return fail("fleet_artifact_refused")
	}
	st, err := os.Lstat(c.Binary)
	if err != nil || writeInstallFile(c.Binary, data, st.Mode().Perm(), installFiles{}, nil) != nil {
		return fail("fleet_publish_failed")
	}
	r.Phase = "installed"
	if writeFleetReceipt(c, r) != nil {
		return fail("fleet_receipt_write_failed")
	}
	return activate()
}

func fleetBytesHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func fetchFleetArtifact(ctx context.Context, c ServiceConfig, client Client, i FleetUpdateInstruction, a ReleaseArtifact) (string, error) {
	v := url.Values{"operation": {i.Operation}, "target": {a.File}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(client.URL, "/")+"/api/fleet/artifact?"+v.Encode(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+client.Token)
	// Large binaries need a transfer budget separate from small control calls.
	// This bounds a stuck transfer; no retries or production-work deadline.
	hc := *client.HTTP
	hc.Timeout = 10 * time.Minute
	res, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", errors.New("fleet_artifact_refused")
	}
	f, err := os.OpenFile(fleetStagingPath(c, i), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
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
	if err != nil || n != a.Size {
		return "", errors.New("fleet_artifact_refused")
	}
	var extra [1]byte
	if n, err := res.Body.Read(extra[:]); n != 0 || err != io.EOF || hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		return "", errors.New("fleet_artifact_refused")
	}
	if err := errors.Join(f.Chmod(0700), f.Sync(), f.Close()); err != nil {
		return "", err
	}
	keep = true
	return path, nil
}

func fleetStagingPath(c ServiceConfig, i FleetUpdateInstruction) string {
	return filepath.Join(filepath.Dir(c.Binary), "."+fleetHelperLabel(c, i)+".partial")
}

// Observe readiness, never retry activation. Match enrollment's one-minute
// readiness budget and 30-second stable-manager requirement: the bound stops
// failed startup without granting custody release or completing queue proof.
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
	if opt.StableFor <= 0 {
		opt.StableFor = 30 * time.Second
	}
	deadline := opt.Clock.Now().Add(opt.Timeout)
	var stableSince time.Time
	previous := ""
	for {
		probe, cancel := context.WithTimeout(ctx, 5*time.Second)
		identity, err := EnrollmentServiceHealth(probe, c, d.command)
		cancel()
		pid, _, _ := strings.Cut(identity, "/")
		if err == nil && pid != strconv.Itoa(beforePID) {
			if identity != previous {
				stableSince, previous = opt.Clock.Now(), identity
			}
			if opt.Clock.Now().Sub(stableSince) >= opt.StableFor {
				return nil
			}
		} else {
			previous = ""
		}
		if ctx.Err() != nil || !opt.Clock.Now().Before(deadline) {
			return errors.New("fleet_health_failed")
		}
		if opt.Wait(ctx, time.Second) != nil {
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
	code := "fleet_update_failed"
	if r.Failure == "fleet_health_failed" || r.Failure == "fleet_storage_preflight_failed" {
		code = r.Failure
	}
	return &FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: code}, nil
}
