package myce

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReleaseSlotProoflessWorkerInstall(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("installer runs under an ordinary test account")
	}
	q, clock := fixture(t)
	const token = "installer-fixture"
	server := httptest.NewServer(API{Queue: q, Token: token}.Handler())
	t.Cleanup(server.Close)
	c, source := preserveWorkerFixture(t)
	c.Queue, c.Enrollment = server.URL, true
	ctx := t.Context()
	writeCatalog := func(name string) (string, ModelConfig) {
		t.Helper()
		path := filepath.Join(t.TempDir(), name+".yaml")
		data := fmt.Sprintf("models:\n  - name: %s\n    engine: fake\n    path: fake\n    platforms: [fake]\n    memory_gb: 1\n", name)
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		catalog, err := ReadModels(path)
		if err != nil {
			t.Fatal(err)
		}
		return path, catalog
	}
	oldPath, oldCatalog := writeCatalog("previous")
	nextPath, nextCatalog := writeCatalog("current")
	c.ModelCatalog = oldPath
	if err := q.ConfigureModels(oldCatalog); err != nil {
		t.Fatal(err)
	}
	if err := Install(ctx, c, source, token, fakeInstallManager(c)); err != nil {
		t.Fatal(err)
	}
	_, _, manifest := signedTestRelease(t)
	publisher, signer, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.State, "fleet-publisher.pub"), publisher, 0600); err != nil {
		t.Fatal(err)
	}
	if err := PrepareStateRoots(custodianDir(c.State)); err != nil {
		t.Fatal(err)
	}
	m := machine(c.Name, "fake", c.HostID)
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	m.ModelConfigHash = modelConfigHash(oldCatalog)
	m.Enrollment = &EnrollmentProof{"v1.0.0", StartGateProtocol, AttemptProtocol, true}
	m.Release = &RunningRelease{Version: m.Enrollment.Version}
	if err := saveEnrollmentReceipt(c.State, m); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(c.State, "state.json", workerState{Session: m.Session}); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	// Bind the installed slot to a real executing image so Linux and Darwin
	// exercise their native mapping probes while the manager remains a fixture.
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	link := c.Binary + ".mapped"
	if err := os.Link(path, link); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(link, c.Binary); err != nil {
		t.Fatal(err)
	}
	before, err := readReleaseSlot(c.Binary)
	if err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(t.TempDir(), "candidate")
	next := []byte("new worker image")
	if err := os.WriteFile(candidate, next, 0700); err != nil {
		t.Fatal(err)
	}
	active, stops, starts, pid := true, 0, 0, os.Getpid()
	var replacement Machine
	manager := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "/usr/sbin/lsof" {
			return fleetCommand(ctx, name, args...)
		}
		if name != "systemctl" || len(args) < 2 {
			t.Fatalf("unexpected manager command %s %v", name, args)
		}
		text := strings.Join(args, " ")
		if strings.Contains(text, "myce-host-reconcile.") {
			if args[1] == "is-active" {
				return nil, installTestExit(4)
			}
			return fakeInstallManager(c)(ctx, name, args...)
		}
		switch {
		case strings.Contains(text, "MainPID"):
			return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", pid)), nil
		case strings.Contains(text, "UnitFileState"):
			return []byte("enabled\n"), nil
		case args[1] == "is-active" && !active:
			return nil, installTestExit(3)
		case args[1] == "stop":
			atStop, err := readReleaseSlot(c.Binary)
			if err != nil || atStop != before {
				t.Fatalf("service stopped after the slot was changed: %+v %v", atStop, err)
			}
			active, stops = false, stops+1
		case args[1] == "enable" || args[1] == "restart":
			active, starts = true, starts+1
			installedSlot, err := readReleaseSlot(c.Binary)
			if err != nil || installedSlot == before {
				return nil, err // Restored the original service after a failure.
			}
			installed, err := ReadServiceConfig(c.Config)
			if err != nil {
				return nil, err
			}
			catalog, err := ReadModels(installed.ModelCatalog)
			if err != nil {
				return nil, err
			}
			replacement = m
			replacement.Session += "-reinstalled"
			replacement.ModelConfigHash = modelConfigHash(catalog)
			replacement.Fleet = fleetTestProof(clock, replacement.Session, pid+1, true)
			replacement.Fleet.SHA256 = fleetBytesHash(next)
			_, err = q.Checkin(Checkin{Machine: replacement, Fresh: true, PreviousSession: m.Session, period: time.Second})
			return nil, err
		}
		return fakeInstallManager(c)(ctx, name, args...)
	}
	receiptPath := filepath.Join(c.State, "slot-reconciliation.json")
	refuse := func(want error) {
		t.Helper()
		if err := PreflightInstall(ctx, c, candidate, token, manager); !errors.Is(err, want) {
			t.Fatalf("preview = %v, want %v", err, want)
		}
		if err := Install(ctx, c, candidate, token, manager); !errors.Is(err, want) {
			t.Fatalf("install = %v, want %v", err, want)
		}
		if _, err := os.Stat(receiptPath); !errors.Is(err, os.ErrNotExist) || stops != 0 || starts != 0 {
			t.Fatalf("a refusal changed the installation: %v, stops=%d starts=%d", err, stops, starts)
		}
	}
	refuse(errFleetRecoveryNotQuiescent)
	job := maintenanceAssign(t, q, m, "experiment")
	if err := q.Started(result(job, false)); err != nil {
		t.Fatal(err)
	}
	drain := func() {
		t.Helper()
		if _, err := q.ChangeHostMaintenance(c.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
			t.Fatal(err)
		}
	}
	drain()
	refuse(errFleetRecoveryNotQuiescent)
	requireState(t, q, job.ID, "running")
	finish(t, q, job, true)
	check(t, q, m, nil)

	local := custodianName(ID(), "request")
	if err := os.WriteFile(filepath.Join(custodianDir(c.State), local), []byte("unreported launch"), 0600); err != nil {
		t.Fatal(err)
	}
	refuse(errFleetRecoveryNotQuiescent)
	if err := os.Remove(filepath.Join(custodianDir(c.State), local)); err != nil {
		t.Fatal(err)
	}
	pid++
	refuse(errFleetRecoveryWorker)
	pid--
	if err := os.Link(c.Binary, link); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(source, c.Binary); err != nil {
		t.Fatal(err)
	}
	refuse(errFleetRecoverySlot)
	if err := os.Rename(link, c.Binary); err != nil {
		t.Fatal(err)
	}
	if err := PreflightInstall(ctx, c, candidate, token, manager); err != nil {
		t.Fatal(err)
	}
	// Losing the drain after the first preflight still stops before activation.
	probes := 0
	err = installWithFiles(ctx, c, candidate, token, manager, installFiles{imageProbe: func(ctx context.Context, pid int, f *os.File, command func(context.Context, string, ...string) ([]byte, error)) error {
		probes++
		if probes == 2 {
			if _, err := q.ChangeHostMaintenance(c.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
				t.Fatal(err)
			}
		}
		return verifyInstallServiceImage(ctx, pid, f, command)
	}})
	if !errors.Is(err, errFleetRecoveryNotQuiescent) || stops != 0 || starts != 0 {
		t.Fatalf("lost drain reached activation: %v stops=%d starts=%d", err, stops, starts)
	}
	drain()
	// Even if the first file publication fails after the installer stops the
	// service, the transaction must reactivate the original installation.
	writeFailure := errors.New("injected receipt write failure")
	err = installWithFiles(ctx, c, candidate, token, manager, installFiles{before: func(op, path string) error {
		if op == "write" && path == receiptPath {
			if err := Install(ctx, c, candidate, token, manager); err == nil || err.Error() != "fleet_helper_busy" {
				t.Fatalf("concurrent install observed the temporary stop: %v", err)
			}
			return writeFailure
		}
		return nil
	}})
	if !errors.Is(err, writeFailure) || strings.Contains(err.Error(), "rollback incomplete") || !active || stops != 1 || starts != 1 {
		t.Fatalf("failed publication did not restore the active service: %v stops=%d starts=%d", err, stops, starts)
	}
	stops, starts = 0, 0
	// The old catalog still fences admission and supplies fresh identity after
	// the normal freshness window, even with no fleet runtime proof.
	if err := q.ConfigureModels(nextCatalog); err != nil {
		t.Fatal(err)
	}
	clock.Advance(LossAfter + time.Second)
	queued := submit(t, q, spec("experiment"))
	assertCatalogFencedReply(t, check(t, q, m, nil))
	nextConfig := c
	nextConfig.ModelCatalog = nextPath
	if err := PreflightInstall(ctx, nextConfig, candidate, token, manager); err != nil {
		t.Fatal(err)
	}
	// Protected Linux mappings can authenticate the incumbent slot with its
	// signed artifact. Missing or incorrect evidence stays a read-only refusal.
	privateMapping := installFiles{imageProbe: func(context.Context, int, *os.File, func(context.Context, string, ...string) ([]byte, error)) error {
		return os.ErrPermission
	}}
	if _, err := prepareInstall(ctx, nextConfig, candidate, token, manager, privateMapping, nil); !errors.Is(err, errFleetRecoveryManifest) {
		t.Fatalf("private mapping without signed evidence = %v", err)
	}
	manifestConfig := nextConfig
	manifestConfig.InstalledReleaseManifest = filepath.Join(t.TempDir(), "release.json")
	manifest.Version = m.Release.Version
	writeManifest := func() {
		t.Helper()
		payload, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := json.Marshal(SignedRelease{Manifest: payload, Signature: ed25519.Sign(signer, payload)})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestConfig.InstalledReleaseManifest, envelope, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeManifest()
	if _, err := prepareInstall(ctx, manifestConfig, candidate, token, manager, privateMapping, nil); !errors.Is(err, errFleetRecoveryManifest) {
		t.Fatalf("signed manifest for other bytes = %v", err)
	}
	for i, artifact := range manifest.Artifacts {
		if artifact.OS == c.Detection.OS && artifact.Arch == c.Detection.Arch {
			manifest.Artifacts[i].SHA256, manifest.Artifacts[i].Size = before.SHA256, before.Size
		}
	}
	writeManifest()
	plan, err := prepareInstall(ctx, manifestConfig, candidate, token, manager, privateMapping, nil)
	if err != nil || plan.reconciliation.Recovery.SlotEvidence != "signed_installed_release" {
		t.Fatalf("protected mapping did not reconcile the signed slot: %v", err)
	}
	if err := Install(ctx, nextConfig, candidate, token, manager); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(receiptPath)
	var receipt slotReconciliation
	if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.Running != nil || receipt.Recovery == nil || receipt.Recovery.Session != m.Session || receipt.Slot.SHA256 != before.SHA256 {
		t.Fatalf("recovery receipt lost evidence: %+v %v", receipt, err)
	}
	installed, err := readReleaseSlot(c.Binary)
	if err != nil || installed.SHA256 != fleetBytesHash(next) || stops != 1 || starts != 1 {
		t.Fatalf("recovery failed to install and start the candidate once: %+v %v stops=%d starts=%d", installed, err, stops, starts)
	}
	status, err := q.HostMaintenanceStatus(c.HostID)
	if err != nil || !status.Quiescent || status.Policy.Mode != "drained" || len(status.Workers) != 1 || !status.Workers[0].Fresh ||
		status.Workers[0].Machine.Session != replacement.Session || status.Workers[0].Machine.ModelConfigHash != modelConfigHash(nextCatalog) {
		t.Fatalf("recovery changed host maintenance: %+v %v", status, err)
	}
	requireState(t, q, queued.ID, "queued")
}
