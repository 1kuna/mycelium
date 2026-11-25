package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #253: an already accepted original composed bundle recovers without an
// authoring reader, a local installation, or a submitter-owned check directory.
func TestExtensionFirstDemandFrozenRecoveryDev(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "composed"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) { extensionFrozenRecoveryDev(t, legacy) })
	}
}

func extensionFrozenRecoveryDev(t *testing.T, legacy bool) {
	root := newTestStateRoot(t)
	c := DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"),
		FileStore: filepath.Join(root, "files"), ExtensionRoot: filepath.Join(root, "extensions"), Token: "frozen-recovery-fixture",
		Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}
	t.Setenv("MYCE_TOKEN", c.Token)
	e := compositionExtension(t, false)
	setup := []string{"sh", "-ec", "printf prepared > \"$MYCE_EXTENSION_CACHE/prepared\""}
	command := []string{"sh", "{extension}/frozen-run.sh"}
	if legacy {
		e = fakeExtension(t)
		e.ProgramTypes = nil
		delete(e.Baselines, "program:sample-script")
		e.Setup, e.Run = setup, command
	} else {
		e.Kinds = nil
		v := &e.Variants[0]
		delete(v.Baselines, "kind:sample")
		v.Setup, v.Execution.Main.Command = setup, command
	}
	e.Files["frozen-run.sh"] = []byte("test -z \"$MYCE_TOKEN\" || exit 99\nprintf 'frozen-result\\n' > output\n")
	e = freezeComposition(t, e)
	originalDescription := bytes.Clone(e.Files["extension.yaml"])
	q, err := Open(c.Database, c.Checkpoints, RealClock{}, WithFileStore(c.FileStore, 8<<20, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.AddExtension(e); err != nil {
		q.Close()
		t.Fatal(err)
	}
	s := Spec{Name: "frozen-native", Project: "recovery", SubmissionKey: "accepted-before-worker", Extension: extensionKey(e), ExtensionDigest: e.Digest,
		Outputs: map[string]string{"output": "results"}}
	if legacy {
		s.Extension, s.ExtensionDigest, s.Kind = "", "", "sample"
	}
	accepted, err := q.Submit(s)
	if err != nil {
		q.Close()
		t.Fatal(err)
	}
	fingerprint := q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)].Fingerprint
	if accepted.State != "queued" || len(q.s.Machines) != 0 {
		t.Fatal("job was not accepted before worker startup")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{c.ExtensionRoot, filepath.Join(root, "workers", "fake-1", "environment-cache")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("cold worker cache already exists", err)
		}
	}
	d, err := startTestDev(t, c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Bound a failed dev test and guarantee teardown; scheduling itself uses
	// the real dev queue's injected Clock, never this test's deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	completed, err := d.Client().Wait(ctx, accepted.ID)
	if err != nil || completed.State != "succeeded" {
		t.Fatalf("frozen useful job: %+v %v", completed, err)
	}
	installed, err := ReadExtension(extensionFolder(c.ExtensionRoot, e))
	if err != nil || installed.Digest != e.Digest || !bytes.Equal(installed.Files["extension.yaml"], originalDescription) {
		t.Fatal("original frozen decoder/bundle did not survive recovery", err)
	}
	destination := filepath.Join(root, "downloaded-result")
	if err := d.Client().DownloadResult(ctx, completed.ID, "output", destination); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != "frozen-result\n" {
		t.Fatal("accepted result bytes", err)
	}
	sum := sha256.Sum256(data)
	ref := completed.OutputRefs["output"]
	if ref.SizeBytes != int64(len(data)) || ref.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("accepted result hash/size differs from downloaded bytes")
	}
	replayed, err := d.Client().Submit(ctx, s)
	if err != nil || replayed.ID != accepted.ID || !replayed.SubmissionReused {
		t.Fatal("cold recovery replay created a new job", err)
	}
	d.Queue.mu.RLock()
	after := d.Queue.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)].Fingerprint
	d.Queue.mu.RUnlock()
	if after != fingerprint {
		t.Fatal("cold recovery changed the original submission fingerprint")
	}
	snapshot, err := d.Queue.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	checks := 0
	for _, j := range snapshot.Jobs {
		if r := automaticExtensionCheck(j); r != nil {
			checks++
			if j.State != "succeeded" || !j.ReleaseConfirmed || r.Digest != e.Digest || j.Machine != completed.Machine || j.Attempt != 1 {
				t.Fatal("useful work lacks accepted exact-host frozen qualification", j.ID, j.State)
			}
		}
	}
	if checks != 1 {
		t.Fatal("cold recovery duplicated or omitted native checks", checks)
	}
	if marker, err := os.ReadFile(filepath.Join(extensionCache(c.ExtensionRoot, e, "fake"), "prepared")); err != nil || string(marker) != "prepared" {
		t.Fatal("cold native setup was not prepared", err)
	}
}

func TestConsumerQualificationDev(t *testing.T) {
	root := newTestStateRoot(t)
	c := DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"),
		FileStore: filepath.Join(root, "files"), ExtensionRoot: filepath.Join(root, "extensions"), Token: "consumer-qualification-fixture",
		Workers: 1, Devices: 2, HostID: "test-worker", MemoryGB: 8, Interval: 10 * time.Millisecond}
	t.Setenv("MYCE_TOKEN", c.Token)
	e := compositionExtension(t, false)
	e.Files["check.sh"] = append([]byte("test ! -f \"$MYCE_EXTENSION_CACHE/fail\" || { printf '[99]' > \"$MYCE_CHECK_OUTPUT\"; exit 0; }\n"), e.Files["check.sh"]...)
	e = freezeComposition(t, e)
	d, err := startTestDev(t, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	awaitTestDevDeviceFingerprints(t, d)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := d.Client()
	if err := client.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
		t.Fatal(err)
	}
	request := ConsumerQualification{Project: "consumer", Extension: extensionKey(e), Digest: e.Digest, Worker: c.HostID, Platform: "fake", Device: "1"}
	submitCheck := func() QualificationVerdict {
		t.Helper()
		var v QualificationVerdict
		if err := client.Call(ctx, "POST", "/api/extension-checks", request, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	read := func(path string) QualificationVerdict {
		t.Helper()
		var v QualificationVerdict
		if err := client.Call(ctx, "GET", path, nil, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	first := submitCheck()
	if first.State != "queued" || first.Verdict != "pending" || first.Reused || first.JobID == "" || first.CheckID == "" {
		t.Fatal("missing queued receipt", first)
	}
	j, err := client.Wait(ctx, first.JobID)
	if err != nil || j.State != "succeeded" || !j.ReleaseConfirmed || j.Machine != request.Worker || len(j.AllocatedDevices) != 1 || j.AllocatedDevices[0] != request.Device {
		t.Fatalf("check did not succeed on the exact worker device: %+v (%v)", j, err)
	}
	byCheck := read("/api/extension-checks/" + first.CheckID)
	byJob := read("/api/jobs/" + first.JobID + "/extension-check")
	if byCheck != byJob || byJob.Verdict != "passed" || byJob.Digest != request.Digest || byJob.Worker != request.Worker || byJob.Device != request.Device || byJob.Attempt != 1 {
		t.Fatal("incorrect or inconsistent verdict readback", byCheck, byJob)
	}
	if v := submitCheck(); !v.Reused || v.CheckID != first.CheckID || v.Verdict != "passed" {
		t.Fatal("repeat did not reuse released evidence", v)
	}
	awaitReport := func(checkID string, qualified bool) {
		t.Helper()
		await(t, func() bool {
			d.Queue.mu.RLock()
			defer d.Queue.mu.RUnlock()
			m := d.Queue.qualificationTarget(request.Worker, request.Platform, request.Device)
			for _, r := range m.ExtensionReports {
				if r.CheckID == checkID && r.Qualified == qualified {
					return true
				}
			}
			return false
		})
	}
	awaitReport(first.CheckID, true)
	request.Project = "other-consumer"
	if v := submitCheck(); !v.Reused || v.CheckID != first.CheckID {
		t.Fatal("project could not reuse existing native evidence", v)
	}
	request.Project = "consumer"
	request.Recheck = true
	if err := os.WriteFile(filepath.Join(extensionCache(c.ExtensionRoot, e, "fake"), "fail"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	second := submitCheck()
	if second.CheckID == first.CheckID || second.Verdict != "pending" || second.Reused {
		t.Fatal("recheck did not enqueue a fresh invocation", second)
	}
	j, err = client.Wait(ctx, second.JobID)
	if j == nil || j.State != "failed" || !j.ReleaseConfirmed {
		t.Fatalf("incorrect baseline did not fail the job: %+v (%v)", j, err)
	}
	request.Recheck = false
	if v := submitCheck(); !v.Reused || v.CheckID != second.CheckID || v.Verdict != "failed" {
		t.Fatal("repeat did not retain the latest failed verdict", v)
	}
	awaitReport(second.CheckID, false)
	request.Project = "other-consumer"
	if v := submitCheck(); !v.Reused || v.CheckID != second.CheckID || v.Verdict != "failed" {
		t.Fatal("a failed recheck left the older pass available to another project", v)
	}
	if v := read("/api/extension-checks/" + first.CheckID); v.Verdict != "passed" {
		t.Fatal("recheck mutated the earlier verdict", v)
	}
}
