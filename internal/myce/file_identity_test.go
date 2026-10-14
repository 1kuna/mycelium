package myce

import "testing"

func TestProgramNeedsIdentityPreservesNASAndIncludesStoredBytes(t *testing.T) {
	j := &Job{Spec: Spec{Project: "demo", Program: &Program{Type: "script", File: "train.py"}, Inputs: map[string]string{"train.py": "programs/train.py"}}}
	e := Extension{Digest: "digest"}
	// Frozen v3 identity before file transport was added. Existing NAS learning
	// must survive this upgrade without quietly losing its measurements.
	const legacy = "174b6e8ab06bf1fe1e4ed74e3b53da398a232d2b2a7a5cd1801a76b878deb031"
	if got := programNeedsKey(j, e, "fake"); got != legacy {
		t.Fatalf("NAS identity changed: %s", got)
	}
	j.Spec.Inputs = nil
	j.Spec.Files = FileManifest{"train.py": {SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 4}}
	before := programNeedsKey(j, e, "fake")
	j.Spec.Files["train.py"] = FileRef{SHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", SizeBytes: 4}
	if got := programNeedsKey(j, e, "fake"); got == before {
		t.Fatal("changed stored bytes reused prior program needs")
	}
}
