package myce

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// #135: ordinary preparation must retain an unchanged uv check.
func TestExtensionPrepareCheckOrdinaryEnvironmentIdentity(t *testing.T) {
	e := compositionExtension(t, true)
	q, w := prepareTestWorker(t, e)
	w.environmentCache.Run = func(_ context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
		return os.MkdirAll(filepath.Join(dir, "venv", "bin"), 0700)
	}
	j := submit(t, q, Spec{Name: "ordinary", Project: "test", Extension: extensionKey(e)})
	plan := check(t, q, w.Machine, nil).EnvironmentPlan[0]
	if err := w.prepareRegisteredExtension(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	// Supply the exact successful-check records at the preparation boundary.
	reports := []ExtensionReport{}
	for _, work := range extensionWorks(e) {
		r := newExtensionReport(e, &w.Machine, work)
		r.Setup, r.Qualified, r.CheckID = true, true, "successful-queued-check"
		r.Reason = ""
		reports = append(reports, r)
	}
	if err := writeExtensionReports(w.ExtensionRoot, e, w.Machine.GPU, reports); err != nil {
		t.Fatal(err)
	}
	// Re-deliver preparation for the same frozen content and ready cache.
	if err := w.prepareRegisteredExtension(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, w.Machine.GPU, w.extensionFingerprint, &w.Machine)
	if qualifiedExtension(j, &w.Machine) == nil {
		t.Fatalf("unchanged environment lost qualification: before=%s after=%+v", reports[0].EnvironmentIdentity, w.Machine.ExtensionReports)
	}
}
