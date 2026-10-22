package myce

import (
	"context"
	"fmt"
	"testing"
)

// #136: a saved capacity refusal must be retried after a larger allowance.
func TestExtensionPrepareCapacityChange(t *testing.T) {
	e := compositionExtension(t, true)
	q, w := prepareTestWorker(t, e)
	submit(t, q, Spec{Name: "capacity", Project: "test", Extension: extensionKey(e)})
	key := extensionPreparationKey(e, &w.Machine)
	dir := t.TempDir()
	failed := EnvironmentReport{Key: key, Kind: "extension-setup", State: "failed", Error: fmt.Sprintf("environment cache needs %d bytes; limit %d (in-use entries protected)", 30<<30, 20<<30)}
	if err := saveWorkerJSON(dir, "environment-reports.json", map[string]EnvironmentReport{key: failed}); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{20 << 30, 40 << 30} {
		rt := environmentRuntime{dir: dir, cache: &EnvironmentCache{Root: t.TempDir(), Limit: limit}, reports: map[string]EnvironmentReport{}}
		if err := rt.restore(); err != nil {
			t.Fatal(err)
		}
		_, w.Machine.EnvironmentReports, _ = rt.report()
		plan := check(t, q, w.Machine, nil).EnvironmentPlan
		if limit == 20<<30 {
			if len(plan) != 0 {
				t.Fatal("unchanged limit retried")
			}
			continue
		}
		if len(plan) != 1 {
			t.Fatal("larger allowance retained terminal capacity rejection", w.Machine.EnvironmentReports)
		}
		rt.prepare = func(context.Context, EnvironmentRecipe) error { return nil }
		rt.sync(plan, false)
		<-rt.done
		if rt.reports[key].State != "ready" {
			t.Fatal(rt.reports[key])
		}
	}
}
