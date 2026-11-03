package myce

import (
	"strings"
	"testing"
)

func TestResolveNativeAuthoredMetadataRemainsUnqualified(t *testing.T) {
	q, _, _, s := observationFixture(t)
	e := q.s.Extensions[s.Extension]
	// Even a caller-supplied registration digest cannot turn local metadata into
	// qualified evidence. The existing portability reader clears it.
	before := string(wireJSON(t, q.s))
	r, err := q.Resolve(ResolveRequest{Spec: Spec{Project: s.Project}, Recipe: &e})
	if err != nil {
		t.Fatal(err)
	}
	if r.Portability == nil || len(r.Targets) == 0 {
		t.Fatalf("native metadata not resolved: %+v", r)
	}
	for _, target := range r.Targets {
		if target.Eligible || !strings.Contains(target.Reason, "frozen native registration") {
			t.Fatalf("unfrozen metadata claimed eligible: %+v", target)
		}
	}
	if after := string(wireJSON(t, q.s)); before != after {
		t.Fatal("native resolution mutated queue")
	}
}

func TestResolveRegisteredNativeEnvironmentBackend(t *testing.T) {
	for _, backend := range []string{"uv", "pixi"} {
		t.Run(backend, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, true)
			variant := &e.Variants[0]
			if backend == "pixi" {
				variant.Environments["main"] = EnvironmentRecipe{Backend: "pixi", LockFile: "locks/pixi.lock"}
				e.Files["locks/pixi.lock"] = []byte("locked offline fixture")
				e.Files["locks/pixi.toml"] = []byte("[workspace]\nchannels=[]\nplatforms=['linux-64','osx-arm64']\n")
			}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			m := machine("native", "fake")
			m.LastSeen = clock.Now()
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
			m.ExtensionFingerprint = "synthetic-native-v1"
			recipe := e.Variants[0].Environments["main"]
			m.Environments = []string{recipe.Key}
			report := newExtensionReport(e, &m, "extension:"+e.Name)
			report.Setup, report.Qualified, report.CheckID = true, true, "synthetic-native-check"
			m.ExtensionReports = []ExtensionReport{report}
			q.s.Machines[m.ID] = &m
			r, err := q.Resolve(ResolveRequest{Spec: Spec{Name: "native", Project: "fixture", Extension: extensionKey(e)}})
			if err != nil || len(r.Targets) != 1 || r.Targets[0].Environment == nil || r.Targets[0].Environment.Key != recipe.Key || r.Targets[0].Environment.Backend != backend {
				t.Fatalf("registered environment misidentified: %+v %v", r, err)
			}
		})
	}
}
