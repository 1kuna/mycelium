package myce

import (
	"errors"
	"strings"
	"testing"
)

func TestNativeNetworkPlacement(t *testing.T) {
	q, _ := fixture(t)
	offline := machine("offline", "fake")
	offline.Capabilities = []string{"native_isolation_landlock", "native_sidecars", "native_network_partial"}
	check(t, q, offline, nil)
	s := spec("experiment")
	s.Isolation = "sandboxed"
	s.Needs.Network = true
	_, err := q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "no_qualified_target" || !strings.Contains(api.Message, "needs.network: true") {
		t.Fatalf("unsupported online job was not clearly refused: %v", err)
	}
	if len(q.s.Jobs) != 0 {
		t.Fatal("unsupported job was queued indefinitely")
	}
	online := machine("online", "fake")
	online.Capabilities = []string{"native_isolation_seatbelt", "native_network", "native_sidecars"}
	check(t, q, online, nil)
	first := submit(t, q, s)
	if r := check(t, q, offline, nil); r.Job != nil {
		t.Fatal("partial network host received general egress job")
	}
	r := check(t, q, online, nil)
	if r.Job == nil || r.Job.ID != first.ID {
		t.Fatal("capable host did not receive online job", r.Job)
	}
	// A temporarily occupied qualified host still accepts queued work.
	second := submit(t, q, s)
	if second.State != "queued" {
		t.Fatal("busy online target prevented normal queuing", second.State)
	}
}

// Native policy is not an additional prerequisite for explicitly contained
// Docker commands; their existing runtime/network policy continues to apply.
func TestNativeNetworkDockerPlacement(t *testing.T) {
	j := dockerJob()
	j.Spec.Needs.Network = true
	m := machine("docker", "fake")
	if !nativeNetworkFits(&j, &m) || nativeNetworkRequired(&j) {
		t.Fatal("native network policy applied to Docker")
	}
}

func TestNativeNetworkExtensionDeclaration(t *testing.T) {
	for _, caller := range []bool{false, true} {
		e := compositionExtension(t, false)
		e.Variants[0].Needs.Network = !caller
		m := machine("extension", "fake")
		m.CapabilityProfile.OS = e.Variants[0].Target.OS
		m.CapabilityProfile.Arch = e.Variants[0].Target.Arch
		j := Job{Spec: Spec{Extension: extensionKey(e), Kind: "sample", Needs: ResourceNeeds{Network: caller}}, Extensions: []Extension{e}}
		candidates := extensionJobs(&j, &m, false)
		if !nativeNetworkRequired(&j) || len(candidates) != 1 || !candidates[0].Spec.Needs.Network {
			t.Fatalf("caller=%v: network declaration lost during lowering", caller)
		}
	}
}

// A mixed-version fleet must never dispatch an explicitly sandboxed native job to a worker
// whose report predates isolation, even though it can execute plain commands.
func TestNativeIsolationMixedWorkerPlacement(t *testing.T) {
	q, _ := fixture(t)
	old := machine("old", "fake")
	old.Capabilities = nil
	check(t, q, old, nil)
	s := spec("experiment")
	s.Isolation = "sandboxed"
	j := submit(t, q, s)
	for _, caps := range [][]string{nil, {"native_isolation_unavailable"}, {"native_network"}} {
		old.Capabilities = caps
		if r := check(t, q, old, nil); r.Job != nil {
			t.Fatalf("unisolated worker received job with %v", caps)
		}
	}
	current := machine("current", "fake")
	if r := check(t, q, current, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("isolated worker did not receive ordinary native job", r.Job)
	}
}
