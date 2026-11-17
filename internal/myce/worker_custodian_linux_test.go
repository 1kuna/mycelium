package myce

import "testing"

// #1709: the CLI guard must recognize an exact custodian scope while keeping
// the ordinary worker-service allowlist closed to scopes and unrelated units.
func TestCustodianEntryScopeIdentity(t *testing.T) {
	id := "0123456789abcdef0123456789abcdef"
	base := "/user.slice/user-1000.slice/user@1000.service/myce-jobs.slice/" + custodianUnit(id)
	for _, suffix := range []string{"", "/worker"} {
		b := []byte("0::" + base + suffix + "\n")
		if got, err := custodianCgroup(b, id); err != nil || got != base {
			t.Fatalf("valid independent scope refused: %q %v", got, err)
		}
		if got := workerUnitFromCgroup(b); got != "" {
			t.Fatalf("scope widened worker allowlist: %s", got)
		}
	}
	for _, path := range []string{"/myce-worker.service", base + "/payload", "/" + custodianUnit("ffffffffffffffffffffffffffffffff")} {
		if _, err := custodianCgroup([]byte("0::"+path), id); err == nil {
			t.Fatal("unrelated cgroup accepted", path)
		}
	}
	if _, err := custodianCgroup([]byte("0::"+base), "../invalid"); err == nil {
		t.Fatal("invalid attempt identity accepted")
	}
}
