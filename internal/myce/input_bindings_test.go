package myce

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestManagedInputBindings(t *testing.T) {
	for _, count := range []int{1, 771, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			j, acquired := syntheticInputBindings(count)
			env := map[string]string{"KEEP": "authored", "MYCE_TOKEN": "synthetic"}
			for key, value := range acquired {
				env[key] = value
			}
			work := t.TempDir()
			if err := bindManagedInputs(work, j, acquired, env); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(env[inputBindingsVariable])
			if err != nil {
				t.Fatal(err)
			}
			var bindings inputBindings
			if err := json.Unmarshal(data, &bindings); err != nil {
				t.Fatal(err)
			}
			if len(bindings.Inputs) != count || len(bindings.Models) != 1 || env["KEEP"] != "authored" {
				t.Fatal("binding manifest or authored environment lost")
			}
			for name, path := range bindings.Inputs {
				if path != acquired["MYCE_INPUT_"+strings.ToUpper(name)] {
					t.Fatal("manifest changed an acquired binding")
				}
			}
			legacy := 0
			for key := range acquired {
				if env[key] != "" {
					legacy++
				}
			}
			if count == 1 && legacy != len(acquired) || count > 1 && legacy != 0 {
				t.Fatalf("legacy environment is partial or unbounded: %d", legacy)
			}
			if len(strings.Join(environmentList(env), "\x00")) > inputBindingEnvironmentBytes {
				t.Fatal("bindings exceeded exec environment budget")
			}
			cmd := exec.Command("/bin/sh", "-ec", `test -r "$MYCE_BINDINGS_FILE"; test -z "${MYCE_TOKEN+x}"; test "$KEEP" = authored`)
			cmd.Env = environmentList(env)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("bounded launch: %v: %s", err, out)
			}
		})
	}
}

// Synthetic public names and Linux cache-shaped paths; no real source, input
// data, host identity or service environment participates in these exec tests.
func syntheticInputBindings(count int) (Job, map[string]string) {
	j := Job{Spec: Spec{SharedInputs: map[string]SharedInput{}, ModelSnapshots: map[string]ModelSnapshot{}}}
	paths := map[string]string{}
	for i := range count {
		name := fmt.Sprintf("input_%04d_%s", i, strings.Repeat("a", 48))
		j.Spec.SharedInputs[name] = SharedInput{}
		paths["MYCE_INPUT_"+strings.ToUpper(name)] = "/var/lib/worker/shared-inputs/" + fmt.Sprintf("%064x", i) + "/data"
	}
	j.Spec.ModelSnapshots["model"] = ModelSnapshot{}
	paths["MYCE_MODEL_MODEL"] = "/var/lib/worker/models/synthetic/data"
	return j, paths
}
