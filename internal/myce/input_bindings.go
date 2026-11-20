package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// Leave room for the command, activation and worker environment even on hosts
// with a small exec budget. Larger binding sets travel only in the manifest;
// there is no binding-count limit and no partial set of legacy variables.
const inputBindingEnvironmentBytes = 32 << 10
const inputBindingsVariable = "MYCE_BINDINGS_FILE"

type inputBindings struct {
	Inputs map[string]string `json:"inputs"`
	Models map[string]string `json:"models"`
}

func bindManagedInputs(work string, j Job, acquired, env map[string]string) error {
	delete(env, inputBindingsVariable)
	if len(j.Spec.SharedInputs)+len(j.Spec.ModelSnapshots) == 0 {
		return nil
	}
	bindings := inputBindings{Inputs: map[string]string{}, Models: map[string]string{}}
	bytes := 0
	add := func(name, prefix string, paths map[string]string) {
		key := prefix + strings.ToUpper(name)
		paths[name] = acquired[key]
		// Include the terminator and the pointer in the exec environment.
		bytes += len(key) + 1 + len(acquired[key]) + 1 + 8
	}
	for name := range j.Spec.SharedInputs {
		add(name, "MYCE_INPUT_", bindings.Inputs)
	}
	for name := range j.Spec.ModelSnapshots {
		add(name, "MYCE_MODEL_", bindings.Models)
	}
	data, err := json.Marshal(bindings)
	if err != nil {
		return err
	}
	// The manifest is payload data, inside the existing attempt lifecycle and
	// read grant. It never supplies isolation grants; those use acquired paths.
	f, err := os.CreateTemp(work, ".myce-bindings-*.json")
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Close()
	} else {
		_ = f.Close()
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	env[inputBindingsVariable] = filepath.Clean(f.Name())
	if bytes > inputBindingEnvironmentBytes {
		for key := range acquired {
			delete(env, key)
		}
	}
	return nil
}
