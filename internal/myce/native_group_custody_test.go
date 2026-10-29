package myce

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeGroupCustodyAccelerator(t *testing.T) {
	for _, platform := range []string{"cpu", "xpu"} {
		for _, sidecars := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sidecars=%v", platform, sidecars), func(t *testing.T) {
				s := nativeSpec(t)
				if platform == "cpu" {
					s.Needs = cpuSpec(1, 2).Needs
				}
				if !sidecars {
					s.Run.Sidecars = nil
					s.Run.Command = []string{"sh", "-c", `touch "$NATIVE_TEST_DIR/main-ready"; while [ ! -f "$NATIVE_TEST_DIR/finish" ]; do sleep .01; done`}
				}
				r := nativeStart(t, s, platform)
				nativeFile(t, r.dir, "main-ready")
				checkCustody := func(name string) {
					t.Helper()
					data, err := os.ReadFile(filepath.Join(r.s.dir, name))
					if err != nil {
						t.Fatal(err)
					}
					var custody workerState
					if err := json.Unmarshal(data, &custody); err != nil {
						t.Fatal(err)
					}
					if custody.Accelerator != (platform != "cpu") {
						t.Errorf("%s custody accelerator=%v", platform, custody.Accelerator)
					}
				}
				checkCustody("attempt-" + r.s.state.Identity + ".json")
				nativeTouch(t, r.dir, "finish")
				if result := nativeFinish(t, r); !result.Success {
					t.Fatal(result)
				}
				checkCustody("attempt-" + r.s.state.Identity + ".json")
				if !sidecars && platform != "cpu" {
					checkCustody(r.s.state.Identity + ".stop.json")
				}
			})
		}
	}
}
