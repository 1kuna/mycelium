package myce

import "testing"

func TestFixedAcceleratorMemoryExtensionPlacement(t *testing.T) {
	for _, mode := range []string{"qualified", "prepare"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			e := compositionExtension(t, false)
			e.Variants[0].Target.Platform = "xpu"
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			m := placementMachine("fixed-xpu", "separate", 64, 31, e)
			m.GPU = "xpu"
			m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "xpu"}}
			m.ExtensionReports = nil
			m.EnvironmentBuilder = true
			if mode != "prepare" {
				r := newExtensionReport(e, &m, "kind:sample")
				r.Setup, r.Qualified, r.CheckID = true, mode == "qualified", "check"
				m.ExtensionReports = []ExtensionReport{r}
			}
			s := Spec{Name: "fixed-xpu", Project: "test", Extension: extensionKey(e), Kind: "sample", AllowedMachines: []string{m.ID}}
			if mode == "check" {
				s.Kind = ""
				s.ExtensionDigest = e.Digest
				s.ExtensionCheck = &ExtensionCheckRequest{Work: "kind:sample", Directory: t.TempDir()}
			}
			j := submit(t, q, s)
			if reply := check(t, q, m, nil); reply.Job != nil || len(reply.EnvironmentPlan) != 0 {
				t.Fatal("unsized accelerator admitted work")
			}
			applyFixedAcceleratorMemory(m.CapabilityProfile, m, true)
			reply := check(t, q, m, nil)
			if mode == "prepare" {
				if len(reply.EnvironmentPlan) != 1 {
					t.Fatalf("fixed-budget preparation blocked: %+v", reply)
				}
			} else if reply.Job == nil || reply.Job.ID != j.ID {
				t.Fatalf("fixed-budget %s blocked: %s", mode, job(t, q, j.ID).Reason)
			}
		})
	}
}
