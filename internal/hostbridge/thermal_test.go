package hostbridge

import "testing"

func TestGPUCannotCertifyHost(t *testing.T) {
	for _, gpu := range []string{"nominal", "serious"} {
		s := Unknown()
		s.SetGPUHeat(gpu)
		want := "unknown"
		if gpu == "serious" || gpu == "fair" {
			want = gpu
		}
		if s.Heat != want || s.GPUHeat != gpu || s.HeatCoverage != "gpu-only" {
			t.Fatalf("%+v", s)
		}
	}
}
