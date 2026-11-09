package hostbridge

import "testing"

func TestNVIDIAThermalMargin(t *testing.T) {
	for _, tt := range []struct{ raw, want string }{
		{"38, 46\r\n", "nominal"}, // T.Limit is margin, not a 46 C ceiling.
		{"80, 4", "fair"}, {"84, 0", "serious"}, {"87, -3", "serious"},
		{"38, 46\n84, 0", "serious"}, {"38, N/A", "unknown"},
		{"", "unknown"}, {"38, 46\nbad", "unknown"}, {"NaN, 4", "unknown"},
		{"201, 46", "unknown"}, {"38, 201", "unknown"},
	} {
		if got := NVIDIAHeat(tt.raw); got != tt.want {
			t.Errorf("%q: %s, want %s", tt.raw, got, tt.want)
		}
	}
}
func TestGPUCannotCertifyHost(t *testing.T) {
	for _, gpu := range []string{"nominal", "fair", "serious", "unknown"} {
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
