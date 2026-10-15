package myce

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

// The optional extension observation belongs to this execution, outside its
// input/output directory. No unsupported accelerator counter is invented.
func programUsageFile(spool, identity string) string {
	return filepath.Join(spool, identity+".usage.json")
}

func collectProgramUsage(result *Result, process *os.ProcessState, path string) {
	usage := SizeHint{}
	if process != nil {
		if r, ok := process.SysUsage().(*syscall.Rusage); ok {
			bytes := float64(r.Maxrss)
			if runtime.GOOS != "darwin" {
				bytes *= 1024
			}
			usage.HostMemoryGB = bytes / (1 << 30)
		}
	}
	if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() && info.Size() <= 4096 {
		if f, err := os.Open(path); err == nil {
			defer f.Close()
			var report struct {
				SizeHint
				OutOfMemory bool `json:"out_of_memory"`
			}
			decoder := json.NewDecoder(io.LimitReader(f, 4097))
			decoder.DisallowUnknownFields()
			var extra any
			if decoder.Decode(&report) == nil && decoder.Decode(&extra) == io.EOF && validMemoryValue(report.MemoryGB) && validMemoryValue(report.HostMemoryGB) {
				usage.MemoryGB = report.MemoryGB
				usage.HostMemoryGB = max(usage.HostMemoryGB, report.HostMemoryGB)
				result.OutOfMemory = report.OutOfMemory && !result.Success
			}
		}
	}
	if usage.valid() {
		result.Usage = &usage
	}
}
