package myce

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

func nativeSeqpacketPathOwner(name string) (uint32, bool) {
	if !filepath.IsAbs(name) {
		return 0, false
	}
	info, err := os.Lstat(name)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return 0, false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Uid, true
}

func nativeSeqpacketStartupWarning(data []byte, readErr error) string {
	endpoints, err := nativeSeqpacketEndpoints(data)
	if readErr != nil || err != nil {
		log.Printf("WARNING: native SEQPACKET startup inventory unavailable: %v", errors.Join(readErr, err))
		return "SEQPACKET startup inventory unavailable; pathname endpoint exposure unverified"
	}
	unexpected := nativeUnexpectedSeqpacket(endpoints, nativeSeqpacketPathOwner)
	for _, endpoint := range unexpected {
		uid, known := nativeSeqpacketPathOwner(endpoint.Name)
		log.Printf("WARNING: unexpected listening SEQPACKET endpoint name=%q inode=%q owner_uid=%d owner_known=%t; same-UID pathname SEQPACKET endpoints reachable (partial)", endpoint.Name, endpoint.Inode, uid, known)
	}
	if len(unexpected) != 0 {
		return fmt.Sprintf("%d unexpected listening SEQPACKET endpoint(s); inspect worker startup warnings", len(unexpected))
	}
	return ""
}

func (w *Worker) checkNativeSeqpacketStartup() {
	if w.NativeMinimalRoot {
		return
	} // No fallback selection or advisory inventory in this opt-in path.
	if nativeLinuxDetection().mode != "landlock" {
		return
	}
	data, err := os.ReadFile("/proc/net/unix")
	w.Machine.NativeIsolationWarning = nativeSeqpacketStartupWarning(data, err)
}
