//go:build !linux

package myce

func (w *Worker) checkNativeSeqpacketStartup() {}
