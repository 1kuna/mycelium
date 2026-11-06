//go:build !linux

package myce

func processMemoryHeadroom(available int64) int64 { return available }
