//go:build !linux

package myce

func processMemoryHeadroom(available int64) (int64, error) { return available, nil }
