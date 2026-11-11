//go:build !darwin && !linux

package myce

func modelRootLocalFilesystem(string) bool { return false }
