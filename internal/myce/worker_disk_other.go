//go:build !linux

package myce

import "syscall"

func diskBlockSize(st *syscall.Statfs_t) uint64 { return uint64(st.Bsize) }
