package myce

import "syscall"

func nativeStorageFilesystemIdentity(fd int) (storageFilesystemIdentity, error) {
	var fs syscall.Statfs_t
	var st syscall.Stat_t
	if err := syscall.Fstatfs(fd, &fs); err != nil {
		return storageFilesystemIdentity{}, err
	}
	if err := syscall.Fstat(fd, &st); err != nil {
		return storageFilesystemIdentity{}, err
	}
	return linuxStorageFilesystemIdentity(fs, st), nil
}

func linuxStorageFilesystemIdentity(fs syscall.Statfs_t, st syscall.Stat_t) storageFilesystemIdentity {
	return storageFilesystemIdentity{Platform: "linux", FSID: fs.Fsid.X__val, Device: uint64(st.Dev)}
}
