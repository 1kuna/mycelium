package myce

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Local publication barriers, also used by isolated crash fixtures. No callbacks
// into the queue are allowed here. A failed barrier never means success.
type durableFileOps struct {
	syncFile func(*os.File) error
	rename   func(string, string) error
	syncDir  func(string) error
}

func (o durableFileOps) flush(f *os.File) error {
	if o.syncFile != nil {
		return o.syncFile(f)
	}
	return syncLocalFile(f)
}
func (o durableFileOps) move(from, to string) error {
	if o.rename != nil {
		return o.rename(from, to)
	}
	return os.Rename(from, to)
}
func (o durableFileOps) directory(path string) error {
	if o.syncDir != nil {
		return o.syncDir(path)
	}
	return syncLocalDirectory(path)
}

func (o durableFileOps) replace(path string, write func(*os.File) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".myce-write-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = write(f); err == nil {
		err = o.flush(f)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = o.move(f.Name(), path); err != nil {
		return err
	}
	return o.directory(filepath.Dir(path))
}

func (o durableFileOps) bytes(path string, data []byte) error {
	return o.replace(path, func(f *os.File) error {
		n, err := f.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		return err
	})
}

// Persist newly created ancestor names from the first existing anchor outward.
// Re-sync an existing leaf's parent too: it may be the directory left by a
// previous process death between mkdir and its parent barrier.
func (o durableFileOps) mkdir(path string) error {
	path = filepath.Clean(path)
	st, err := os.Lstat(path)
	if err == nil {
		if !st.IsDir() {
			return errors.New("state directory must be a directory, not a symlink")
		}
		return o.directory(filepath.Dir(path))
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(path)
	if err = o.mkdir(parent); err != nil {
		return err
	}
	if err = os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	if err = o.directory(path); err != nil {
		return err
	}
	return o.directory(parent)
}

// File-store roots must be local. Do not inherit the checkpoint CIFS directory
// fsync exemption: an unsupported name barrier is a publication failure here.
func syncLocalDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
