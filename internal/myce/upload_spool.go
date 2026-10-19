package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// DefaultUploadSpoolBytes bounds the queue's held media to two maximum uploads.
const DefaultUploadSpoolBytes int64 = 16 << 30

var errUploadSpoolFull = errors.New("upload spool byte budget exhausted; retry after outstanding calls finish or increase --upload-spool-bytes")

type uploadSpool struct {
	mu          sync.Mutex
	dir         string
	lock        *os.File
	defaultDir  string
	limit, used int64
}

// ConfigureUploadSpool configures queue-local storage before serving requests.
// An empty directory uses a private directory beside the queue DB. The budget must be positive.
func (q *Queue) ConfigureUploadSpool(dir string, budget int64) error {
	if budget <= 0 {
		return errors.New("upload-spool-bytes must be positive")
	}
	q.uploadSpool.mu.Lock()
	defer q.uploadSpool.mu.Unlock()
	if q.uploadSpool.used != 0 {
		return errors.New("cannot configure upload spool while uploads are held")
	}
	if dir == "" {
		dir = q.uploadSpool.defaultDir
	}
	if err := PrepareStateRoots(dir); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if abs != q.uploadSpool.dir || q.uploadSpool.lock == nil {
		lock, err := os.OpenFile(abs+".lock", os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			lock.Close()
			return errors.New("upload spool already in use")
		}
		files, err := filepath.Glob(filepath.Join(abs, "myce-model-request-*"))
		if err == nil {
			for _, file := range files {
				if err = os.Remove(file); err != nil {
					break
				}
			}
		}
		if err != nil {
			lock.Close()
			return errors.New("upload spool startup cleanup failed")
		}
		if q.uploadSpool.lock != nil {
			q.uploadSpool.lock.Close()
		}
		q.uploadSpool.lock = lock
	}
	dir = abs
	q.uploadSpool.dir, q.uploadSpool.limit = dir, budget
	return nil
}

func (s *uploadSpool) reserve(n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > s.limit-s.used {
		return errUploadSpoolFull
	}
	s.used += n
	return nil
}
func (s *uploadSpool) release(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.used -= n
}

type spooledModelRequest struct {
	*os.File
	spool                   *uploadSpool
	held, written, maxBytes int64
}

func (f *spooledModelRequest) Write(p []byte) (int, error) {
	if int64(len(p)) > f.maxBytes-f.written {
		return 0, errModelRequestTooLarge
	}
	need := f.written + int64(len(p)) - f.held
	if need > 0 {
		if err := f.spool.reserve(need); err != nil {
			return 0, err
		}
		f.held += need
	}
	n, err := f.File.Write(p)
	f.written += int64(n)
	return n, err
}

// Close removes the upload before releasing its quota. Removal failure retains
// the charge and is reported, so storage is never silently undercounted.
func (f *spooledModelRequest) Close() error {
	closeErr := f.File.Close()
	removeErr := os.Remove(f.Name())
	if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
		f.spool.release(f.held)
		f.held = 0
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}

// Known lengths reserve before reading; unknown/chunked bodies reserve before
// each write. Both keep their charge through placement, inference and retry.
func (s *uploadSpool) request(ctx context.Context, body io.Reader, perRequest, length int64) (*spooledModelRequest, int64, error) {
	if length > perRequest {
		return nil, 0, errModelRequestTooLarge
	}
	held := max(int64(0), length)
	if err := s.reserve(held); err != nil {
		return nil, 0, err
	}
	file, err := os.CreateTemp(s.dir, "myce-model-request-*")
	if err != nil {
		s.release(held)
		return nil, 0, err
	}
	f := &spooledModelRequest{File: file, spool: s, held: held, maxBytes: perRequest}
	// Wrap the destination to prevent io.Copy from selecting File.ReadFrom and
	// bypassing the accounting Write method promoted alongside the embedded File.
	n, err := io.Copy(struct{ io.Writer }{f}, io.LimitReader(contextReader{ctx: ctx, reader: body}, perRequest+1))
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		return nil, 0, errors.Join(err, f.Close())
	}
	return f, n, nil
}
