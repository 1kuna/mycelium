package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// DefaultUploadSpoolBytes leaves physical-disk admission as the binding limit.
// Saved configs must remain positive for older installers. The largest integer
// exactly interoperable through JSON float64 readers satisfies that schema
// without imposing a workload-sized ceiling; measured free disk and unwritten commitments always bind first.
// New peers also accept -1 as an explicit automatic-mode alias.
const DefaultUploadSpoolBytes int64 = 1<<53 - 1

var errUploadSpoolFull = errors.New("upload spool capacity exhausted")

type uploadSpool struct {
	mu                   sync.Mutex
	dir                  string
	lock                 *os.File
	defaultDir           string
	limit, used, written int64
	free                 func(string) (int64, error) // injected local filesystem capacity
}

// ConfigureUploadSpool configures queue-local storage before serving requests.
// An empty directory uses a private directory beside the queue DB. Use -1 for measured disk capacity or a positive explicit byte budget.
func (q *Queue) ConfigureUploadSpool(dir string, budget int64) error {
	if budget == 0 || budget < -1 {
		return errors.New("upload-spool-bytes must be positive or -1 for automatic capacity")
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

// Statfs and control-file metadata are local observations outside q.mu. Only
// unwritten reservations are deducted from free disk; written bytes already
// reduced it. One current control working set remains free for WAL/DB growth.
func (s *uploadSpool) reserve(n int64) error {
	// Serialize the local measurement with written-accounting publication.
	// In-flight writes remain unwritten commitments until this lock releases,
	// so a stale free-space sample cannot be combined with newer write credit.
	// This mutex is independent of q.mu; filesystem I/O never holds q.mu.
	s.mu.Lock()
	defer s.mu.Unlock()
	probe := s.free
	if probe == nil {
		probe = diskFree
	}
	dir := s.dir
	if dir == "" {
		dir = os.TempDir() // Same filesystem selected by os.CreateTemp below.
	}
	free, err := probe(dir)
	if err != nil {
		return fmt.Errorf("%w: disk measurement unavailable; fitting bytes=0", errUploadSpoolFull)
	}
	var control int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(strings.TrimSuffix(s.defaultDir, ".uploads") + suffix)
		if err == nil {
			control += info.Size()
		}
	}
	available := max(0, free-control-(s.used-s.written))
	if s.limit > 0 {
		available = min(available, max(0, s.limit-s.used))
	}
	if n < 0 || n > available {
		return fmt.Errorf("%w: required bytes=%d; fitting bytes=%d", errUploadSpoolFull, n, available)
	}
	s.used += n
	return nil
}
func (s *uploadSpool) release(n int64) { s.mu.Lock(); s.used -= n; s.mu.Unlock() }

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
	f.spool.mu.Lock()
	f.spool.written += int64(n)
	f.spool.mu.Unlock()
	return n, err
}

// Close removes the upload before releasing its quota. Removal failure retains
// the charge and is reported, so storage is never silently undercounted.
func (f *spooledModelRequest) Close() error {
	closeErr := f.File.Close()
	removeErr := os.Remove(f.Name())
	if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
		f.spool.mu.Lock()
		f.spool.used -= f.held
		f.spool.written -= f.written
		f.spool.mu.Unlock()
		f.held = 0
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}

// Known lengths reserve before reading; unknown/chunked bodies reserve before
// each write. Both keep their charge through placement, inference and retry.
func (s *uploadSpool) request(ctx context.Context, body io.Reader, perRequest, length int64) (*spooledModelRequest, int64, error) {
	if perRequest <= 0 {
		perRequest = math.MaxInt64 - 1
	} // io.LimitReader needs one sentinel byte
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
