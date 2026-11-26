package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"
	"os"
)

// Publication used to read a whole upload twice inside its final PATCH. On a
// network share a 2 GiB upload outlasted the client's response window, which
// cancelled it with 408 and threw the work away (#2155). Each PATCH now
// hashes the bytes it writes and records the running SHA-256 beside the
// upload, so publication only compares digests.
//
// A record at offset N names the SHA-256 of the bytes this store's writers
// put at [0, N), written only after those bytes were synced, so a crash can
// never leave a record that names bytes the disk lost. It vouches for nothing
// it did not see. A write at any other offset, a record another writer
// replaced during the write, a torn record or a large gap removes it, and
// publication then verifies the file from disk as it always did.
const uploadDigestSuffix = ".sha256"

const uploadDigestMagic = "myce upload sha256 v1\n"

// A PATCH interrupted after its bytes landed but before its record leaves a
// gap of at most one client chunk (fileUploadChunkSize, 4 MiB). Rejoining the
// record reads that gap from disk. Four chunks take about a quarter second at
// the 60 MB/s one share stream measured (#2155); a larger gap is not a lost
// chunk, so it falls back to verification at publication instead.
const uploadDigestCatchUp = 4 * fileUploadChunkSize

func encodeUploadDigest(offset int64, h hash.Hash) ([]byte, error) {
	state, err := h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return nil, err
	}
	b := binary.BigEndian.AppendUint64([]byte(uploadDigestMagic), uint64(offset))
	b = append(b, state...)
	sum := sha256.Sum256(b) // Detects a torn or truncated record.
	return append(b, sum[:]...), nil
}

func decodeUploadDigest(b []byte) (int64, hash.Hash, bool) {
	head := len(uploadDigestMagic) + 8
	if len(b) < head+sha256.Size || string(b[:len(uploadDigestMagic)]) != uploadDigestMagic {
		return 0, nil, false
	}
	body := b[:len(b)-sha256.Size]
	if sha256.Sum256(body) != [sha256.Size]byte(b[len(body):]) {
		return 0, nil, false
	}
	offset := int64(binary.BigEndian.Uint64(body[len(uploadDigestMagic):head]))
	h := sha256.New()
	if offset < 0 || h.(encoding.BinaryUnmarshaler).UnmarshalBinary(body[head:]) != nil {
		return 0, nil, false
	}
	return offset, h, true
}

// uploadDigest carries one write's view of the record.
type uploadDigest struct {
	path   string
	loaded []byte    // The record as this writer found it.
	hash   hash.Hash // Nil when the record cannot vouch for [0, offset).
}

// openUploadDigest prepares to hash a write at offset into the data file.
func openUploadDigest(ctx context.Context, data string, offset int64) uploadDigest {
	d := uploadDigest{path: data + uploadDigestSuffix}
	d.loaded, _ = os.ReadFile(d.path) // Missing or unreadable: restart or reset below.
	// Creation removes any earlier record, so a valid one past offset means
	// this write repeats recorded bytes, even at offset zero.
	at, h, ok := decodeUploadDigest(d.loaded)
	if !ok {
		at, h = 0, sha256.New()
	}
	if at > offset || offset-at > uploadDigestCatchUp {
		return d // A rewrite of recorded bytes, or too far behind to rejoin.
	}
	if at < offset {
		f, err := os.Open(data)
		if err != nil {
			return d
		}
		defer f.Close()
		n, err := io.Copy(h, contextReader{ctx, io.NewSectionReader(f, at, offset-at)})
		if err != nil || n != offset-at {
			return d
		}
	}
	d.hash = h
	return d
}

// record names the digest of [0, end) once f's bytes are durable, unless
// another writer replaced the record during this write. Otherwise it removes
// the record, so no later writer resumes from one that may not match the file.
func (d uploadDigest) record(f *os.File, end int64, perm os.FileMode) {
	if d.hash != nil && f.Sync() == nil {
		current, err := os.ReadFile(d.path)
		if (err == nil || os.IsNotExist(err)) && bytes.Equal(current, d.loaded) {
			if b, err := encodeUploadDigest(end, d.hash); err == nil && os.WriteFile(d.path, b, perm) == nil {
				return
			}
		}
	}
	_ = os.Remove(d.path)
}

type hashingWriter struct {
	w io.Writer
	h hash.Hash
}

// Hash only what reached the file: a short write must not advance the record.
func (w hashingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.h.Write(p[:n])
	return n, err
}

// uploadDigestMatches reports whether the record vouches for exactly ref.
func uploadDigestMatches(data string, ref FileRef) bool {
	b, err := os.ReadFile(data + uploadDigestSuffix)
	if err != nil {
		return false
	}
	at, h, ok := decodeUploadDigest(b)
	if !ok || at != ref.SizeBytes || hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return false
	}
	st, err := os.Lstat(data)
	return err == nil && st.Mode().IsRegular() && st.Size() == ref.SizeBytes
}
