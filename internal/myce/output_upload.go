package myce

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strconv"
)

// Reuse tus creation-with-upload, already supported by previous queues. A new
// one-chunk file needs just POST: tus returns its offset only after the finish
// callback has verified the digest and committed publication. Our older wrapper
// returns no offset for an existing blob/session; that case uses ordinary tus
// reconciliation. Never infer accepted bytes from a local body/socket write.
func (c Client) uploadOutputFile(ctx context.Context, path string, ref FileRef) (result error) {
	if ref.SizeBytes > fileUploadChunkSize {
		return c.UploadFile(ctx, path, ref)
	}
	if err := ref.validate(); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	source := &uploadSource{ctx: ctx, reader: f, digest: sha256.New(), expected: ref.SizeBytes}
	if err = source.start(0); err != nil {
		return err
	}
	c.uploadTiming = &uploadTiming{}
	if c.publication != nil {
		c.uploadTiming = &c.publication.timing
	}
	body := &uploadBody{reader: io.LimitReader(source, ref.SizeBytes)}
	res, err := c.fileRequest(ctx, "POST", "/api/files/uploads/", body, map[string]string{
		"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10),
		"Upload-Metadata": "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256)),
		"Content-Type":    "application/offset+octet-stream",
	}, ref.SizeBytes)
	body.Close() // Fence net/http's asynchronous reader before verification/seek.
	if err != nil {
		return err // Lost replies reconcile through tus on the owner's next pass.
	}
	offset, offsetErr := strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
	if err = fileResponse(res, 201, c.clock().Now()); err != nil {
		return err
	}
	if err = source.verify(ref); err != nil {
		return err
	}
	if offsetErr == nil {
		if offset < 0 || offset > ref.SizeBytes {
			return errors.New("invalid upload acknowledgement")
		}
		if offset == ref.SizeBytes {
			c.publication.confirm(ref)
			return nil
		}
	}
	return c.uploadFileReader(ctx, f, ref, "")
}
