package myce

import (
	"errors"
	"fmt"
)

// ClientErrorCause reports only classified causes, never server messages, URLs,
// credentials or expanded local paths. HTTP codes must be known protocol codes.
func ClientErrorCause(err error) string {
	var h *HTTPError
	if errors.As(err, &h) {
		code := "request_failed"
		if publicErrorCode(h.Code) || fileStoreRefusalCode(h.Code) || h.Code == "request_failed" {
			code = h.Code
		}
		return fmt.Sprintf("HTTP %d code=%s", h.Status, code)
	}
	return stagingErrorName(err)
}
