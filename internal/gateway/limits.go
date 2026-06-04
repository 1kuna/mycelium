package gateway

import (
	"bytes"
	"fmt"
	"io"
)

const (
	MaxGatewayRequestBodyBytes   = 16 << 20
	MaxUpstreamResponseBodyBytes = 64 << 20
	MaxStreamTelemetryBodyBytes  = 1 << 20
)

func readLimited(r io.Reader, limit int64, label string) ([]byte, error) {
	var buf bytes.Buffer
	n, err := buf.ReadFrom(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if n > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", label, limit)
	}
	return buf.Bytes(), nil
}
