package myce

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type queueEndpoints struct {
	origins   []*url.URL
	preferred atomic.Uint32
}

type queueEndpointDialBudget struct{}

// NewMultiEndpointClient uses one fleet credential and independently verifies
// each HTTPS origin with system trust. HTTP is limited to loopback proofs.
func NewMultiEndpointClient(addresses, token string) (Client, error) {
	if token == "" {
		return Client{}, errors.New("queue credential is missing")
	}
	e := &queueEndpoints{}
	for _, address := range strings.Split(addresses, ",") {
		u, err := url.Parse(strings.TrimRight(strings.TrimSpace(address), "/"))
		if err != nil || u.Hostname() == "" || u.User != nil || u.EscapedPath() != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			(u.Scheme != "https" && (u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback())) {
			return Client{}, errors.New("queue endpoints must be HTTPS origins or loopback HTTP origins")
		}
		e.origins = append(e.origins, u)
	}
	if len(e.origins) < 2 {
		return Client{}, errors.New("provide at least two queue endpoints")
	}
	transport := clientTransport.Clone()
	// Give a blackholed address the same connect budget used by joining.
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
		if budget, ok := ctx.Value(queueEndpointDialBudget{}).(time.Duration); ok {
			dialer.Timeout = max(time.Nanosecond, min(dialer.Timeout, budget))
		}
		return dialer.DialContext(ctx, network, address)
	}
	return Client{URL: e.origins[0].String(), Token: token, endpoints: e, HTTP: &http.Client{Transport: transport}}, nil
}

// Do failover before handing any response to a caller. A dial failure proves
// no request arrived. An ambiguous response can only be retried by callers
// whose operation already supports replay.
func (c Client) doQueueRequest(hc *http.Client, req *http.Request, replaySafe bool) (*http.Response, error) {
	e := c.endpoints
	if e == nil {
		return hc.Do(req)
	}
	local := *hc
	local.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	start := int(e.preferred.Load())
	for offset := 0; ; offset++ {
		index := (start + offset) % len(e.origins)
		endpoint := e.origins[index]
		ctx := req.Context()
		if end, ok := ctx.Deadline(); ok {
			// Reserve dialing time for each remaining origin. Once connected,
			// the request can use the caller's remaining deadline.
			budget := time.Until(end) / time.Duration(len(e.origins)-offset)
			ctx = context.WithValue(ctx, queueEndpointDialBudget{}, budget)
		}
		attempt := req.Clone(ctx)
		attempt.URL.Scheme, attempt.URL.Host, attempt.Host = endpoint.Scheme, endpoint.Host, endpoint.Host
		if offset > 0 && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			attempt.Body = body
		}
		res, err := local.Do(attempt)
		var network *net.OpError
		unreachable := err != nil && !IsTLSIdentityError(err) && (replaySafe || errors.As(err, &network) && network.Op == "dial")
		unavailable := res != nil && (res.StatusCode == 502 || res.StatusCode == 503 || res.StatusCode == 504)
		if !unreachable && !unavailable {
			if err == nil && res.StatusCode != http.StatusUnauthorized && res.StatusCode != http.StatusForbidden {
				e.preferred.Store(uint32(index))
			}
			return res, err
		}
		if (unreachable || replaySafe) && offset+1 < len(e.origins) {
			// A streaming upload resumes through its normal offset recovery.
			// Its consumed request body cannot be repeated here.
			if req.GetBody == nil && req.Body != nil && req.Body != http.NoBody {
				return res, err
			}
			if req.Context().Err() == nil {
				if res != nil {
					_ = res.Body.Close()
				}
				continue
			}
		}
		return res, err
	}
}
