package myce

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"
)

// Retry only the refused control message, never the surrounding publication or
// manager action. Fresh instructions are evidence to retry, not replacement
// authority: the original operation, release, session and helper revision remain
// frozen. No local proof, custody or maintenance fence is bypassed.
func fleetRetryControl(ctx context.Context, client Client, i FleetUpdateInstruction, wait func(context.Context, time.Duration) error, call func() error) error {
	err := call()
	if !fleetStaleControl(err) {
		return err
	}
	stale := err
	if wait == nil {
		wait = enrollmentPause
	}
	query := url.Values{"operation": {i.Operation}, "revision": {strconv.Itoa(i.Revision)}, "host": {i.Host}, "machine": {i.Machine}, "session": {i.Session}}
	for {
		var fresh FleetUpdateInstruction
		err = client.Call(ctx, http.MethodGet, "/api/fleet/instruction?"+query.Encode(), nil, &fresh)
		if err != nil {
			var rejection *HTTPError
			if !errors.As(err, &rejection) || rejection.Status != http.StatusTooManyRequests {
				return fleetControlFailure(err, "fleet_authorization_refused")
			}
		} else if fresh.Operation != "" {
			revision := fresh.Revision
			fresh.Revision = i.Revision
			if revision < i.Revision || !reflect.DeepEqual(fresh, i) {
				return stale
			}
			err = call()
			if !fleetStaleControl(err) {
				return err
			}
		}
		// At most two refetches per proof-freshness window while the existing
		// runtime observer refreshes proof. This wait grants no authority itself.
		if wait(ctx, fleetRuntimeFreshFor/2) != nil {
			return stale
		}
	}
}
