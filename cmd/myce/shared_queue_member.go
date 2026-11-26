package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"mycelium/internal/myce"
)

type sharedMemberListeners struct {
	address, discoveryListen, identityDir, serviceConfig, db, notifySocket string
	discovery                                                              bool
}

// serveSharedQueueMember runs `myce queue --shared-authority` on the queue's
// usual listeners. Requests reach whichever member holds the coordinator turn.
// With discovery on, the member serves the pinned joining endpoint with the
// identity in its identity directory; copy the fleet's identity there first so
// that existing invitations verify against every member.
func serveSharedQueueMember(ctx context.Context, cfg myce.SharedQueueMemberConfig, l sharedMemberListeners) error {
	member, err := myce.OpenSharedQueueMember(ctx, cfg)
	if err != nil {
		return err
	}
	defer member.Close()
	listener, err := listenWithRetry(ctx, l.address, net.Listen, myce.RealClock{}, waitForBind, log.Printf)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	defer listener.Close()
	errs := make(chan error, 2)
	var servers []*http.Server
	if l.discovery {
		dir := l.identityDir
		if dir == "" {
			dir = l.db + ".joining"
		}
		identity, err := myce.LoadJoiningIdentity(dir)
		if err != nil {
			return err
		}
		tlsListener, err := myce.ListenJoining(listener.Addr(), l.discoveryListen, l.serviceConfig, dir, func(bind string) (net.Listener, error) {
			return listenWithRetry(ctx, bind, net.Listen, myce.RealClock{}, waitForBind, log.Printf)
		}, log.Printf)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		defer tlsListener.Close()
		joining, joiningErrs, err := myce.ServeJoining(tlsListener, member, identity)
		if err != nil {
			return err
		}
		servers = append(servers, joining)
		go func() { errs <- <-joiningErrs }()
		if withdraw, err := myce.AdvertiseJoining(tlsListener.Addr(), identity.Fingerprint()); err != nil {
			fmt.Fprintln(os.Stderr, "Discovery unavailable; use a saved HTTPS joining address")
		} else {
			defer withdraw()
		}
		fmt.Printf("Mycelium joining: https://%s\n", tlsListener.Addr())
	}
	server := &http.Server{Handler: member, ReadHeaderTimeout: 5 * time.Second, ConnContext: myce.QueueConnectionContext}
	servers = append(servers, server)
	go func() { errs <- server.Serve(listener) }()
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		member.Run(loopCtx)
	}()
	fmt.Printf("Mycelium queue member: http://%s\n", listener.Addr())
	// Ready means listening. Requests return 503 until a member coordinates.
	if err = notifyQueueReady(l.notifySocket); err == nil {
		select {
		case <-ctx.Done():
		case err = <-errs:
		}
	}
	stopLoop()
	<-loopDone
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		err = errors.Join(err, s.Shutdown(shutdown))
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
