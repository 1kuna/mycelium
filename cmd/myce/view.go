package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"mycelium/internal/myce"
	"mycelium/internal/view"
)

// viewCommand serves a read-only live picture of the queue in a browser. It
// is an ordinary status client: one shared GET /api/status read at a time,
// only while a page is open, paced by how fast the queue answers.
func viewCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	f := flag.NewFlagSet("view", flag.ContinueOnError)
	f.SetOutput(errOut)
	clientFlags := installedClientFlags(f)
	listen := f.String("listen", "127.0.0.1:0", "local address for the page (loopback unless you choose otherwise)")
	anonymize := f.Bool("anonymize", false, "replace machine, project and job names with stable generic labels (for recordings)")
	f.Lookup("token").DefValue = ""
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &exitError{2, err}
	}
	if f.NArg() != 0 {
		return &exitError{2, errors.New("usage: myce view [--listen ADDR] [--anonymize] [--queue URL]")}
	}
	client, err := clientFlags.client()
	if err != nil {
		return err
	}
	logger := log.New(errOut, "", 0)
	poller := view.NewPoller(view.UpstreamReader{URL: client.URL, Token: client.Token, HTTP: client.HTTP, UserAgent: "myce-view/" + myce.BuildVersion}, *anonymize, logger)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Event streams use the command's context, so Ctrl-C ends them too.
	srv := &http.Server{Handler: view.Handler(poller, ln.Addr().String()), ReadHeaderTimeout: 10 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { _ = poller.Run(ctx) }()
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	note := ""
	if *anonymize {
		note = ", anonymized"
	}
	fmt.Fprintf(out, "Mycelium view: http://%s (read-only%s; Ctrl-C to stop)\n", ln.Addr(), note)
	select {
	case <-ctx.Done():
	case err = <-served:
		return err
	}
	shutdown, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()
	_ = srv.Shutdown(shutdown)
	return srv.Close()
}
