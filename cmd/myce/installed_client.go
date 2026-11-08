package main

import (
	"flag"
	"net/url"
	"os"

	"mycelium/internal/myce"
)

type clientFlags struct{ config, joining, queue, token *string }

func installedClientFlags(f *flag.FlagSet) clientFlags {
	return clientFlags{
		f.String("config", "", "installed config (default: installed worker, then queue)"),
		f.String("joining-file", os.Getenv("MYCE_JOINING_FILE"), "private pinned joining authority"),
		f.String("queue", os.Getenv("MYCE_QUEUE"), "queue address (default: installed config)"),
		f.String("token", "", "fleet token (prefer installed config or MYCE_TOKEN)"),
	}
}

func (f clientFlags) client() (myce.Client, error) {
	token := *f.token
	if token == "" {
		token = os.Getenv("MYCE_TOKEN")
	}
	return runClient(*f.config, *f.joining, *f.queue, token)
}

// Owner operations must not turn a missing pin into ordinary system TLS trust.
// The queue's configured HTTP endpoint remains supported; joining HTTPS always
// requires its provisioned transport. No credential is included in refusals.
func (f clientFlags) ownerClient() (myce.Client, error) {
	client, err := f.client()
	if err != nil {
		return myce.Client{}, &myce.HTTPError{Status: 403, Code: "owner_authority_unavailable", Message: "cannot resolve owner authority; run on the queue host with --config <installed-queue-config>"}
	}
	endpoint, _ := url.Parse(client.URL)
	if endpoint != nil && endpoint.Scheme == "https" && client.HTTP == nil {
		return myce.Client{}, &myce.HTTPError{Status: 403, Code: "owner_authority_unpinned", Message: "HTTPS owner authority requires installed certificate trust; restore the pinned installation"}
	}
	return client, nil
}
