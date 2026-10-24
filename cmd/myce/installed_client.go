package main

import (
	"flag"
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
