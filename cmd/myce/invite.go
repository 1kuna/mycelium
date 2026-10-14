package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"mycelium/internal/myce"
)

func inviteCommand(args []string) error {
	f := flag.NewFlagSet("invite", flag.ContinueOnError)
	out := f.String("out", "", "private invitation file to create (never printed)")
	identityDir := f.String("identity-dir", "myce.db.joining", "queue joining identity directory")
	source := f.String("joining-file", "", "existing hypha's private invitation")
	address := f.String("queue", "", "saved HTTPS address (optional; discovery runs first)")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *out == "" {
		return errors.New("usage: myce invite --out FILE [--joining-file FILE | --identity-dir DIR] [--queue HTTPS_ORIGIN]")
	}
	var invitation myce.Invitation
	var err error
	if *source != "" {
		invitation, err = myce.ReadInvitation(*source)
	} else {
		// Inviting must never accidentally create a fresh queue identity.
		if _, err = os.Stat(filepath.Join(*identityDir, "identity.json")); err == nil {
			var identity myce.JoiningIdentity
			identity, err = myce.LoadJoiningIdentity(*identityDir)
			invitation = myce.Invitation{Version: 1, Fingerprint: identity.Fingerprint(), Token: os.Getenv("MYCE_TOKEN")}
		}
	}
	if err != nil {
		return errors.New("cannot read existing joining authority; supply the queue identity directory or an existing private invitation")
	}
	if *address != "" {
		invitation.Address = *address
	}
	if err = invitation.Validate(); err != nil {
		return err
	}
	path, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	// No overwrite: invitations are credentials and may belong to another mycelium.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = myce.WriteInvitation(path, invitation); err != nil {
		return err
	}
	fmt.Println("Transfer the private invitation and myce binary to the new hypha. Then run:")
	fmt.Printf("myce install worker --joining-file %s\n", shellQuote(filepath.Base(path)))
	return nil
}
func shellQuote(s string) string {
	result := "'"
	for _, r := range s {
		if r == '\'' {
			result += "'\\''"
		} else {
			result += string(r)
		}
	}
	return result + "'"
}
