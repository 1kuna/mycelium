package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func inviteCommand(args []string) error {
	f := flag.NewFlagSet("invite", flag.ContinueOnError)
	private := f.Bool("private-file", false, "export a private credential file for the first worker or installs without a signed release; requires --out")
	manifest := f.String("release-manifest", "", "override queue-approved signed release JSON")
	key := f.String("release-key", "", "provisioned public PEM for a manifest override")
	origin := f.String("release-url", "", "trusted publisher HTTPS directory override")
	ca := f.String("release-ca", "", "provisioned publisher CA PEM for a manifest override")
	ttl := f.Duration("expires", 15*time.Minute, "one-time invite lifetime, at most one hour")
	out := f.String("out", "", "optionally save the paste-ready command as a private file")
	identityDir := f.String("identity-dir", "myce.db.joining", "existing queue joining identity directory")
	source := f.String("joining-file", os.Getenv("MYCE_JOINING_FILE"), "existing private invitation (default: installed hypha)")
	address := f.String("queue", os.Getenv("MYCE_QUEUE"), "saved HTTPS queue origin")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *private && *out == "" {
		return errors.New("usage: myce invite [--out FILE] [--joining-file FILE | --identity-dir DIR] [--queue HTTPS_ORIGIN]")
	}
	explicitIdentity := false
	f.Visit(func(flag *flag.Flag) {
		if flag.Name == "identity-dir" {
			explicitIdentity = true
		}
	})
	var invitation myce.Invitation
	var err error
	if *source != "" && !explicitIdentity {
		invitation, err = myce.ReadInvitation(*source)
	} else {
		// An installed worker's private joining file is already the authority. Never
		// read a fleet EnvironmentFile or create an identity merely to issue an invite.
		home, e := os.UserHomeDir()
		if e != nil {
			return e
		}
		saved, e := myce.ReadServiceConfig(filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json"))
		if !explicitIdentity && e == nil && saved.JoiningFile != "" {
			invitation, err = myce.ReadInvitation(saved.JoiningFile)
		} else {
			if _, err = os.Stat(filepath.Join(*identityDir, "identity.json")); err == nil {
				var identity myce.JoiningIdentity
				identity, err = myce.LoadJoiningIdentity(*identityDir)
				invitation = myce.Invitation{Version: 1, Fingerprint: identity.Fingerprint(), Token: os.Getenv("MYCE_TOKEN")}
			}
		}
	}
	if err != nil {
		return errors.New("cannot read existing joining authority; use an installed hypha, queue identity directory or private invitation")
	}
	if *address != "" {
		invitation.Address = *address
	}
	if err = invitation.Validate(); err != nil {
		return err
	}
	if *private {
		if err = writeInviteOutput(*out, nil, &invitation); err != nil {
			return err
		}
		fmt.Println("Transfer the private invitation and myce binary to the new hypha. Then run:")
		fmt.Printf("myce install worker --joining-file %s\n", shellQuote(filepath.Base(*out)))
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := invitation.Client(invitation.Address)
	if err != nil {
		return err
	}
	defer client.HTTP.CloseIdleConnections()
	var distribution myce.EnrollmentRelease
	if *manifest != "" || *key != "" || *origin != "" || *ca != "" {
		data, e := os.ReadFile(*manifest)
		if e != nil {
			return e
		}
		pub, e := myce.ReadReleasePublicKey(*key)
		if e != nil {
			return e
		}
		distribution.Manifest, e = myce.VerifyRelease(data, pub)
		if e != nil {
			return e
		}
		distribution.Origin = *origin
		if *ca != "" {
			cert, e := os.ReadFile(*ca)
			if e != nil {
				return e
			}
			distribution.CACert = string(cert)
		}
	} else if err = client.Call(ctx, "GET", "/api/invites/release", nil, &distribution); err != nil {
		return err
	}
	if err = distribution.Validate(); err != nil {
		return err
	}
	if _, err = myce.BootstrapCommand(distribution.Manifest, distribution.Origin, invitation.Address, invitation.Fingerprint, strings.Repeat("a", 64), distribution.CACert); err != nil {
		return err
	}
	var ticket myce.EnrollmentTicket
	if err = client.Call(ctx, "POST", "/api/invites", myce.InviteRequest{TTL: *ttl}, &ticket); err != nil {
		return err
	}
	command, err := myce.BootstrapCommand(distribution.Manifest, distribution.Origin, invitation.Address, invitation.Fingerprint, ticket.Ticket, distribution.CACert)
	if err != nil {
		return err
	}
	if *out != "" {
		if err = writeInviteOutput(*out, []byte(command+"\n"), nil); err != nil {
			return err
		}
	}
	fmt.Println(command)
	return nil
}
func writeInviteOutput(path string, data []byte, invitation *myce.Invitation) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if invitation != nil {
		if err = file.Close(); err != nil {
			return err
		}
		return myce.WriteInvitation(path, *invitation)
	}
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
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
