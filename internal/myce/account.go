package myce

import (
	"errors"
	"os"
	"os/user"
	"strconv"
)

var ErrHostIdentity = errors.New("run myce as the ordinary worker account")

// WorkerAccount comes from the OS account database, never HOME or the caller's bus.
type WorkerAccount struct {
	Name   string   `json:"name"`
	UID    uint32   `json:"uid"`
	GID    uint32   `json:"gid"`
	Groups []uint32 `json:"groups"`
	Home   string   `json:"home"`
}

func LookupWorkerAccount(name string) (WorkerAccount, error) {
	var a WorkerAccount
	var u *user.User
	var err error
	if name == "" {
		u, err = user.LookupId(strconv.Itoa(os.Geteuid()))
	} else {
		u, err = user.Lookup(name)
	}
	if err != nil {
		return a, ErrHostIdentity
	}
	uid, e1 := strconv.ParseUint(u.Uid, 10, 32)
	gid, e2 := strconv.ParseUint(u.Gid, 10, 32)
	groups, e3 := u.GroupIds()
	if e1 != nil || e2 != nil || e3 != nil {
		return a, ErrHostIdentity
	}
	a = WorkerAccount{Name: u.Username, UID: uint32(uid), GID: uint32(gid), Home: u.HomeDir}
	for _, g := range groups {
		n, e := strconv.ParseUint(g, 10, 32)
		if e != nil {
			return WorkerAccount{}, ErrHostIdentity
		}
		a.Groups = append(a.Groups, uint32(n))
	}
	return a, nil
}
