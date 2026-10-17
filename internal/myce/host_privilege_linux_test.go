package myce

import (
	"os/exec"
	"reflect"
	"testing"
)

func TestHostWorkerCredentialSelection(t *testing.T) {
	a := WorkerAccount{UID: 1001, GID: 1002, Groups: []uint32{1002, 44, renderGroupFixture}}
	cmd := exec.Command("unused")
	if e := setHostWorkerCredential(cmd, a); e != nil {
		t.Fatal(e)
	}
	c := cmd.SysProcAttr.Credential
	if c.Uid != a.UID || c.Gid != a.GID || !reflect.DeepEqual(c.Groups, a.Groups) || c.NoSetGroups {
		t.Fatal(c)
	}
}

const renderGroupFixture = 109
