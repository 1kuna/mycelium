package myce

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const xpuHealthRulePath = "/etc/udev/rules.d/70-mycelium-xpu-health.rules"
const xpuHealthAccessPath = "/var/lib/mycelium/xpu-health-access/access.sh"

func xpuHealthRules(gid int) string {
	// Capture the pre-grant permissions in the root helper, including for nodes
	// recreated after install. Do not assign GROUP/MODE before that snapshot.
	return fmt.Sprintf(`# Mycelium XPU health access
SUBSYSTEM=="mei", KERNEL=="mei[0-9]*", DRIVERS=="mei_gsc", RUN+="%s apply %d /dev/%%k"
SUBSYSTEM=="intel_pmt", KERNEL=="telem[0-9]*", DRIVERS=="xe", RUN+="%s apply %d /sys%%p/telem"
`, xpuHealthAccessPath, gid, xpuHealthAccessPath, gid)
}

// Root-owned code and receipts; neither credentials nor a user-writable program
// is passed to root. The stable lock serializes install, udev and uninstall.
// A failed install retains its ownership/permission journal for repair or removal.
const xpuHealthAccessScript = `#!/bin/sh
set -eu
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
umask 077
action=$1
gid=$2
case "$gid" in ''|*[!0-9]*) exit 1;; esac
state=/var/lib/mycelium/xpu-health-access
destination=/etc/udev/rules.d/70-mycelium-xpu-health.rules
exec 9>/run/lock/mycelium-xpu-health.lock
flock -x 9
pending=
cleanup_pending() {
  if [ -n "$pending" ]; then
    if [ -d "$pending" ]; then
      rm -f "$pending/format" "$pending/gid"
      rmdir "$pending/owners" "$pending/nodes" "$pending"
    else rm -f "$pending"; fi
  fi
}
trap cleanup_pending EXIT
publish() {
  pending=$(mktemp "$(dirname "$1")/.mycelium-xpu-health.XXXXXX")
  printf '%s' "$3" > "$pending"
  chmod "$2" "$pending"
  chown 0:0 "$pending"
  mv -f "$pending" "$1"
  pending=
}
node_mode() {
  case "$1" in
    /dev/mei[0-9]*) printf 660;;
    /sys/*/telem) printf 440;;
    *) return 1;;
  esac
}
apply_node() {
  node=$1
  [ -e "$node" ] || return 0
  mode=$(node_mode "$node")
  key=$(printf '%s' "$node" | sha256sum)
  key=${key%% *}
  receipt=$state/nodes/$key
  object=$(stat -Lc '%d:%i' "$node")
  previous_object=
  if [ -f "$receipt" ]; then previous_object=$(sed -n '2p' "$receipt"); fi
  if [ "$previous_object" != "$object" ]; then
    baseline=$(stat -Lc '%g %a' "$node")
    publish "$receipt" 0600 "$(printf '%s\n%s\n%s\n' "$node" "$object" "$baseline")"
  fi
  # IGSC/HECI uses O_RDWR for the health exchange. PMT is read-only.
  chgrp "$gid" "$node"
  chmod "$mode" "$node"
}
[ ! -L "$state" ] || exit 1
if [ "$action" = apply ]; then
  [ "$(cat "$state/format")" = mycelium-xpu-health-v1 ]
  [ "$(cat "$state/gid")" = "$gid" ]
  apply_node "$3"
  exit 0
fi
owner=$3
rules=$(printf '%s' "$4")
case "$owner" in ''|*[!0-9a-f]*) exit 1;; esac
[ "${#owner}" = 64 ]
if [ -L "$destination" ] || { [ -e "$destination" ] && [ ! -f "$destination" ]; }; then exit 1; fi
# Never remove or overwrite a rule changed by another owner/administrator.
if [ -f "$destination" ]; then [ "$(cat "$destination")" = "$rules" ]; fi
if [ "$action" = install ] && [ ! -d "$state" ]; then
  [ ! -e "$destination" ] # Do not adopt grants whose baseline journal is lost.
  mkdir -p "$(dirname "$state")"
  pending=$(mktemp -d "$state.XXXXXX")
  mkdir "$pending/owners" "$pending/nodes"
  printf '%s' mycelium-xpu-health-v1 > "$pending/format"
  printf '%s' "$gid" > "$pending/gid"
  mv "$pending" "$state"
  pending=
fi
if [ -d "$state" ]; then
  [ "$(cat "$state/format")" = mycelium-xpu-health-v1 ]
  [ "$(cat "$state/gid")" = "$gid" ]
fi
case "$action" in
install)
  mkdir -p "$state/owners" "$state/nodes" /etc/udev/rules.d
  chmod 0700 "$state" "$state/owners" "$state/nodes"
  publish "$state/format" 0600 mycelium-xpu-health-v1
  publish "$state/gid" 0600 "$gid"
  publish "$state/owners/$owner" 0600 "$owner"
  publish "$state/access.sh" 0700 "$5"
  publish "$destination" 0644 "$4"
  # Matching rule bytes alone never prove these following operations completed.
  udevadm control --reload-rules
  for node in /sys/class/mei/mei*; do
    [ -e "$node" ] || continue
    [ "$(basename "$(readlink -f "$node/device/driver")")" = mei_gsc ] || continue
    apply_node "/dev/$(basename "$node")"
  done
  for node in /sys/class/intel_pmt/telem*; do
    [ -f "$node/telem" ] || continue
    parent=$(readlink -f "$node")
    while [ "$parent" != / ]; do
      if [ "$(basename "$(readlink -f "$parent/driver")")" = xe ]; then
        apply_node "$node/telem"
        break
      fi
      parent=$(dirname "$parent")
    done
  done
  ;;
remove)
  if [ ! -d "$state" ]; then [ ! -e "$destination" ]; exit; fi
  [ -f "$state/owners/$owner" ] || exit 0
  for other in "$state"/owners/*; do
    if [ "$other" != "$state/owners/$owner" ]; then
      rm "$state/owners/$owner"
      exit 0
    fi
  done
  if [ -f "$destination" ]; then rm "$destination"; fi
  udevadm control --reload-rules
  for receipt in "$state"/nodes/*; do
    [ -f "$receipt" ] || continue
    node=$(sed -n '1p' "$receipt")
    object=$(sed -n '2p' "$receipt")
    baseline=$(sed -n '3p' "$receipt")
    mode=$(node_mode "$node")
    if [ -e "$node" ] && [ "$(stat -Lc '%d:%i' "$node")" = "$object" ]; then
      current=$(stat -Lc '%g %a' "$node")
      # Restore only our still-current grant, preserving subsequent admin edits.
      original_gid=${baseline%% *}
      original_mode=${baseline#* }
      if { [ "$current" = "$gid $mode" ] || [ "$current" = "$gid $original_mode" ] || [ "$current" = "$original_gid $mode" ]; }; then
        chgrp "${baseline%% *}" "$node"
        chmod "${baseline#* }" "$node"
      fi
    fi
    rm "$receipt"
  done
  rm "$state/owners/$owner" "$state/access.sh" "$state/gid" "$state/format"
  rmdir "$state/owners" "$state/nodes" "$state"
  ;;
*) exit 1;;
esac
`

func needsXPUHealthAccess(c ServiceConfig) bool {
	if c.Role != "worker" || c.Detection.OS != "linux" {
		return false
	}
	if c.Detection.Platform == "xpu" {
		return true
	}
	for _, d := range c.MachineDevices {
		if d.Detection.Platform == "xpu" {
			return true
		}
	}
	return false
}

func xpuHealthOwner(c ServiceConfig) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strconv.Itoa(os.Getuid())+"\x00"+filepath.Clean(c.Config))))
}

// EnsureXPUHealthAccess also migrates binary-only rollouts before activation.
// Always reapply: a published rule does not establish successful provisioning.
func EnsureXPUHealthAccess(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	return xpuHealthAccess(ctx, c, "install", command)
}

func xpuHealthAccess(ctx context.Context, c ServiceConfig, action string, command func(context.Context, string, ...string) ([]byte, error)) error {
	if !needsXPUHealthAccess(c) {
		return nil
	}
	code := "xpu_health_access_unavailable"
	if action == "remove" {
		code = "xpu_health_access_cleanup_failed"
	}
	if command == nil {
		return errors.New(code)
	}
	_, err := command(ctx, "sudo", "-n", "--", "/bin/sh", "-c", xpuHealthAccessScript, "myce-xpu-health", action, strconv.Itoa(os.Getgid()), xpuHealthOwner(c), xpuHealthRules(os.Getgid()), xpuHealthAccessScript)
	if err != nil {
		return errors.New(code)
	}
	return nil
}
