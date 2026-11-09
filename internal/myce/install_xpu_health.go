package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
)

const xpuHealthRulePath = "/etc/udev/rules.d/70-mycelium-xpu-health.rules"

func xpuHealthRules(gid int) string {
	// IGSC opens MEI O_RDWR even for read-only health queries. PMT is only
	// read. The primary group is already effective in the running user manager.
	return fmt.Sprintf(`# Mycelium XPU health access
SUBSYSTEM=="mei", KERNEL=="mei[0-9]*", DRIVERS=="mei_gsc", GROUP="%d", MODE="0660"
SUBSYSTEM=="intel_pmt", KERNEL=="telem[0-9]*", DRIVERS=="xe", RUN+="/bin/chgrp %d /sys%%p/telem", RUN+="/bin/chmod 0440 /sys%%p/telem"
`, gid, gid)
}

// The fixed script receives only a numeric group and generated rule text. It
// grants no sudoers entry and never executes a user-writable program as root.
const xpuHealthAccessScript = `set -eu
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
gid=$1
rules=$2
case "$gid" in ''|*[!0-9]*) exit 1;; esac
directory=/etc/udev/rules.d
destination=$directory/70-mycelium-xpu-health.rules
if [ -L "$destination" ] || { [ -e "$destination" ] && [ ! -f "$destination" ]; }; then exit 1; fi
if [ -f "$destination" ]; then
  [ "$(head -n 1 "$destination")" = '# Mycelium XPU health access' ] || exit 1
fi
mkdir -p "$directory"
pending=$(mktemp "$directory/.mycelium-xpu-health.XXXXXX")
trap 'rm -f "$pending"' EXIT
printf '%s' "$rules" > "$pending"
chmod 0644 "$pending"
chown 0:0 "$pending"
mv -f "$pending" "$destination"
udevadm control --reload-rules
# Apply only the GPU MEI nodes, preserving unrelated management-engine nodes.
for node in /sys/class/mei/mei*; do
  [ -e "$node" ] || continue
  [ "$(basename "$(readlink -f "$node/device/driver")")" = mei_gsc ] || continue
  device=/dev/$(basename "$node")
  chgrp "$gid" "$device"
  chmod 0660 "$device"
done
# PMT attributes are sysfs files, not /dev nodes. Restrict this to Xe ancestry.
for node in /sys/class/intel_pmt/telem*; do
  [ -f "$node/telem" ] || continue
  parent=$(readlink -f "$node")
  while [ "$parent" != / ]; do
    if [ "$(basename "$(readlink -f "$parent/driver")")" = xe ]; then
      chgrp "$gid" "$node/telem"
      chmod 0440 "$node/telem"
      break
    fi
    parent=$(dirname "$parent")
  done
done
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

// EnsureXPUHealthAccess is an idempotent installer prerequisite, also used on
// installed service startup so a binary-only release rollout migrates access
// before accepting work. It does not run a GPU probe or restart any service.
func EnsureXPUHealthAccess(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	return ensureXPUHealthAccess(ctx, c, command, os.ReadFile)
}

func ensureXPUHealthAccess(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error), read func(string) ([]byte, error)) error {
	if !needsXPUHealthAccess(c) {
		return nil
	}
	rules := xpuHealthRules(os.Getgid())
	if data, err := read(xpuHealthRulePath); err == nil && string(data) == rules {
		return nil
	}
	if command == nil {
		return errors.New("xpu_health_access_unavailable")
	}
	if _, err := command(ctx, "sudo", "-n", "--", "/bin/sh", "-c", xpuHealthAccessScript, "myce-xpu-health", strconv.Itoa(os.Getgid()), rules); err != nil {
		return errors.New("xpu_health_access_unavailable: installer could not provision GPU MEI/PMT access")
	}
	return nil
}
