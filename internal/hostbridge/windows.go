//go:build windows

package hostbridge

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

var (
	user32      = windows.NewLazySystemDLL("user32.dll")
	lastInput   = user32.NewProc("GetLastInputInfo")
	kernel32    = windows.NewLazySystemDLL("kernel32.dll")
	powerStatus = kernel32.NewProc("GetSystemPowerStatus")
	tickCount   = kernel32.NewProc("GetTickCount64")
)

func sessionCoverage() (uint32, bool) {
	var self uint32
	if windows.ProcessIdToSessionId(windows.GetCurrentProcessId(), &self) != nil {
		return 0, false
	}
	var list *windows.WTS_SESSION_INFO
	var count uint32
	if windows.WTSEnumerateSessions(0, 0, 1, &list, &count) != nil {
		return self, false
	}
	defer windows.WTSFreeMemory(uintptr(unsafe.Pointer(list)))
	if list == nil || count == 0 || count > 4096 {
		return self, false
	}
	sessions := make([]Session, 0, count)
	for _, s := range unsafe.Slice(list, int(count)) {
		sessions = append(sessions, Session{ID: s.SessionID, State: s.State})
	}
	return self, Covered(self, windows.WTSGetActiveConsoleSessionId(), sessions)
}

// WindowsSource queries only aggregate last-input age, connection state and
// system power. No WTS user names or window-station names are inspected.
func WindowsSource() func(context.Context) Signals {
	var mu sync.Mutex
	var continuity Continuity
	return func(ctx context.Context) Signals {
		mu.Lock()
		defer mu.Unlock()
		s := Unknown()
		var power struct {
			AC, Battery, Percent, Saver byte
			Life, FullLife              uint32
		}
		if ok, _, _ := powerStatus.Call(uintptr(unsafe.Pointer(&power))); ok != 0 {
			s.Power, s.Laptop = Power(power.AC, power.Battery, power.Saver)
		}
		// Use the driver-installed executable, never PATH/current-directory lookup.
		probe, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		system, err := windows.GetSystemDirectory()
		if err == nil {
			cmd := exec.CommandContext(probe, filepath.Join(system, "nvidia-smi.exe"), "--query-gpu=temperature.gpu,temperature.gpu.tlimit", "--format=csv,noheader")
			cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
			if raw, err := cmd.Output(); err == nil && len(raw) <= 4096 {
				s.SetGPUHeat(NVIDIAHeat(string(raw)))
			}
		}
		cancel()
		self, covered := sessionCoverage()
		var input struct{ Size, Tick uint32 }
		input.Size = uint32(unsafe.Sizeof(input))
		ok, _, _ := lastInput.Call(uintptr(unsafe.Pointer(&input)))
		now, _, _ := tickCount.Call()
		after, stillCovered := sessionCoverage()
		if ctx.Err() != nil || !covered || !stillCovered || after != self || ok == 0 {
			continuity = Continuity{}
			return s
		}
		s.IdleSeconds, s.IdleKnown = continuity.Observe(uint64(now), input.Tick)
		return s
	}
}

func InstallRoot() (string, error) {
	root := os.Getenv("LOCALAPPDATA")
	if root == "" || !filepath.IsAbs(root) {
		return "", errors.New("LOCALAPPDATA unavailable")
	}
	return filepath.Join(root, "MyceliumHostBridge"), nil
}

const runKey = `Software\Microsoft\Windows\CurrentVersion\Run`
const runName = "MyceliumHostBridge"

func WindowsInstallHooks() InstallHooks {
	return InstallHooks{
		Secure: func(path string) error {
			user, err := windows.GetCurrentProcessToken().GetTokenUser()
			if err != nil {
				return err
			}
			sd, err := windows.SecurityDescriptorFromString("D:P(A;OICI;FA;;;" + user.User.Sid.String() + ")")
			if err != nil {
				return err
			}
			acl, _, err := sd.DACL()
			if err != nil {
				return err
			}
			return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil)
		},
		Registration: func() (string, error) {
			k, err := registry.OpenKey(registry.CURRENT_USER, runKey, registry.QUERY_VALUE)
			if errors.Is(err, registry.ErrNotExist) {
				return "", nil
			}
			if err != nil {
				return "", err
			}
			defer k.Close()
			v, _, err := k.GetStringValue(runName)
			if errors.Is(err, registry.ErrNotExist) {
				return "", nil
			}
			return v, err
		},
		Register: func(value string) error {
			k, _, err := registry.CreateKey(registry.CURRENT_USER, runKey, registry.SET_VALUE)
			if err != nil {
				return err
			}
			defer k.Close()
			return k.SetStringValue(runName, value)
		},
		Unregister: func() error {
			k, err := registry.OpenKey(registry.CURRENT_USER, runKey, registry.SET_VALUE)
			if err != nil {
				return err
			}
			defer k.Close()
			return k.DeleteValue(runName)
		},
	}
}

// InteractiveInstallAllowed verifies the installer belongs to the covered console.
func InteractiveInstallAllowed() bool { _, covered := sessionCoverage(); return covered }
