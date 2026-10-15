Mycelium Windows host helper

Keep all files in this folder. From your normal Windows desktop:
  Double-click Install.cmd to install and start.
  Double-click Uninstall.cmd to stop and remove the installed helper.

No administrator prompt, service, driver or firewall change is needed.
If setup fails, the window stays open. Do not use Run as administrator.
Installation refuses an existing install. To replace it, uninstall first.

PowerShell alternative, from this folder:
  powershell -NoProfile -ExecutionPolicy Bypass -File .\Setup.ps1 -Action Install
Replace Install with Uninstall to remove it.

The helper starts again at your next interactive logon. It reads aggregate
idle time, power and NVIDIA GPU temperature; never input contents.
GPU temperature alone does not establish CPU/host thermal coverage.
Daily-use jobs wait when required heat or session coverage is unknown.

For an authorized remote operator signed in as the same desktop user:
  powershell -NoProfile -ExecutionPolicy Bypass -File .\Remote-Setup.ps1 -Action Install
This launches Setup.ps1 in the existing interactive session with limited
rights, without a password. The one-shot task is deleted after completion.
A small aggregate receipt remains beside these files for verification.
A timed-out running task is retained for inspection, never blindly stopped.

Downloads files are intentionally kept for reinstall/uninstall. Uninstall
removes the installed directory, credential, startup entry and listener.
It preserves unknown files or a modified startup entry with an error.
