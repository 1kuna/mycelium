# Transport for an authorized remote operator. The helper still installs as the
# SAME logged-on desktop user with InteractiveToken and LUA (limited) run level.
param([ValidateSet('Install','Uninstall')][string]$Action = 'Install')
$ErrorActionPreference = 'Stop'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$desktop = (Get-CimInstance Win32_ComputerSystem).UserName
if (!$desktop -or $desktop -ne $identity) { throw 'Remote identity must match the logged-on desktop owner.' }
$taskName = 'MyceliumHostBridge-Setup'
$receipt = Join-Path $PSScriptRoot ($taskName + '-' + [guid]::NewGuid().ToString('N') + '.json')
$service = New-Object -ComObject 'Schedule.Service'
$service.Connect()
$folder = $service.GetFolder('\')
# Refuse to replace an active setup, including one still queued.
$leftover = @($folder.GetTasks(0)) | Where-Object { $_.Name -eq $taskName }
if ($leftover) {
    if ($leftover.State -eq 4 -or $leftover.State -eq 2) { throw 'Interactive setup is still running or queued; inspect its receipt before retrying.' }
    $folder.DeleteTask($taskName, 0)
}
$task = $service.NewTask(0)
$task.Principal.UserId = $identity
$task.Principal.LogonType = 3 # TASK_LOGON_INTERACTIVE_TOKEN; no stored password
$task.Principal.RunLevel = 0 # TASK_RUNLEVEL_LUA
$task.Settings.Enabled = $true
$task.Settings.DisallowStartIfOnBatteries = $false
$task.Settings.StopIfGoingOnBatteries = $false
$task.Settings.ExecutionTimeLimit = 'PT0S' # Allow finally after launcher timeout.
$actionSpec = $task.Actions.Create(0)
$actionSpec.Path = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$setup = Join-Path $PSScriptRoot 'Setup.ps1'
$actionSpec.Arguments = '-NoProfile -ExecutionPolicy Bypass -File "' + $setup + '" -Action ' + $Action + ' -Receipt "' + $receipt + '" -TaskName ' + $taskName
# The limited desktop token must be able to delete this exact task even when
# the authorized SSH launcher has an elevated token.
$taskSecurity = 'D:P(A;;GA;;;SY)(A;;GA;;;' + [Security.Principal.WindowsIdentity]::GetCurrent().User.Value + ')'
$registered = $folder.RegisterTaskDefinition($taskName, $task, 2, $identity, $null, 3, $taskSecurity)
$running = $registered.Run($null)
$deadline = [DateTime]::UtcNow.AddSeconds(40)
do {
    Start-Sleep -Milliseconds 250
    $remaining = @($folder.GetTasks(0)) | Where-Object { $_.Name -eq $taskName }
    if ([DateTime]::UtcNow -gt $deadline) { throw 'Interactive setup timed out; it will delete its task on exit. Inspect the receipt before retrying.' }
} while ($remaining)
if (!(Test-Path -LiteralPath $receipt)) { throw 'Interactive setup did not produce a receipt.' }
$result = Get-Content -LiteralPath $receipt -Raw | ConvertFrom-Json
if ($result.exit_code -ne 0 -or $result.elevated -or $result.session_id -eq 0) {
    throw 'Interactive setup failed; inspect the local receipt.'
}
$result | ConvertTo-Json -Compress
