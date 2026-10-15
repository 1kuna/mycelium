# Transport for an authorized remote operator. The helper still installs as the
# SAME logged-on desktop user with InteractiveToken and LUA (limited) run level.
param([ValidateSet('Install','Uninstall')][string]$Action = 'Install')
$ErrorActionPreference = 'Stop'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$desktop = (Get-CimInstance Win32_ComputerSystem).UserName
if (!$desktop -or $desktop -ne $identity) { throw 'Remote identity must match the logged-on desktop owner.' }
$taskName = 'MyceliumHostBridge-Setup-' + [guid]::NewGuid().ToString('N')
$receipt = Join-Path $PSScriptRoot ($taskName + '.json')
$service = New-Object -ComObject 'Schedule.Service'
$service.Connect()
$folder = $service.GetFolder('\')
$task = $service.NewTask(0)
$task.Principal.UserId = $identity
$task.Principal.LogonType = 3 # TASK_LOGON_INTERACTIVE_TOKEN; no stored password
$task.Principal.RunLevel = 0 # TASK_RUNLEVEL_LUA
$task.Settings.Enabled = $true
$task.Settings.DisallowStartIfOnBatteries = $false
$task.Settings.StopIfGoingOnBatteries = $false
$task.Settings.ExecutionTimeLimit = 'PT1M'
$actionSpec = $task.Actions.Create(0)
$actionSpec.Path = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$setup = Join-Path $PSScriptRoot 'Setup.ps1'
$actionSpec.Arguments = '-NoProfile -ExecutionPolicy Bypass -File "' + $setup + '" -Action ' + $Action + ' -Receipt "' + $receipt + '"'
$registered = $null
try {
    $registered = $folder.RegisterTaskDefinition($taskName, $task, 2, $identity, $null, 3, $null)
    $running = $registered.Run($null)
    $deadline = [DateTime]::UtcNow.AddSeconds(40)
    do {
        Start-Sleep -Milliseconds 250
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Interactive setup timed out; inspect task and receipt before retrying.' }
    } while ($registered.State -eq 4 -or $registered.State -eq 2)
    if (!(Test-Path -LiteralPath $receipt)) { throw 'Interactive setup did not produce a receipt.' }
    $result = Get-Content -LiteralPath $receipt -Raw | ConvertFrom-Json
    if ($result.exit_code -ne 0 -or $result.elevated -or $result.session_id -eq 0 -or $registered.LastTaskResult -ne 0) {
        throw 'Interactive setup failed; inspect the local receipt.'
    }
    $result | ConvertTo-Json -Compress
} finally {
    if ($registered) {
        # Never stop a still-running install. Leave its task/receipt for inspection
        # on timeout; successful/completed tasks are deleted by this remote owner.
        if ($registered.State -ne 4 -and $registered.State -ne 2) {
            $folder.DeleteTask($taskName, 0)
        }
    }
}
