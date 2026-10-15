param(
    [ValidateSet('Install','Uninstall')][string]$Action = 'Install',
    [string]$Receipt
)
$ErrorActionPreference = 'Stop'
$code = 1
try {
    $session = (Get-Process -Id $PID).SessionId
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $elevated = ([Security.Principal.WindowsPrincipal]$identity).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if ($session -eq 0 -or $elevated) { throw 'Open from your regular Windows desktop, without Run as administrator.' }
    $download = Join-Path $PSScriptRoot 'myce-host-bridge.exe'
    $p = Start-Process -FilePath $download -ArgumentList $Action.ToLowerInvariant() -Wait -PassThru
    if ($p.ExitCode -ne 0) { throw "Helper $Action failed (exit $($p.ExitCode)); existing files are preserved." }
    if ($Action -eq 'Install') {
        $helper = Join-Path $env:LOCALAPPDATA 'MyceliumHostBridge\myce-host-bridge.exe'
        $server = Start-Process -FilePath $helper -ArgumentList 'serve' -PassThru
        Start-Sleep -Seconds 2
        if ($server.HasExited) { throw 'Installed, but helper did not stay running. Use Uninstall, then inspect the port/session.' }
    }
    $code = 0
    Write-Host "$Action succeeded."
} catch {
    Write-Error $_ -ErrorAction Continue
} finally {
    if ($Receipt) {
        # Aggregate evidence only; never copy config/key into a receipt.
        @{ exit_code=$code; session_id=$session; elevated=$elevated; action=$Action } |
            ConvertTo-Json -Compress | Set-Content -LiteralPath $Receipt -Encoding UTF8
    }
}
exit $code
