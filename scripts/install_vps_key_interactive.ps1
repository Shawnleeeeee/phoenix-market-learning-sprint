param(
    [string]$SecretsFile = (Join-Path (Split-Path -Parent $PSScriptRoot) "vps_secrets.local.env")
)

$ErrorActionPreference = "Stop"

function Read-SimpleEnv {
    param([string]$Path)
    $values = @{}
    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            return
        }
        $key, $value = $line.Split("=", 2)
        $values[$key.Trim()] = $value.Trim()
    }
    return $values
}

$values = Read-SimpleEnv -Path $SecretsFile
$hostName = $values["VPS_HOST"]
$user = $values["VPS_USER"]
$port = if ($values["VPS_PORT"]) { $values["VPS_PORT"] } else { "22" }
$keyPath = $values["VPS_SSH_PRIVATE_KEY_PATH"]
if (-not $keyPath) {
    $keyPath = Join-Path $HOME ".ssh\phoenix_vps_ed25519"
}
$pubPath = "$keyPath.pub"

if (-not $hostName -or -not $user) {
    throw "VPS_HOST and VPS_USER must be filled in $SecretsFile"
}
if (-not (Test-Path -LiteralPath $pubPath)) {
    throw "Public key not found: $pubPath"
}

$publicKey = (Get-Content -LiteralPath $pubPath -Raw).Trim()
$publicKeyB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($publicKey))
$remoteCommand = "umask 077; mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && key=`$(printf %s '$publicKeyB64' | base64 -d) && (grep -qxF `"`$key`" ~/.ssh/authorized_keys || printf '%s\n' `"`$key`" >> ~/.ssh/authorized_keys) && printf 'phoenix-key-installed:%s:%s\n' `"`$(hostname)`" `"`$HOME`""

Write-Host "Installing Phoenix SSH key on VPS." -ForegroundColor Cyan
Write-Host "When prompted, type the VPS password. The password will not be shown while typing." -ForegroundColor Cyan
Write-Host "User: $user" -ForegroundColor DarkGray
Write-Host "Port: $port" -ForegroundColor DarkGray
Write-Host ""

$sshArgs = @(
    "-o", "PubkeyAuthentication=no",
    "-o", "PreferredAuthentications=password",
    "-o", "StrictHostKeyChecking=accept-new",
    "-p", "$port",
    "$user@$hostName",
    $remoteCommand
)

& ssh.exe @sshArgs
$exitCode = $LASTEXITCODE
Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "If you see phoenix-key-installed above, come back to Codex and say done." -ForegroundColor Green
} else {
    Write-Host "SSH command failed with exit code $exitCode. Check username/password, then tell Codex what happened." -ForegroundColor Yellow
}
Read-Host "Press Enter to close this window"
