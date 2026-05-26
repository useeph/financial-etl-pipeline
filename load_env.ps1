cd ..
@'
# Load .env file into current PowerShell session
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*?)\s*=\s*(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}
Write-Host ".env loaded" -ForegroundColor Green
'@ | Out-File -FilePath load_env.ps1 -Encoding utf8