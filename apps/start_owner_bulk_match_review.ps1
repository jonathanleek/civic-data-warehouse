param(
    [int]$Port = 8503,
    [switch]$Restart
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$Deps = Join-Path $env:TEMP "cdw-streamlit-deps"
$Log = Join-Path $env:TEMP "cdw-owner-bulk-match-review-$Port.log"

if ($Restart) {
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -like "python*" -and
            $_.CommandLine -like "*-m streamlit run*owner_bulk_match_review.py*"
        } |
        ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }

    Start-Sleep -Seconds 1
}

$listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($listener) {
    Write-Host "Owner Bulk Match Review is already listening at http://127.0.0.1:$Port"
    Write-Host "Process id: $($listener.OwningProcess)"
    return
}

if (-not (Test-Path $Deps)) {
    python -m pip install --target $Deps streamlit psycopg2-binary pandas
}

Remove-Item -LiteralPath $Log -ErrorAction SilentlyContinue

$command = @"
`$env:PYTHONPATH = '$Deps;' + `$env:PYTHONPATH
`$env:STREAMLIT_SERVER_HEADLESS = 'true'
`$env:STREAMLIT_SERVER_SHOW_EMAIL_PROMPT = 'false'
`$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = 'false'
Set-Location '$RepoRoot'
python -m streamlit run apps\owner_bulk_match_review.py --global.developmentMode=false --server.address=127.0.0.1 --server.port=$Port --server.headless=true --server.showEmailPrompt=false --browser.gatherUsageStats=false *> '$Log'
"@

Start-Process `
    -FilePath powershell.exe `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command) `
    -WindowStyle Hidden

Start-Sleep -Seconds 6

$response = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port"
Write-Host "Owner Bulk Match Review is running at http://127.0.0.1:$Port"
Write-Host "HTTP $($response.StatusCode) $($response.StatusDescription)"
Write-Host "Log: $Log"
