<# Reproduce the immutable TopVenues profile on native Windows PowerShell. #>
param(
    [string]$Profile = "security-20-v4",
    [string]$PythonCommand = "",
    [switch]$SkipInstall
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
if ($Profile -notin @("security-20", "security-20-v2", "security-20-v3", "security-20-v4")) {
    throw "Unknown profile. Choose security-20, security-20-v2, security-20-v3, or security-20-v4."
}

# Prefer the Windows launcher pinned to the supported minor version.  On many
# Windows hosts `python` still resolves to an older, system-wide installation.
$python = $null
$pythonArgs = @()
if ($PythonCommand) {
    $python = Get-Command $PythonCommand -ErrorAction SilentlyContinue
    if ($null -ne $python) {
        & $python.Source -c "import sys; raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 15) else 1)"
        if ($LASTEXITCODE -ne 0) { $python = $null }
    }
} else {
    $python = Get-Command py -ErrorAction SilentlyContinue
}
if ($null -ne $python -and -not $PythonCommand) {
    foreach ($minor in @("3.14", "3.13", "3.12", "3.11")) {
        & $python.Source "-$minor" -c "import sys; raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 15) else 1)"
        if ($LASTEXITCODE -eq 0) {
            $pythonArgs = @("-$minor")
            break
        }
    }
    if ($pythonArgs.Count -eq 0) { $python = $null }
}
if ($null -eq $python -and -not $PythonCommand) {
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $python) {
        & $python.Source -c "import sys; raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 15) else 1)"
        if ($LASTEXITCODE -ne 0) { $python = $null }
    }
}
if ($null -eq $python) {
    throw "Python 3.11-3.14 is required. Install a supported Python from python.org, then rerun."
}

if (-not (Test-Path ".venv")) {
    if ($SkipInstall) { throw "-SkipInstall requires an existing .venv." }
    & $python.Source @pythonArgs -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Could not create the Python virtual environment." }
}
$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    throw "The existing .venv is not native Windows. Remove only .venv and rerun; do not remove corpus data."
}
& $venvPython -c "import sys; raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 15) else 1)"
if ($LASTEXITCODE -ne 0) {
    throw "The existing .venv uses Python older than 3.11. Remove only .venv and rerun; do not remove corpus data."
}
if (-not $SkipInstall) {
    & $venvPython -m pip install --disable-pip-version-check --prefer-binary --require-hashes -r requirements-frozen.txt
    if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }
}
& $venvPython scripts\verify_profile_snapshot.py --profile $Profile
if ($LASTEXITCODE -ne 0) { throw "Snapshot manifest verification failed." }
# A concurrent reproduction is serialized; a Windows file lock is reported clearly.
& $venvPython -m src.cli --profile $Profile refresh-db
if ($LASTEXITCODE -ne 0) { throw "Database refresh failed." }
& $venvPython -m src.cli --profile $Profile stats
if ($LASTEXITCODE -ne 0) { throw "Statistics check failed." }
& $venvPython scripts\verify_paper_claims.py --profile $Profile
if ($LASTEXITCODE -ne 0) { throw "A paper claim does not hold." }
& $venvPython scripts\reproduce_paper_table2.py --profile $Profile
if ($LASTEXITCODE -ne 0) { throw "Table 2 does not reproduce." }
$evidence = "evidence-$Profile-" + (Get-Date -Format "yyyyMMddTHHmmssZ") + ".txt"
& {
  "TopVenues reproduction evidence"
  "profile:     $Profile"
  "date (UTC):  " + (Get-Date -Format "u")
  "os:          " + [System.Environment]::OSVersion.VersionString
  "python:      " + (& $venvPython --version)
  ""
  "--- paper claims ---"
  & $venvPython scripts\verify_paper_claims.py --profile $Profile
  ""
  "--- Table 2 ---"
  & $venvPython scripts\reproduce_paper_table2.py --profile $Profile
} | Out-File -Encoding utf8 $evidence
Write-Host "execution evidence written to $evidence"
& $venvPython -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "Test suite failed." }
& $venvPython scripts\smoke_web_app.py
if ($LASTEXITCODE -ne 0) { throw "Web interface smoke test failed." }
& $venvPython scripts\benchmark_search.py --profile $Profile --trials 11
if ($LASTEXITCODE -ne 0) { throw "Search exercise failed." }
$sample = Join-Path ([System.IO.Path]::GetTempPath()) ("topvenues_repro_" + [guid]::NewGuid().ToString() + ".bib")
try {
    & $venvPython -m src.cli --profile $Profile export --title intrusion --format bibtex --output $sample
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $sample) -or (Get-Item $sample).Length -le 1000) { throw "BibTeX export check failed." }
} finally { Remove-Item $sample -ErrorAction SilentlyContinue }
Write-Host "Profile $Profile reproduced successfully on native Windows PowerShell."
