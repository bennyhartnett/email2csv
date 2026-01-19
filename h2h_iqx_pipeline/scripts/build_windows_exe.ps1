$ErrorActionPreference = "Stop"

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $here "..")
Set-Location $repoRoot

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

. .\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
pip install pyinstaller

pyinstaller --noconfirm --clean --windowed --name "H2H IQX Pipeline" `
  --paths "src" `
  --add-data "assets;assets" `
  --add-data "web;web" `
  --add-data "config;config" `
  "src\\h2h_pipeline\\webview_app.py"
