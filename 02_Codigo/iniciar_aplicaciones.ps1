param(
 [string]$Python = 'python',
 [string]$DatabaseUrl = 'postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301',
 [int]$ApiPort = 18080,
 [int]$DashboardPort = 18050
)
$ErrorActionPreference = 'Stop'
$taskProject = Split-Path $PSScriptRoot -Parent
$taskLogs = Join-Path $taskProject 'data/logs'
$taskPythonCommand = if (Test-Path -LiteralPath $Python) { (Resolve-Path -LiteralPath $Python).Path } else { (Get-Command $Python -ErrorAction Stop).Source }
New-Item -ItemType Directory -Path $taskLogs -Force | Out-Null
$env:PROJECT_ROOT = $taskProject
$env:LCD301_PROJECT_ROOT = $taskProject
$env:DATABASE_URL = $DatabaseUrl
$env:PYTHONPATH = $PSScriptRoot
$env:DASH_HOST = '127.0.0.1'
$env:DASH_PORT = [string]$DashboardPort
# No usar este archivo para iniciar una segunda copia de servicios ya activos.
Start-Process -FilePath $taskPythonCommand -ArgumentList @('-m','uvicorn','api.main:app','--host','127.0.0.1','--port',[string]$ApiPort) -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $taskLogs 'api.stdout.log') -RedirectStandardError (Join-Path $taskLogs 'api.stderr.log')
Start-Process -FilePath $taskPythonCommand -ArgumentList @('-u','app.py') -WorkingDirectory (Join-Path $taskProject '03_Visualizaciones/dashboard') -WindowStyle Hidden -RedirectStandardOutput (Join-Path $taskLogs 'dashboard.stdout.log') -RedirectStandardError (Join-Path $taskLogs 'dashboard.stderr.log')
Write-Output "API: http://127.0.0.1:$ApiPort | Dashboard: http://127.0.0.1:$DashboardPort"
