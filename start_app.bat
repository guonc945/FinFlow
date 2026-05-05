@echo off
setlocal

cd /d "%~dp0"

set "MANAGER_EXE=%~dp0deploy\windows\dist\FinFlowManager.exe"
if not exist "%MANAGER_EXE%" set "MANAGER_EXE=%~dp0FinFlowManager.exe"

if not exist "%MANAGER_EXE%" (
  echo [ERROR] FinFlowManager.exe not found.
  echo Expected one of:
  echo   %~dp0deploy\windows\dist\FinFlowManager.exe
  echo   %~dp0FinFlowManager.exe
  pause
  exit /b 1
)

echo Starting FinFlow Manager...
start "" "%MANAGER_EXE%"
echo FinFlow Manager launched. Use the manager to start Windows services or open the tray UI.

endlocal
