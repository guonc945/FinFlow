@echo off
setlocal

set PROJECT_ROOT=%~dp0..\..
set SCRIPT_PATH=%PROJECT_ROOT%\tools\pack_update.py

if not exist "%SCRIPT_PATH%" (
    echo [ERROR] Update package script not found: %SCRIPT_PATH%
    exit /b 1
)

python "%SCRIPT_PATH%" %*
exit /b %ERRORLEVEL%
