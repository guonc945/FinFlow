@echo off
setlocal

set PROJECT_ROOT=%~dp0..\..
set TOOLS_PY=%PROJECT_ROOT%\tools\build_manager.py

echo ================================================================
echo FinFlowManager EXE Build
echo ================================================================
echo Project root: %PROJECT_ROOT%
echo Build script: %TOOLS_PY%
echo.

if not exist "%TOOLS_PY%" (
    echo Build script not found: %TOOLS_PY%
    exit /b 1
)

python "%TOOLS_PY%"
set EXIT_CODE=%ERRORLEVEL%

echo.
if not "%EXIT_CODE%"=="0" (
    echo Build failed. Exit code: %EXIT_CODE%
    exit /b %EXIT_CODE%
)

echo Build completed successfully.
exit /b 0
