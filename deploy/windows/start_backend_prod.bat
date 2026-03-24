@echo off
setlocal

cd /d D:\FinFlow\backend

if not exist .venv\Scripts\python.exe (
  echo [ERROR] Python virtual environment not found: D:\FinFlow\backend\.venv
  exit /b 1
)

if not exist .env (
  echo [ERROR] backend\.env not found
  exit /b 1
)

if not exist .encryption.key (
  echo [ERROR] backend\.encryption.key not found
  exit /b 1
)

mkdir logs 2>nul

set APP_RELOAD=false

.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8100

endlocal
