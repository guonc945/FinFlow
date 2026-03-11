@echo off
echo Starting FinFlow Platform...

start "FinFlow Backend" cmd /k "cd backend && call .venv\Scripts\activate && python main.py"
start "FinFlow Frontend" cmd /k "cd frontend && npm run dev"

echo Services starting based on .env configurations...
echo Backend expected on port 8100 (refer to backend/.env)
echo Frontend expected on port 5273 (refer to frontend/.env)
echo.
echo Press any key to exit this launcher (windows will remain open)...
pause
