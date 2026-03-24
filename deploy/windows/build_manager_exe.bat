@echo off
setlocal

cd /d D:\FinFlow

py -3.10 -m pip install --upgrade pip
py -3.10 -m pip install -r deploy\windows\manager_requirements.txt
py -3.10 -m pip install pyinstaller

py -3.10 -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onefile ^
  --noconsole ^
  --name FinFlowManager ^
  --distpath deploy\windows\dist ^
  --workpath deploy\windows\build ^
  --specpath deploy\windows ^
  tools\finflow_manager.py

echo.
echo Build finished:
echo D:\FinFlow\deploy\windows\dist\FinFlowManager.exe

endlocal
