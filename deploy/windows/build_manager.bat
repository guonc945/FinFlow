@echo off
setlocal

echo ==================================================
echo FinFlow Manager 打包脚本
echo ==================================================

set PROJECT_ROOT=%~dp0..
set TOOLS_DIR=%PROJECT_ROOT%\tools
set DIST_DIR=%PROJECT_ROOT%\deploy\windows\dist
set BUILD_DIR=%PROJECT_ROOT%\deploy\windows\build

echo 项目目录: %PROJECT_ROOT%
echo 工具目录: %TOOLS_DIR%
echo 输出目录: %DIST_DIR%
echo 构建目录: %BUILD_DIR%

echo.
echo ==================================================
echo 步骤 1: 清理旧文件
echo ==================================================

if exist "%DIST_DIR%" (
    echo 删除旧输出目录...
    rmdir /s /q "%DIST_DIR%"
)

if exist "%BUILD_DIR%" (
    echo 删除旧构建目录...
    rmdir /s /q "%BUILD_DIR%"
)

echo 创建输出目录...
mkdir "%DIST_DIR%" 2>nul
mkdir "%BUILD_DIR%" 2>nul

echo.
echo ==================================================
echo 步骤 2: 安装管理器依赖
echo ==================================================

python -m pip install -r "%TOOLS_DIR%\manager_requirements.txt" --quiet

echo.
echo ==================================================
echo 步骤 3: 执行 PyInstaller 打包
echo ==================================================

python -m PyInstaller ^
    --name "FinFlowManager" ^
    --onefile ^
    --windowed ^
    --icon "%TOOLS_DIR%\finflow_icon_temp.ico" ^
    --add-data "%TOOLS_DIR%;tools" ^
    --add-data "%PROJECT_ROOT%\backend;backend" ^
    --add-data "%PROJECT_ROOT%\frontend\dist;frontend\dist" ^
    --add-data "%PROJECT_ROOT%\deploy;deploy" ^
    --hidden-import=pystray ^
    --hidden-import=PIL ^
    --hidden-import=PIL.Image ^
    --hidden-import=PIL.ImageDraw ^
    --collect-all=pystray ^
    --outputpath="%DIST_DIR%" ^
    --workpath="%BUILD_DIR%" ^
    --specpath="%BUILD_DIR%" ^
    "%TOOLS_DIR%\finflow_manager.py"

echo.
echo ==================================================
echo 打包完成
echo ==================================================

if exist "%DIST_DIR%\FinFlowManager.exe" (
    echo 生成文件: %DIST_DIR%\FinFlowManager.exe
    dir "%DIST_DIR%\FinFlowManager.exe"
) else (
    echo 警告: 未找到生成的 exe 文件
)

echo.
echo ==================================================
echo 步骤 4: 清理构建中间文件
echo ==================================================

if exist "%BUILD_DIR%" (
    echo 删除构建目录...
    rmdir /s /q "%BUILD_DIR%"
)

echo 清理完成
echo ==================================================
