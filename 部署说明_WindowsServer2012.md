# FinFlow 部署说明与指导手册（Windows Server 2012）

本文适用于：

- 服务器系统：Windows Server 2012
- 部署目标：生产环境
- 部署模式：`FinFlowManager.exe` 常驻托盘 + 直接管理 Python 后端进程

## 1. 最终部署方案

当前推荐的正式方案如下：

1. 不使用 IIS。
2. 不使用 NSSM。
3. 不注册 Windows 服务。
4. 前端先在构建机完成 `npm run build`。
5. 后端直接托管 `frontend/dist`。
6. 服务器上只运行 `FinFlowManager.exe`，由它负责配置、启停、备份、升级和监控。

这个方案更贴合你当前的目标：所有服务管理、配置管理、部署操作都收口到一个 EXE。

## 2. 建议目录结构

```text
D:\FinFlow\
  backend\
  frontend\
  deploy\
  tools\
```

建议日志目录：

```text
D:\FinFlow\backend\logs\
```

建议备份目录：

```text
D:\FinFlow\backups\
```

## 3. 服务器准备

建议提前安装：

- Python 3.10 x64
- PostgreSQL 客户端工具
- Visual C++ 运行库

说明：

- Windows Server 2012 较老，不建议在服务器上承担前端构建任务
- 前端最好在开发机或构建机完成打包后再上传

## 4. 配置文件准备

### 4.1 后端

- 示例文件：[backend/.env.example](/D:/FinFlow/backend/.env.example)
- 实际文件：`D:\FinFlow\backend\.env`
- 密钥文件：`D:\FinFlow\backend\.encryption.key`

至少确认这些配置：

- `DATABASE_URL` 或 `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD`
- `SECRET_KEY`
- `ACCESS_TOKEN_EXPIRE_MINUTES`
- `MARKI_USER`
- `MARKI_PASSWORD`
- `MARKI_SYSTEM_ID`
- `APP_HOST`
- `APP_PORT`
- `APP_RELOAD=false`

推荐生产配置示例：

```env
APP_HOST=127.0.0.1
APP_PORT=8100
APP_RELOAD=false
ALLOWED_ORIGINS=http://127.0.0.1:8100,http://localhost:8100
ALLOW_LAN_ORIGINS=false
DATABASE_URL=postgresql+psycopg2://finflow_user:your_password@127.0.0.1:5432/finflow
```

### 4.2 前端

- 示例文件：[frontend/.env.example](/D:/FinFlow/frontend/.env.example)
- 生产示例：[frontend/.env.production.example](/D:/FinFlow/frontend/.env.production.example)

生产环境建议：

```env
VITE_API_BASE_URL=/api
```

## 5. 首次部署步骤

### 5.1 准备代码

把项目代码放到：

```text
D:\FinFlow
```

### 5.2 创建后端虚拟环境

```powershell
cd D:\FinFlow\backend
py -3.10 -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\pip.exe install -r requirements.txt
```

如果未来需要 SQL Server 相关连接，再补装：

```powershell
.venv\Scripts\pip.exe install pyodbc
```

### 5.3 配置 `backend/.env`

复制：

```text
backend\.env.example -> backend\.env
```

然后填写生产配置。

也可以后续直接用 `FinFlowManager` 的“配置管理”页填写并保存。

### 5.4 准备加密密钥

新环境首次部署可生成：

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

把结果写入：

```text
D:\FinFlow\backend\.encryption.key
```

如果是旧环境迁移：

- 必须沿用原来的 `backend\.encryption.key`
- 不要随意更换，否则历史加密数据会无法解密

### 5.5 准备前端构建产物

推荐在构建机执行：

```powershell
cd D:\FinFlow\frontend
copy .env.production.example .env.production
npm ci
npm run build
```

然后把 `frontend\dist` 整个目录复制到服务器。

### 5.6 语法检查

```powershell
cd D:\FinFlow\backend
.venv\Scripts\python.exe -m py_compile main.py database.py models.py schemas.py utils\auth.py utils\crypto.py
```

## 6. 管理器运行方式

### 6.1 从源码运行

```powershell
cd D:\FinFlow
py -3.10 -m pip install -r deploy\windows\manager_requirements.txt
py -3.10 tools\finflow_manager.py
```

### 6.2 打包 EXE

```powershell
cd D:\FinFlow
deploy\windows\build_manager_exe.bat
```

生成文件：

```text
D:\FinFlow\deploy\windows\dist\FinFlowManager.exe
```

### 6.3 首次启动后建议操作

打开管理器后按下面顺序：

1. 在“配置管理”页保存 `backend/.env`
2. 确认 `backend/.encryption.key` 已存在
3. 在“服务状态”页点击“启动后端”
4. 点击“打开前端”确认系统可访问
5. 根据需要勾选“Windows 登录后自动启动管理器”

## 7. 对外访问说明

### 7.1 仅服务器本机访问

使用：

```env
APP_HOST=127.0.0.1
APP_PORT=8100
```

访问地址：

```text
http://127.0.0.1:8100/
```

### 7.2 局域网访问

使用：

```env
APP_HOST=0.0.0.0
APP_PORT=8100
ALLOW_LAN_ORIGINS=true
```

还需要：

- Windows 防火墙放行 `8100`
- 客户端使用服务器实际 IP 访问

## 8. 一键升级说明

管理器支持导入 ZIP 发布包。

推荐 ZIP 内容包含：

```text
backend\
frontend\dist\
tools\
deploy\
start_app.bat
```

升级动作会：

- 自动停止后端
- 覆盖发布包中的代码和前端产物
- 保留本机 `.env`、密钥、虚拟环境、日志、管理器状态文件
- 如原先后端正在运行，升级后自动尝试重新启动

升级前仍建议先做一次数据库备份。

## 9. 数据库备份

管理器“运维工具”页支持直接调用 `pg_dump` 备份。

你只需要配置：

- `pg_dump` 可执行文件路径
- 备份输出目录

备份连接信息来自当前 `backend/.env`。

## 10. 上线后检查

至少确认：

- 首页能正常打开
- 登录可用
- 主要菜单和核心列表页正常
- 后端日志无持续报错
- 外部系统凭据可正常使用
- 数据库备份可正常执行
- 关闭主窗口后，管理器已最小化到托盘

## 11. 重要注意事项

- `.env` 和密钥文件不要重新提交到 Git
- 如果管理器退出，后端也会一起停止
- 当前方案不依赖 IIS、NSSM 或 Windows 服务
- 若使用开机自启，本质是登录后自动启动托盘管理器，不是系统级服务

详细功能请配合阅读：[FinFlowManager_使用说明.md](/D:/FinFlow/FinFlowManager_使用说明.md)
