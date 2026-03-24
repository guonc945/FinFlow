# FinFlowManager 使用说明

`FinFlowManager` 是一个面向 Windows Server 2012 的常驻托盘管理器，用来统一管理：

- `backend/.env` 配置
- `backend/.encryption.key`
- 后端启动、停止、重启
- 前端访问入口
- 前端 `dist` 部署
- PostgreSQL 备份
- 发布包 ZIP 一键升级
- 日志查看

## 1. 适用场景

适用于以下模式：

- 不使用 IIS
- 不使用 NSSM
- 不注册 Windows 服务
- 通过一个单独 EXE 负责配置和服务管理
- 关闭窗口后最小化到系统托盘继续运行

## 2. 当前架构

当前项目采用单端口模式：

- 后端继续提供 `/api/*`
- 后端直接托管 `frontend/dist`
- 管理器只需要管理一个 Python 后端进程

也就是说，生产环境主要入口只有一个：

```text
http://服务器地址:8100/
```

## 3. 从源码运行

先安装管理器依赖：

```powershell
cd D:\FinFlow
py -3.10 -m pip install -r deploy\windows\manager_requirements.txt
```

再启动管理器：

```powershell
py -3.10 tools\finflow_manager.py
```

## 4. 打包为单 EXE

执行：

```powershell
cd D:\FinFlow
deploy\windows\build_manager_exe.bat
```

生成位置：

```text
D:\FinFlow\deploy\windows\dist\FinFlowManager.exe
```

## 5. 运行要求

管理器附近需要能找到项目目录，至少应存在：

- `backend\main.py`
- `frontend\dist`

推荐做法：

- 直接把 EXE 放在 `D:\FinFlow\deploy\windows\dist`
- 项目根目录保持为 `D:\FinFlow`

## 6. 功能说明

### 6.1 服务状态页

可查看：

- 项目目录
- `backend/.env` 是否存在
- `backend/.encryption.key` 是否存在
- `frontend/dist` 是否存在
- 后端是否正在运行
- 健康检查状态
- 管理器开机自启状态
- 当前访问地址

可执行：

- 启动后端
- 停止后端
- 重启后端
- 打开前端
- 打开日志目录
- 隐藏到托盘

### 6.2 配置管理页

可编辑：

- `APP_HOST`
- `APP_PORT`
- `ALLOWED_ORIGINS`
- `ALLOW_LAN_ORIGINS`
- `DATABASE_URL` 或 `DB_*`
- `SECRET_KEY`
- `ACCESS_TOKEN_EXPIRE_MINUTES`
- `MARKI_*`

可执行：

- 保存到 `backend/.env`
- 重新加载配置
- 生成 JWT 密钥
- 生成 `backend/.encryption.key`

### 6.3 运维工具页

可执行：

- 选择前端 `dist` 来源目录
- 覆盖部署到 `frontend/dist`
- 选择发布包 ZIP
- 一键升级发布包
- 选择 `pg_dump`
- 执行 PostgreSQL 备份

发布包升级会保留这些服务器本地文件：

- `backend/.env`
- `backend/.encryption.key`
- `backend/.venv`
- `backend/logs`
- `deploy/windows/manager_state.json`

### 6.4 日志页

可查看：

- 后端标准输出
- 后端错误输出
- 项目同步日志

## 7. 推荐配置

如果只允许服务器本机访问：

```env
APP_HOST=127.0.0.1
APP_PORT=8100
ALLOWED_ORIGINS=http://127.0.0.1:8100,http://localhost:8100
ALLOW_LAN_ORIGINS=false
APP_RELOAD=false
```

如果需要局域网访问：

```env
APP_HOST=0.0.0.0
APP_PORT=8100
ALLOWED_ORIGINS=
ALLOW_LAN_ORIGINS=true
APP_RELOAD=false
```

同时还需要：

- Windows 防火墙放行对应端口
- 客户端使用实际 IP 访问

## 8. 开机自启说明

管理器提供“Windows 登录后自动启动管理器”选项。

启用后会自动写入当前用户启动目录：

```text
%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\FinFlowManager.cmd
```

这不是 Windows 服务，只是在用户登录后自动启动托盘管理器。

## 9. 发布包建议结构

推荐 ZIP 内部结构如下：

```text
backend\
frontend\dist\
tools\
deploy\
start_app.bat
```

最少需要包含：

- `backend\`
或
- `frontend\dist\`

## 10. 注意事项

- 当前模式下，管理器本身就是总控进程
- 如果管理器退出，后端也会随之停止
- 如果数据库里已有历史加密数据，不要随意重建 `backend/.encryption.key`
- 发布包升级前建议先做数据库备份
