# FinFlow

FinFlow 是一个前后端分离的财务业务项目，包含：

- `backend/`: FastAPI + SQLAlchemy 后端服务
- `frontend/`: React + TypeScript + Vite 前端
- `deploy/`: Windows 部署与管理器相关脚本
- `docs/`: 补充文档与部署说明

## 快速开始

### 后端

1. 复制 `backend/.env.example` 为 `backend/.env`
2. 按实际环境填写数据库、鉴权和第三方集成配置
3. 安装依赖并启动

```powershell
cd backend
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python -m uvicorn main:app --host 0.0.0.0 --port 8100 --reload
```

### 前端

1. 复制 `frontend/.env.example` 为 `frontend/.env`
2. 安装依赖并启动开发服务器

```powershell
cd frontend
npm install
npm run dev
```

默认情况下，前端通过 `VITE_API_PROXY_TARGET` 将 `/api` 代理到本地后端。

## 常用校验

```powershell
# 后端测试
.\backend\.venv\Scripts\python.exe -m pytest

# 前端静态检查
cd frontend
npm run lint

# 前端生产构建
cd frontend
npm run build
```

## 当前工程基线

- 后端测试当前通过
- 前端构建当前通过
- 前端 ESLint 当前可运行，但仍保留较多 warning，主要集中在历史 `any` 类型和部分 Hook 依赖整理

## 版本控制建议

项目中存在数据库、日志、打包产物、调试目录和敏感配置文件。本仓库已补充常见忽略规则，提交前仍建议确认以下内容不会进入版本控制：

- `backend/.env`
- `.encryption.key`
- `backend/finflow.db`
- `frontend/dist/`
- `backend/logs/`
- 根目录压缩包、临时目录和本地调试文件
