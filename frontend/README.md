# FinFlow Frontend

FinFlow 前端基于 React 19、TypeScript 和 Vite，负责财务数据管理、报表配置、档案维护和集成配置等页面。

## 本地开发

1. 复制环境变量模板

```powershell
Copy-Item .env.example .env
```

2. 安装依赖

```powershell
npm install
```

3. 启动开发环境

```powershell
npm run dev
```

默认端口由 `VITE_PORT` 控制，API 代理目标由 `VITE_API_PROXY_TARGET` 控制。

## 构建与检查

```powershell
npm run lint
npm run build
```

## 目录说明

- `src/pages/`: 业务页面
- `src/components/`: 通用组件
- `src/services/`: API 请求封装
- `src/routes/`: 路由与懒加载
- `src/styles/`: 全局样式

## 当前注意事项

- ESLint 当前已经可以完整运行，但仍存在较多 warning，主要来自历史 `any` 类型和部分 Hook 依赖问题
- `dist/` 为构建产物目录，不应手动修改
- 如果后端地址变化，请同步调整 `.env` 中的 API 相关配置
