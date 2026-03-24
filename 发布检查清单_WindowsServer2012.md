# FinFlow 发布检查清单（Windows Server 2012）

## 一、代码与文件

- 已确认本次要发布的代码版本
- `git status` 已检查，无误提交敏感文件
- 未提交 `backend/.env`
- 未提交 `frontend/.env`
- 未提交 `.encryption.key`
- 未提交 `backend/.encryption.key`
- 未提交 `__pycache__`
- 未提交日志文件
- `frontend/dist` 已确认是否应随发布包提供

## 二、后端配置

- `backend/.env` 已存在
- `DATABASE_URL` 或 `DB_*` 已填写正确
- `SECRET_KEY` 已填写生产值
- `ACCESS_TOKEN_EXPIRE_MINUTES` 已确认
- `APP_RELOAD=false`
- `APP_HOST` 和 `APP_PORT` 已确认
- `ALLOWED_ORIGINS` 已按正式访问方式配置
- `ALLOW_LAN_ORIGINS` 已按实际场景配置
- `backend/.encryption.key` 已存在
- 已确认 `backend/.encryption.key` 来源正确

## 三、后端运行环境

- Python 虚拟环境已创建
- `pip install -r requirements.txt` 已完成
- `python -m py_compile ...` 已通过
- `D:\FinFlow\backend\logs` 已存在
- 后端可由管理器正常启动
- 后端可由管理器正常停止
- 后端可由管理器正常重启
- `127.0.0.1:8100` 或正式地址可访问

## 四、前端发布

- 前端已在构建机完成 `npm run build`
- 生产环境使用 `VITE_API_BASE_URL=/api`
- `frontend/dist` 已复制到服务器或已包含在发布包中
- 首页可正常打开
- 前端子路由刷新不会异常

## 五、管理器能力

- `FinFlowManager.exe` 可正常启动
- 关闭主窗口后会最小化到托盘
- “打开前端”功能可用
- “日志查看”功能可用
- “配置管理”保存可用
- “健康检查”状态可更新
- “Windows 登录后自动启动管理器”已按需要设置

## 六、升级与备份

- 本次发布前已完成数据库备份
- `pg_dump` 路径已配置或可直接调用
- 已确认发布包 ZIP 结构正确
- 已确认升级时不会覆盖本机 `.env` 和密钥
- 已确认如需回滚的数据库恢复方式

## 七、上线后验证

- 能正常打开首页
- 能正常登录
- 菜单权限正常
- 关键列表页可正常加载
- 核心接口无持续 401/403/500
- 外部系统凭据可正常使用
- 后端日志无持续报错
- 托盘管理器状态正常

## 八、回滚准备

- 已保留上一个稳定版本发布包或代码包
- 已保留本次发布前数据库备份
- 已确认回滚负责人和回滚步骤
