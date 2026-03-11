# API 接口编辑器增强功能说明

## 概述
API 接口管理中心的"新增/编辑接口"窗口已升级为一个专业、全面的 API 管理工具，支持多标签页编辑和更多配置选项。

## 新增功能

### 1. 多标签页组织 (Tab-based Organization)
编辑窗口现在分为 4 个标签页，清晰组织不同类型的信息：

#### 📋 基本信息 (Basic Info)
- **API 名称**: 接口的显示名称（必填）
- **请求方法**: GET/POST/PUT/DELETE/PATCH（必填）
- **接口路径**: API 的 URL 路径（必填）
- **所属凭证服务**: 选择此接口对应的外部系统凭证（必填）
- **简要描述**: 一句话描述接口功能
- **启用状态**: 勾选框，控制接口是否在生产中使用

#### 📤 请求配置 (Request Config)
- **请求头 (Headers)**: 以 JSON 格式输入请求头配置
  - 支持变量替换，如 `{token}`、`{API_KEY}`
  - 示例：`{"Authorization": "Bearer {token}", "Content-Type": "application/json"}`
- **请求体示例 (Request Body)**: 
  - POST/PUT/PATCH 请求的示例 JSON
  - 用于文档展示和前端集成参考

#### 📥 响应示例 (Response Example)
- **响应示例**: 接口成功响应的完整 JSON 示例
  - 包含业务数据结构
  - 便于前端开发者快速集成
  - 可用于自动化测试

#### 📝 文档说明 (Documentation)
- **详细文档**: 支持 Markdown 格式的完整文档
  - 详细的功能说明
  - 参数详细说明
  - HTTP 状态码和错误处理
  - 版本变更历史
  - 调用限制和注意事项
  - 使用示例代码

## 数据模型扩展

现有 `ExternalApi` 接口已扩展以支持新字段：

```typescript
interface ExternalApi {
    id: number;
    name: string;
    method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
    url_path: string;
    description: string;
    is_active: boolean;
    service_id?: number;
    request_headers?: string;      // 新增
    request_body?: string;          // 新增
    response_example?: string;      // 新增
    notes?: string;                 // 新增
}
```

## 用户界面改进

### 窗口尺寸
- 宽度: 700px（相比之前的 500px 更宽）
- 最大高度: 90vh（支持滚动）
- 灵活的弹性布局确保内容完整显示

### 视觉反馈
- 标签页有明显的激活状态指示器（蓝色下划线）
- 鼠标悬停时的视觉反馈
- 表单字段获焦时的渐变阴影效果
- 必填字段用红色星号 (*) 标记

### 表单增强
- 每个字段配有帮助文本 (Helper Text)
- 代码编辑区域使用等宽字体
- 复选框使用原生样式，颜色与主题匹配
- 文本框支持自动高度调整

## 使用流程

### 创建新接口
1. 点击"新增接口"按钮
2. 在"基本信息"标签页填写必填项
3. 切换到其他标签页补充详细信息
4. 点击"💾 保存接口配置"

### 编辑现有接口
1. 点击接口行的编辑按钮（铅笔图标）
2. 修改需要更新的信息
3. 点击"💾 保存接口配置"

## 最佳实践

### 请求头配置
```json
{
  "Content-Type": "application/json",
  "Authorization": "Bearer {access_token}",
  "X-API-Version": "1.0"
}
```

### 请求体示例
```json
{
  "page": 1,
  "pageSize": 20,
  "filters": {
    "status": "active"
  }
}
```

### 响应示例
```json
{
  "success": true,
  "code": 0,
  "message": "操作成功",
  "data": {
    "total": 100,
    "items": []
  }
}
```

### 文档说明模板
```markdown
## 功能说明
此接口用于获取...

## 参数说明
- page: 页码，从1开始
- pageSize: 每页记录数，最大100

## 错误码
- 401: 认证失败
- 403: 权限不足
- 429: 请求过于频繁

## 版本历史
- v1.0: 初始版本 (2026-01-15)
```

## 数据持久化

所有字段在保存时都会被持久化到后端数据库。前端验证已集成到 `handleSaveApi()` 函数中。

## 浏览器兼容性

- Chrome/Edge: ✅ 完全支持
- Firefox: ✅ 完全支持  
- Safari: ✅ 完全支持
- IE11: ⚠️ 不支持（CSS Grid 等）

## 性能考虑

- 标签页切换使用状态管理，不需要重新渲染整个表单
- JSON 格式的字段允许用户输入任意文本，不做实时验证
- 大型响应示例（>10KB）会自动截断滚动区域

## 未来计划

- [ ] 代码编辑器集成（语法高亮、代码折叠）
- [ ] JSON Schema 验证
- [ ] 请求体/响应示例的 IDE 集成
- [ ] API 版本管理
- [ ] 变更日志自动生成
- [ ] API 文档自动导出（Markdown/HTML/PDF）
