
import { useState, useEffect } from 'react';
import {
    Plus, Edit2, Trash2, FileJson, X, Search,
    Server, Terminal, Code, Copy, Check,
    LayoutGrid, List as ListIcon, Info, ExternalLink, Hash,
    Play, CheckCircle, AlertTriangle, Clock, Activity,
    ChevronDown, ChevronUp, HelpCircle
} from 'lucide-react';
import axios from 'axios';
import JsonEditor from '../../../components/data/JsonEditor';
import VariablePicker from '../../settings/VariablePicker';
import { API_BASE_URL } from '../../../services/apiBase';
import './APIManager.css';

interface ExternalApi {
    id: number;
    name: string;
    method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
    url_path: string;
    description: string;
    is_active: boolean;
    service_id?: number;
    request_headers?: string;
    request_body?: string;
    response_example?: string;
    notes?: string;
    category?: string;
}

const CATEGORY_OPTIONS = [
    { value: 'ALL', label: '全部分类' },
    { value: '金蝶系统', label: '金蝶系统' },
    { value: '马克系统', label: '马克系统' },
    { value: '泛微OA', label: '泛微OA' }
];

const MODAL_CATEGORY_OPTIONS = [
    { value: '', label: '未分类' },
    { value: '金蝶系统', label: '金蝶系统' },
    { value: '马克系统', label: '马克系统' },
    { value: '泛微OA', label: '泛微OA' }
];

interface ExternalService {
    id: number;
    service_name: string;
    display_name: string;
    base_url: string;
    auth_type: string;
    apis: ExternalApi[];
}

const APIManager = () => {
    const [services, setServices] = useState<ExternalService[]>([]);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedCategory, setSelectedCategory] = useState<string>('ALL');
    const [selectedServiceId, setSelectedServiceId] = useState<string>('ALL');
    const [viewMode, setViewMode] = useState<'list' | 'grid'>('list');
    const [isCollapsed, setIsCollapsed] = useState(false);

    // Pagination State
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(10);

    // Editing State
    const [isEditing, setIsEditing] = useState(false);
    const [currentApi, setCurrentApi] = useState<Partial<ExternalApi>>({});
    const [editTabActive, setEditTabActive] = useState<'basic' | 'request' | 'response' | 'notes' | 'test'>('basic');
    const [requestSubTab, setRequestSubTab] = useState<'headers' | 'body'>('headers');
    const [headerRows, setHeaderRows] = useState<Array<{ key: string, value: string }>>([]);
    const [queryRows, setQueryRows] = useState<Array<{ key: string, value: string }>>([]);
    const [isVariablePickerOpen, setIsVariablePickerOpen] = useState(false);
    const [lastFocusedField, setLastFocusedField] = useState<{
        ref: HTMLInputElement | HTMLTextAreaElement | null,
        setter: (val: string) => void
    } | null>(null);

    // Code View State
    const [showCode, setShowCode] = useState<(ExternalApi & { _service?: ExternalService }) | null>(null);
    const [copied, setCopied] = useState(false);

    // Test State
    const [isTesting, setIsTesting] = useState(false);
    const [testResult, setTestResult] = useState<any>(null);

    // Import State
    const [showImport, setShowImport] = useState(false);
    const [importContent, setImportContent] = useState('');
    const [importTargetServiceId, setImportTargetServiceId] = useState<number | null>(null);

    useEffect(() => {
        fetchServices();
    }, []);

    useEffect(() => {
        setCurrentPage(1);
    }, [searchTerm, viewMode, selectedCategory, selectedServiceId]);

    useEffect(() => {
        if (services.length > 0 && importTargetServiceId === null) {
            setImportTargetServiceId(services[0].id);
        }
    }, [services, importTargetServiceId]);

    const fetchServices = async () => {
        try {
            const res = await axios.get(`${API_BASE_URL}/external/services`);
            setServices(res.data);
        } catch (error) {
            console.error('Failed to fetch services:', error);
        }
    };

    // Parse JSON headers to form rows
    const parseHeadersToRows = (jsonStr: string) => {
        if (!jsonStr) return [];
        try {
            const obj = JSON.parse(jsonStr);
            return Object.entries(obj).map(([key, value]) => ({
                key,
                value: String(value)
            }));
        } catch {
            return [];
        }
    };

    // Convert form rows back to JSON
    const rowsToHeadersJson = (rows: Array<{ key: string, value: string }>) => {
        const obj: Record<string, string> = {};
        rows.forEach(row => {
            if (row.key.trim()) {
                obj[row.key.trim()] = row.value.trim();
            }
        });
        return JSON.stringify(obj, null, 2);
    };

    // New helpers for Query Rows (Same logic as headers)
    const parseQueryToRows = (jsonStr: string) => {
        if (!jsonStr) return [];
        try {
            const obj = JSON.parse(jsonStr);
            return Object.entries(obj).map(([key, value]) => ({
                key,
                value: String(value)
            }));
        } catch {
            return [];
        }
    };

    const rowsToQueryJson = (rows: Array<{ key: string, value: string }>) => {
        const obj: Record<string, string> = {};
        rows.forEach(row => {
            if (row.key.trim()) {
                obj[row.key.trim()] = row.value.trim();
            }
        });
        return JSON.stringify(obj, null, 2);
    };

    const handleSaveApi = async () => {
        try {
            const targetServiceId = (currentApi as any).service_id;
            if (!targetServiceId) {
                alert('请先为此接口选择所属服务');
                return;
            }

            const payload = {
                ...currentApi,
                service_id: targetServiceId,
                request_headers: rowsToHeadersJson(headerRows),
                request_body: currentApi.method === 'GET' ? rowsToQueryJson(queryRows) : currentApi.request_body
            };

            if (currentApi.id) {
                await axios.put(`${API_BASE_URL}/external/apis/${currentApi.id}`, payload);
            } else {
                await axios.post(`${API_BASE_URL}/external/services/${targetServiceId}/apis`, payload);
            }

            setIsEditing(false);
            fetchServices();
        } catch (error: any) {
            alert('保存失败: ' + (error.response?.data?.detail || error.message));
        }
    };

    const handleTestApi = async () => {
        setIsTesting(true);
        setTestResult(null);
        try {
            const headersObj: Record<string, string> = {};
            headerRows.forEach(row => {
                if (row.key) headersObj[row.key] = row.value;
            });
            const payload = {
                ...currentApi,
                request_headers: headersObj,
                request_body: currentApi.method === 'GET' ? JSON.parse(rowsToQueryJson(queryRows) || '{}') : JSON.parse(currentApi.request_body || '{}')
            };
            const res = await axios.post(`${API_BASE_URL}/archives/test`, payload);
            setTestResult(res.data);
        } catch (err: any) {
            setTestResult({
                success: false,
                error: err.response?.data?.error || err.message || '请求执行失败'
            });
        } finally {
            setIsTesting(false);
        }
    };

    const handleDeleteApi = async (id: number) => {
        if (!confirm('确定删除此 API 接口吗？')) return;
        try {
            await axios.delete(`${API_BASE_URL}/external/apis/${id}`);
            fetchServices();
        } catch (error) {
            console.error('Delete failed', error);
        }
    };

    const handleImportApis = async () => {
        if (!importTargetServiceId || !importContent) {
            alert('请选择目标服务并填写 JSON 内容');
            return;
        }
        try {
            const data = JSON.parse(importContent);
            if (!data.paths) throw new Error("Invalid OpenAPI/Swagger JSON: missing 'paths'");

            let count = 0;
            for (const [path, methods] of Object.entries(data.paths)) {
                for (const [method, details] of Object.entries(methods as any)) {
                    if (['get', 'post', 'put', 'delete', 'patch'].indexOf(method.toLowerCase()) === -1) continue;

                    const apiData = {
                        name: (details as any).summary || (details as any).operationId || `${method.toUpperCase()} ${path}`,
                        method: method.toUpperCase(),
                        url_path: path,
                        description: (details as any).description || '',
                        is_active: true
                    };

                    await axios.post(`${API_BASE_URL}/external/services/${importTargetServiceId}/apis`, apiData);
                    count++;
                }
            }
            alert(`成功导入 ${count} 个接口`);
            setShowImport(false);
            setImportContent('');
            fetchServices();
        } catch (e: any) {
            alert('导入失败: ' + e.message);
        }
    };

    const generateCodeSnippet = (api: ExternalApi, service: ExternalService) => {
        const fullUrl = service.base_url ? `\${BASE_URL}${api.url_path}` : api.url_path;
        return `# Python Integration Example
import requests

def call_${api.name.toLowerCase().replace(/\s+/g, '_')}():
    url = "${fullUrl}"
    # Retrieve token from database or environment
    headers = {
        "Authorization": "Bearer <YOUR_ACCESS_TOKEN>",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.request("${api.method}", url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error calling API: {e}")
        return None`;
    };

    const handleVariableSelect = (variable: any) => {
        if (lastFocusedField && lastFocusedField.ref) {
            const input = lastFocusedField.ref;
            const varTag = variable?.insert_text || (variable?.key ? `{${variable.key}}` : String(variable || ''));
            const start = input.selectionStart || 0;
            const end = input.selectionEnd || 0;
            const text = input.value;
            const newValue = text.substring(0, start) + varTag + text.substring(end);

            lastFocusedField.setter(newValue);

            // Re-focus and set cursor position after state update (approximate)
            setTimeout(() => {
                input.focus();
                const newPos = start + varTag.length;
                input.setSelectionRange(newPos, newPos);
            }, 0);
        }
    };

    const copyToClipboard = (text: string) => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    // Aggregate all APIs across services and attach owning service as `_service`
    const allApis: Array<ExternalApi & { _service?: ExternalService }> = services.flatMap(s =>
        s.apis.map(api => ({ ...api, _service: s }))
    );

    const filteredApis = allApis.filter(api => {
        const matchesCategory = selectedCategory === 'ALL' || api.category === selectedCategory;
        const matchesService = selectedServiceId === 'ALL' || api.service_id?.toString() === selectedServiceId;
        const matchesSearch = api.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            api.url_path.toLowerCase().includes(searchTerm.toLowerCase());
        return matchesCategory && matchesService && matchesSearch;
    });

    const totalPages = Math.ceil(filteredApis.length / pageSize);
    const startIndex = (currentPage - 1) * pageSize;
    const paginatedApis = filteredApis.slice(startIndex, startIndex + pageSize);

    return (
        <div className="api-manager-container animate-in fade-in duration-500">
            {/* Service Selector & Top Controls */}
            <div className={`service-panel ${isCollapsed ? 'collapsed' : ''}`}>
                        <div className="service-header" style={{ marginBottom: 0, cursor: 'pointer' }} onClick={() => setIsCollapsed(!isCollapsed)}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flex: 1 }}>
                                <div className="service-icon-box" style={{ width: '32px', height: '32px' }}>
                                    <Server size={16} />
                                </div>
                                <div className="service-title" style={{ marginRight: '1rem' }}>
                                    <h2 style={{ fontSize: '1rem', whiteSpace: 'nowrap' }}>API 接口管理中心</h2>
                                </div>

                                {!isCollapsed && (
                                    <div className="api-controls animate-in fade-in zoom-in-95 duration-300" onClick={(e) => e.stopPropagation()}>
                                        <div className="search-box" style={{ maxWidth: '200px', minWidth: '120px' }}>
                                            <Search size={14} className="search-icon" />
                                            <input
                                                type="text"
                                                placeholder="搜索接口..."
                                                value={searchTerm}
                                                onChange={e => setSearchTerm(e.target.value)}
                                                style={{ padding: '0.4rem 0.6rem 0.4rem 2rem', fontSize: '0.85rem' }}
                                            />
                                        </div>
                                        <select
                                            className="form-input"
                                            style={{ padding: '0.4rem', borderRadius: '6px', border: '1px solid #e2e8f0', width: '130px', minWidth: '90px', background: 'white', color: '#334155', fontSize: '0.85rem', height: '34px', textOverflow: 'ellipsis', overflow: 'hidden', whiteSpace: 'nowrap' }}
                                            value={selectedServiceId}
                                            onChange={e => setSelectedServiceId(e.target.value)}
                                        >
                                            <option value="ALL">全部服务</option>
                                            {services.map(s => (
                                                <option key={s.id} value={s.id.toString()}>{s.display_name || s.service_name}</option>
                                            ))}
                                        </select>
                                        <select
                                            className="form-input"
                                            style={{ padding: '0.4rem', borderRadius: '6px', border: '1px solid #e2e8f0', width: '110px', minWidth: '80px', background: 'white', color: '#334155', fontSize: '0.85rem', height: '34px', textOverflow: 'ellipsis', overflow: 'hidden', whiteSpace: 'nowrap' }}
                                            value={selectedCategory}
                                            onChange={e => setSelectedCategory(e.target.value)}
                                        >
                                            {CATEGORY_OPTIONS.map(opt => (
                                                <option key={opt.value} value={opt.value}>{opt.label}</option>
                                            ))}
                                        </select>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginLeft: 'auto' }}>
                                            <div className="view-toggle" style={{ padding: '2px' }}>
                                                <button
                                                    className={viewMode === 'list' ? 'active' : ''}
                                                    onClick={() => setViewMode('list')}
                                                    style={{ padding: '4px 8px' }}
                                                >
                                                    <ListIcon size={14} />
                                                </button>
                                                <button
                                                    className={viewMode === 'grid' ? 'active' : ''}
                                                    onClick={() => setViewMode('grid')}
                                                    style={{ padding: '4px 8px' }}
                                                >
                                                    <LayoutGrid size={14} />
                                                </button>
                                            </div>
                                            <button className="button button-secondary" style={{ padding: '0.4rem 0.75rem', fontSize: '0.85rem' }} onClick={() => { setShowImport(true); setImportTargetServiceId(services[0]?.id ?? null); }}>
                                                <FileJson size={14} /> 导入
                                            </button>
                                            <button className="button button-primary" style={{ padding: '0.4rem 0.75rem', fontSize: '0.85rem' }} onClick={() => { setCurrentApi({ method: 'POST', is_active: true, service_id: services[0]?.id }); setIsEditing(true); setEditTabActive('basic'); }}>
                                                <Plus size={14} /> 新增
                                            </button>
                                        </div>
                                    </div>
                                )}

                                {isCollapsed && (
                                    <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                                        <span style={{ fontSize: '0.75rem', color: '#64748b', background: '#f1f5f9', padding: '0.1rem 0.6rem', borderRadius: '10px' }}>
                                            {filteredApis.length} 个结果
                                        </span>
                                    </div>
                                )}
                            </div>
                            <button className="collapse-toggle-btn" style={{ background: 'none', border: 'none', color: '#64748b', cursor: 'pointer', padding: '4px' }}>
                                {isCollapsed ? <ChevronDown size={18} /> : <ChevronUp size={18} />}
                            </button>
                        </div>
                    </div>

            {/* API Content Area */}
            <div className="api-content-area">
                        {/* API List/Grid */}
                        {filteredApis.length > 0 ? (
                            <div className="api-results-panel">
                                {viewMode === 'list' ? (
                                    <div className="api-table-container api-results-scroll custom-scrollbar">
                                        <table className="api-table">
                                            <thead>
                                                <tr>
                                                    <th style={{ width: '60px', textAlign: 'center' }}>序号</th>
                                                    <th style={{ width: '100px' }}>方法</th>
                                                    <th style={{ width: '35%' }}>接口路径</th>
                                                    <th>名称与描述</th>
                                                    <th style={{ width: '120px', textAlign: 'right' }}>操作</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {paginatedApis.map((api, index) => (
                                                    <tr key={api.id}>
                                                        <td data-label="序号" style={{ textAlign: 'center', color: '#94a3b8', fontWeight: 500 }}>
                                                            {startIndex + index + 1}
                                                        </td>
                                                        <td data-label="方法">
                                                            <span className={`method-badge ${api.method}`}>
                                                                {api.method}
                                                            </span>
                                                        </td>
                                                        <td data-label="接口路径">
                                                            <div className="api-path">
                                                                <code className="api-path-code" title={api.url_path}>
                                                                    {api.url_path}
                                                                </code>
                                                                <ExternalLink size={14} className="text-gray-300" />
                                                            </div>
                                                        </td>
                                                        <td data-label="名称与描述">
                                                            <div className="api-name">
                                                                {api.name}
                                                                {api.category && (
                                                                    <span style={{ marginLeft: '0.5rem', fontSize: '0.75rem', padding: '0.1rem 0.4rem', background: '#f1f5f9', color: '#64748b', borderRadius: '4px' }}>
                                                                        {api.category}
                                                                    </span>
                                                                )}
                                                            </div>
                                                            <div className="api-description">{api.description || '暂无描述'}</div>
                                                        </td>
                                                        <td data-label="操作">
                                                            <div className="api-actions">
                                                                <button
                                                                    className="action-btn primary"
                                                                    title="调试代码"
                                                                    onClick={() => setShowCode(api)}
                                                                >
                                                                    <Terminal size={16} />
                                                                </button>
                                                                <button
                                                                    className="action-btn primary"
                                                                    title="编辑接口"
                                                                    onClick={() => { setCurrentApi(api); setEditTabActive('basic'); setRequestSubTab('headers'); setHeaderRows(parseHeadersToRows(api.request_headers || '')); setIsEditing(true); }}
                                                                >
                                                                    <Edit2 size={16} />
                                                                </button>
                                                                <button
                                                                    className="action-btn danger"
                                                                    title="删除接口"
                                                                    onClick={() => handleDeleteApi(api.id)}
                                                                >
                                                                    <Trash2 size={16} />
                                                                </button>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                ) : (
                                    <div className="api-grid api-results-scroll custom-scrollbar">
                                        {paginatedApis.map(api => (
                                            <div key={api.id} className="api-card">
                                                <div className="card-method-header">
                                                    <span className={`method-badge ${api.method}`}>
                                                        {api.method}
                                                    </span>
                                                    <div className="card-actions">
                                                        <button
                                                            className="card-action-btn edit"
                                                            title="编辑接口"
                                                            onClick={() => {
                                                                setCurrentApi(api);
                                                                setEditTabActive('basic');
                                                                setRequestSubTab('headers');
                                                                setHeaderRows(parseHeadersToRows(api.request_headers || ''));
                                                                setQueryRows(api.method === 'GET' ? parseQueryToRows(api.request_body || '') : []);
                                                                setIsEditing(true);
                                                            }}
                                                        >
                                                            <Edit2 size={16} />
                                                        </button>
                                                        <button
                                                            className="card-action-btn delete"
                                                            title="删除接口"
                                                            onClick={() => handleDeleteApi(api.id)}
                                                        >
                                                            <Trash2 size={16} />
                                                        </button>
                                                    </div>
                                                </div>
                                                <div className="card-title-row">
                                                    <h3 className="card-name" title={api.name} style={{ margin: 0 }}>{api.name}</h3>
                                                    {api.category && (
                                                        <span
                                                            className={`card-system-badge ${api.category === '金蝶系统'
                                                                ? 'kingdee'
                                                                : api.category === '马克系统'
                                                                    ? 'marki'
                                                                    : 'default'
                                                                }`}
                                                        >
                                                            {api.category}
                                                        </span>
                                                    )}
                                                </div>
                                                <div className="card-service-meta" title={api._service?.display_name || api._service?.service_name || ''}>
                                                    服务：{api._service?.display_name || api._service?.service_name || '未指定'}
                                                </div>
                                                {api.description && (
                                                    <p className="card-description" title={api.description}>
                                                        {api.description}
                                                    </p>
                                                )}
                                                <div className="card-footer">
                                                    <span className="card-id">ID: {api.id}</span>
                                                    <button
                                                        className="card-code-btn"
                                                        title="查看调用代码"
                                                        onClick={() => setShowCode(api)}
                                                    >
                                                        <Code size={12} /> 代码
                                                    </button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}

                                {/* Pagination UI */}
                                <div className="api-pagination-bar">
                                    <div className="api-pagination-summary">
                                        共 <strong>{filteredApis.length}</strong> 个接口
                                    </div>
                                    <div className="api-pagination-controls">
                                        <select
                                            className="api-page-size-select"
                                            value={pageSize}
                                            onChange={e => { setPageSize(Number(e.target.value)); setCurrentPage(1); }}
                                        >
                                            <option value={10}>10条/页</option>
                                            <option value={20}>20条/页</option>
                                            <option value={50}>50条/页</option>
                                        </select>
                                        <div className="api-page-nav">
                                            <button
                                                className="api-page-btn"
                                                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                                                disabled={currentPage === 1}
                                            >
                                                上一页
                                            </button>
                                            <span className="api-page-indicator">
                                                {currentPage} / {totalPages || 1}
                                            </span>
                                            <button
                                                className="api-page-btn"
                                                onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                                                disabled={currentPage === totalPages || totalPages === 0}
                                            >
                                                下一页
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="empty-state">
                                <div className="empty-icon">
                                    <Search size={32} />
                                </div>
                                <h3 className="empty-title">未找到任何接口定义</h3>
                                <p className="empty-text">
                                    当前服务尚无 API 接口定义。点击下方按钮创建第一个接口，或使用批量导入功能导入 OpenAPI 规范。
                                </p>
                                <button
                                    className="empty-action"
                                    onClick={() => { setCurrentApi({ method: 'POST', is_active: true, service_id: services[0]?.id }); setIsEditing(true); setEditTabActive('basic'); }}
                                >
                                    <Plus size={16} className="inline mr-2" /> 立即创建接口
                                </button>
                            </div>
                        )}
                    </div>

                    {/* Edit Modal */}
                    {isEditing && (
                        <div className="modal-overlay">
                            <div className="modal-content" style={{ maxWidth: '750px', maxHeight: '90vh', display: 'flex', flexDirection: 'column' }}>
                                <div className="modal-header">
                                    <h3 className="modal-title">{currentApi.id ? '编辑 API 接口' : '新建 API 接口'}</h3>
                                    <button
                                        onClick={() => { setIsEditing(false); setEditTabActive('basic'); setRequestSubTab('headers'); }}
                                        className="modal-close-btn"
                                        title="关闭"
                                    >
                                        <X size={20} />
                                    </button>
                                </div>

                                {/* Tabs */}
                                <div style={{
                                    display: 'flex',
                                    borderBottom: '1px solid #e2e8f0',
                                    background: '#f8fafc',
                                    paddingLeft: '1.5rem'
                                }}>
                                    {[
                                        { key: 'basic', label: '基本信息', icon: '📋' },
                                        { key: 'request', label: '请求配置', icon: '📤' },
                                        { key: 'response', label: '响应示例', icon: '📥' },
                                        { key: 'notes', label: '文档说明', icon: '📝' },
                                        { key: 'test', label: '测试预览', icon: '🧪' }
                                    ].map(tab => (
                                        <button
                                            key={tab.key}
                                            onClick={() => setEditTabActive(tab.key as any)}
                                            style={{
                                                padding: '0.75rem 1.5rem',
                                                borderBottom: editTabActive === tab.key ? '2px solid #2563eb' : 'none',
                                                background: editTabActive === tab.key ? 'white' : 'transparent',
                                                color: editTabActive === tab.key ? '#2563eb' : '#64748b',
                                                border: 'none',
                                                cursor: 'pointer',
                                                fontSize: '0.9rem',
                                                fontWeight: editTabActive === tab.key ? '600' : '500',
                                                transition: 'all 0.2s'
                                            }}
                                        >
                                            {tab.icon} {tab.label}
                                        </button>
                                    ))}
                                </div>

                                <div className="modal-body" style={{ flex: 1, overflowY: 'auto' }}>
                                    {/* Basic Info Tab */}
                                    {editTabActive === 'basic' && (
                                        <>
                                            <div className="form-group">
                                                <label className="form-label">API 名称 <span style={{ color: '#ef4444' }}>*</span></label>
                                                <input
                                                    className="form-input"
                                                    placeholder="例如：查询凭证列表"
                                                    value={currentApi.name || ''}
                                                    onChange={e => setCurrentApi({ ...currentApi, name: e.target.value })}
                                                />
                                                <small style={{ color: '#64748b', marginTop: '0.25rem', display: 'block' }}>接口的显示名称，用于列表和文档中</small>
                                            </div>

                                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '1rem', marginBottom: '1.5rem' }}>
                                                <div className="form-group" style={{ marginBottom: 0 }}>
                                                    <label className="form-label">请求方法 <span style={{ color: '#ef4444' }}>*</span></label>
                                                    <select
                                                        className="form-select"
                                                        value={currentApi.method || 'POST'}
                                                        onChange={e => setCurrentApi({ ...currentApi, method: e.target.value as any })}
                                                    >
                                                        <option value="GET">GET</option>
                                                        <option value="POST">POST</option>
                                                        <option value="PUT">PUT</option>
                                                        <option value="DELETE">DELETE</option>
                                                        <option value="PATCH">PATCH</option>
                                                    </select>
                                                </div>
                                                <div className="form-group" style={{ marginBottom: 0 }}>
                                                    <label className="form-label">接口路径 (Path) <span style={{ color: '#ef4444' }}>*</span></label>
                                                    <div style={{ position: 'relative' }}>
                                                        <input
                                                            className="form-input"
                                                            placeholder="/api/v1/credentials"
                                                            value={currentApi.url_path || ''}
                                                            onFocus={(e) => setLastFocusedField({ ref: e.target, setter: (val) => setCurrentApi({ ...currentApi, url_path: val }) })}
                                                            onChange={e => setCurrentApi({ ...currentApi, url_path: e.target.value })}
                                                            style={{ paddingRight: '2.5rem' }}
                                                        />
                                                        <button
                                                            className="field-variable-btn"
                                                            onClick={() => setIsVariablePickerOpen(true)}
                                                            title="插入变量"
                                                        >
                                                            <Hash size={14} />
                                                        </button>
                                                    </div>
                                                </div>
                                            </div>

                                            <div className="form-group">
                                                <label className="form-label">所属凭证服务 <span style={{ color: '#ef4444' }}>*</span></label>
                                                <div style={{ display: 'flex', gap: '1rem' }}>
                                                    <div style={{ flex: 1 }}>
                                                        <select
                                                            className="form-select"
                                                            value={currentApi.service_id ?? (services[0]?.id ?? '')}
                                                            onChange={e => setCurrentApi({ ...currentApi, service_id: Number(e.target.value) })}
                                                        >
                                                            {services.map(s => (
                                                                <option key={s.id} value={s.id}>{s.display_name || s.service_name}</option>
                                                            ))}
                                                        </select>
                                                        <small style={{ color: '#64748b', marginTop: '0.25rem', display: 'block' }}>选择此接口对应的外部系统凭证</small>
                                                    </div>

                                                    <div style={{ flex: 1, borderLeft: '1px solid #e2e8f0', paddingLeft: '1rem' }}>
                                                        <label className="form-label" style={{ marginTop: '-24px' }}>业务分类</label>
                                                        <select
                                                            className="form-select"
                                                            value={currentApi.category || ''}
                                                            onChange={e => setCurrentApi({ ...currentApi, category: e.target.value })}
                                                        >
                                                            {MODAL_CATEGORY_OPTIONS.map(opt => (
                                                                <option key={opt.value} value={opt.value}>{opt.label}</option>
                                                            ))}
                                                        </select>
                                                        <small style={{ color: '#64748b', marginTop: '0.25rem', display: 'block' }}>为接口打上业务标签</small>
                                                    </div>
                                                </div>
                                            </div>

                                            <div className="form-group">
                                                <label className="form-label">简要描述</label>
                                                <input
                                                    className="form-input"
                                                    placeholder="一句话描述此接口的功能..."
                                                    value={currentApi.description || ''}
                                                    onChange={e => setCurrentApi({ ...currentApi, description: e.target.value })}
                                                />
                                            </div>

                                            <div className="form-group">
                                                <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
                                                    <input
                                                        type="checkbox"
                                                        checked={currentApi.is_active !== false}
                                                        onChange={e => setCurrentApi({ ...currentApi, is_active: e.target.checked })}
                                                    />
                                                    <span style={{ fontWeight: '500', color: '#0f172a' }}>启用此接口</span>
                                                </label>
                                            </div>
                                        </>
                                    )}

                                    {/* Request Config Tab */}
                                    {editTabActive === 'request' && (
                                        <>
                                            {/* Sub-tabs for Request Config - Minimal Style */}
                                            <div style={{
                                                display: 'flex',
                                                borderBottom: '1px solid #e2e8f0',
                                                margin: '-1.5rem -1.5rem 1.5rem -1.5rem',
                                                paddingLeft: '1.5rem',
                                                gap: '0'
                                            }}>
                                                <button
                                                    onClick={() => setRequestSubTab('headers')}
                                                    style={{
                                                        padding: '0.5rem 1rem',
                                                        borderBottom: requestSubTab === 'headers' ? '2px solid #2563eb' : 'none',
                                                        background: 'transparent',
                                                        color: requestSubTab === 'headers' ? '#2563eb' : '#94a3b8',
                                                        border: 'none',
                                                        cursor: 'pointer',
                                                        fontSize: '0.85rem',
                                                        fontWeight: requestSubTab === 'headers' ? '600' : '400',
                                                        transition: 'color 0.2s'
                                                    }}
                                                >
                                                    请求头
                                                </button>
                                                <button
                                                    onClick={() => setRequestSubTab('body')}
                                                    style={{
                                                        padding: '0.5rem 1rem',
                                                        borderBottom: requestSubTab === 'body' ? '2px solid #2563eb' : 'none',
                                                        background: 'transparent',
                                                        color: requestSubTab === 'body' ? '#2563eb' : '#94a3b8',
                                                        border: 'none',
                                                        cursor: 'pointer',
                                                        fontSize: '0.85rem',
                                                        fontWeight: requestSubTab === 'body' ? '600' : '400',
                                                        transition: 'color 0.2s'
                                                    }}
                                                >
                                                    {currentApi.method === 'GET' ? '查询参数' : '请求体'}
                                                </button>
                                            </div>

                                            {/* Headers Sub-tab - Form Style */}
                                            {requestSubTab === 'headers' && (
                                                <div>
                                                    {/* Credential Info Panel */}
                                                    {(() => {
                                                        const currentService = services.find(s => s.id === currentApi.service_id);
                                                        return currentService ? (
                                                            <div style={{
                                                                background: '#f0f9ff',
                                                                border: '1px solid #bfdbfe',
                                                                borderRadius: '6px',
                                                                padding: '1rem',
                                                                marginBottom: '1.5rem'
                                                            }}>
                                                                <div style={{ fontSize: '0.85rem', fontWeight: '600', color: '#1e40af', marginBottom: '0.75rem' }}>
                                                                    凭证服务：{currentService.display_name || currentService.service_name}
                                                                </div>
                                                                <div style={{ fontSize: '0.8rem', color: '#1e40af', marginBottom: '1rem' }}>
                                                                    认证方式：<span style={{ fontWeight: '600' }}>{currentService.auth_type?.toUpperCase()}</span>
                                                                </div>
                                                                <div style={{ marginBottom: '0.75rem' }}>
                                                                    <div style={{ fontSize: '0.8rem', color: '#475569', marginBottom: '0.5rem', fontWeight: '500' }}>
                                                                        快速插入认证Header：
                                                                    </div>
                                                                    <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                                                                        <button
                                                                            onClick={() => setHeaderRows([...headerRows, { key: 'Authorization', value: 'Bearer {access_token}' }])}
                                                                            style={{
                                                                                padding: '0.4rem 0.8rem',
                                                                                background: '#dbeafe',
                                                                                border: '1px solid #7dd3fc',
                                                                                borderRadius: '4px',
                                                                                color: '#0369a1',
                                                                                fontSize: '0.75rem',
                                                                                cursor: 'pointer',
                                                                                fontWeight: '500'
                                                                            }}
                                                                        >
                                                                            + Authorization: Bearer
                                                                        </button>
                                                                        <button
                                                                            onClick={() => setHeaderRows([...headerRows, { key: 'X-API-Key', value: '{api_key}' }])}
                                                                            style={{
                                                                                padding: '0.4rem 0.8rem',
                                                                                background: '#dbeafe',
                                                                                border: '1px solid #7dd3fc',
                                                                                borderRadius: '4px',
                                                                                color: '#0369a1',
                                                                                fontSize: '0.75rem',
                                                                                cursor: 'pointer',
                                                                                fontWeight: '500'
                                                                            }}
                                                                        >
                                                                            + X-API-Key
                                                                        </button>
                                                                        <button
                                                                            onClick={() => setHeaderRows([...headerRows, { key: 'Content-Type', value: 'application/json' }])}
                                                                            style={{
                                                                                padding: '0.4rem 0.8rem',
                                                                                background: '#dbeafe',
                                                                                border: '1px solid #7dd3fc',
                                                                                borderRadius: '4px',
                                                                                color: '#0369a1',
                                                                                fontSize: '0.75rem',
                                                                                cursor: 'pointer',
                                                                                fontWeight: '500'
                                                                            }}
                                                                        >
                                                                            + Content-Type
                                                                        </button>
                                                                    </div>
                                                                </div>
                                                                <div style={{ fontSize: '0.75rem', color: '#64748b', lineHeight: '1.4' }}>
                                                                    💡 提示：使用 <code style={{ background: '#f1f5f9', padding: '0.2rem 0.4rem', borderRadius: '3px' }}>{'{'}access_token{'}'}</code> 等变量来引用凭证库中的值
                                                                </div>
                                                            </div>
                                                        ) : (
                                                            <div style={{
                                                                background: '#fef3c7',
                                                                border: '1px solid #fcd34d',
                                                                borderRadius: '6px',
                                                                padding: '1rem',
                                                                marginBottom: '1.5rem',
                                                                fontSize: '0.85rem',
                                                                color: '#b45309'
                                                            }}>
                                                                ⚠️ 请先在"基本信息"标签页中选择所属凭证服务，以便在请求头中引用认证信息
                                                            </div>
                                                        );
                                                    })()}

                                                    <table style={{
                                                        width: '100%',
                                                        borderCollapse: 'collapse',
                                                        marginBottom: '1rem'
                                                    }}>
                                                        <thead>
                                                            <tr>
                                                                <th style={{ width: '40px', padding: '0.75rem', textAlign: 'center', fontSize: '0.85rem', color: '#64748b' }}>序号</th>
                                                                <th style={{ width: '40%', padding: '0.75rem', textAlign: 'left', fontSize: '0.85rem', color: '#64748b' }}>键名 (Key)</th>
                                                                <th style={{ width: '60%', padding: '0.75rem', textAlign: 'left', fontSize: '0.85rem', color: '#64748b' }}>键值 (Value)</th>
                                                                <th></th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            {headerRows.map((row, idx) => (
                                                                <tr key={idx} style={{ borderBottom: '1px solid #e2e8f0' }}>
                                                                    <td style={{ textAlign: 'center', color: '#94a3b8', fontSize: '0.85rem' }}>{idx + 1}</td>
                                                                    <td style={{ padding: '0.75rem', width: '40%' }}>
                                                                        <div style={{ position: 'relative' }}>
                                                                            <input
                                                                                type="text"
                                                                                placeholder="Header name"
                                                                                value={row.key}
                                                                                onFocus={(e) => setLastFocusedField({
                                                                                    ref: e.target,
                                                                                    setter: (val) => {
                                                                                        const newRows = [...headerRows];
                                                                                        newRows[idx].key = val;
                                                                                        setHeaderRows(newRows);
                                                                                    }
                                                                                })}
                                                                                onChange={e => {
                                                                                    const newRows = [...headerRows];
                                                                                    newRows[idx].key = e.target.value;
                                                                                    setHeaderRows(newRows);
                                                                                }}
                                                                                style={{
                                                                                    width: '100%',
                                                                                    padding: '0.5rem 2rem 0.5rem 0.5rem',
                                                                                    border: '1px solid #e2e8f0',
                                                                                    borderRadius: '4px',
                                                                                    outline: 'none',
                                                                                    fontSize: '0.85rem'
                                                                                }}
                                                                            />
                                                                            <button
                                                                                onClick={() => setIsVariablePickerOpen(true)}
                                                                                style={{
                                                                                    position: 'absolute',
                                                                                    right: '0.4rem',
                                                                                    top: '50%',
                                                                                    transform: 'translateY(-50%)',
                                                                                    background: 'none',
                                                                                    border: 'none',
                                                                                    color: '#94a3b8',
                                                                                    cursor: 'pointer',
                                                                                    display: 'flex',
                                                                                    alignItems: 'center'
                                                                                }}
                                                                                title="插入变量"
                                                                            >
                                                                                <Hash size={14} />
                                                                            </button>
                                                                        </div>
                                                                    </td>
                                                                    <td style={{ padding: '0.75rem', width: '60%' }}>
                                                                        <div style={{ position: 'relative' }}>
                                                                            <input
                                                                                type="text"
                                                                                placeholder="Header value"
                                                                                value={row.value}
                                                                                onFocus={(e) => setLastFocusedField({
                                                                                    ref: e.target,
                                                                                    setter: (val) => {
                                                                                        const newRows = [...headerRows];
                                                                                        newRows[idx].value = val;
                                                                                        setHeaderRows(newRows);
                                                                                    }
                                                                                })}
                                                                                onChange={e => {
                                                                                    const newRows = [...headerRows];
                                                                                    newRows[idx].value = e.target.value;
                                                                                    setHeaderRows(newRows);
                                                                                }}
                                                                                style={{
                                                                                    width: '100%',
                                                                                    padding: '0.5rem 2rem 0.5rem 0.5rem',
                                                                                    border: '1px solid #e2e8f0',
                                                                                    borderRadius: '4px',
                                                                                    outline: 'none',
                                                                                    fontSize: '0.85rem'
                                                                                }}
                                                                            />
                                                                            <button
                                                                                onClick={() => setIsVariablePickerOpen(true)}
                                                                                style={{
                                                                                    position: 'absolute',
                                                                                    right: '0.4rem',
                                                                                    top: '50%',
                                                                                    transform: 'translateY(-50%)',
                                                                                    background: 'none',
                                                                                    border: 'none',
                                                                                    color: '#94a3b8',
                                                                                    cursor: 'pointer',
                                                                                    display: 'flex',
                                                                                    alignItems: 'center'
                                                                                }}
                                                                                title="插入变量"
                                                                            >
                                                                                <Hash size={14} />
                                                                            </button>
                                                                        </div>
                                                                    </td>
                                                                    <td style={{ padding: '0.75rem 0', textAlign: 'right', width: '40px' }}>
                                                                        <button
                                                                            onClick={() => {
                                                                                const newRows = headerRows.filter((_, i) => i !== idx);
                                                                                setHeaderRows(newRows);
                                                                            }}
                                                                            style={{
                                                                                background: 'none',
                                                                                border: 'none',
                                                                                color: '#ef4444',
                                                                                cursor: 'pointer',
                                                                                fontSize: '1.2rem',
                                                                                padding: '0'
                                                                            }}
                                                                        >
                                                                            ✕
                                                                        </button>
                                                                    </td>
                                                                </tr>
                                                            ))}
                                                        </tbody>
                                                    </table>
                                                    <button
                                                        onClick={() => setHeaderRows([...headerRows, { key: '', value: '' }])}
                                                        style={{
                                                            padding: '0.5rem 1rem',
                                                            background: '#f1f5f9',
                                                            border: '1px dashed #cbd5e1',
                                                            borderRadius: '4px',
                                                            color: '#2563eb',
                                                            cursor: 'pointer',
                                                            fontSize: '0.85rem',
                                                            fontWeight: '500'
                                                        }}
                                                    >
                                                        + 添加请求头
                                                    </button>
                                                </div>
                                            )}

                                            {/* Body/Query Sub-tab */}
                                            {requestSubTab === 'body' && (
                                                <div className="form-group">
                                                    {currentApi.method === 'GET' ? (
                                                        <div style={{ background: '#f8fafc', padding: '1.25rem', borderRadius: '8px', border: '1px solid #e2e8f0' }}>
                                                            <div style={{ fontSize: '0.85rem', color: '#64748b', marginBottom: '1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                                <HelpCircle size={14} />
                                                                GET 接口的查询参数将自动拼接到 URL 尾部（例如：?key=value）
                                                            </div>
                                                            <table style={{ width: '100%', borderCollapse: 'collapse', marginBottom: '1rem' }}>
                                                                <thead>
                                                                    <tr style={{ borderBottom: '1px solid #e2e8f0' }}>
                                                                        <th style={{ width: '40px', padding: '0.75rem', textAlign: 'center', fontSize: '0.85rem', color: '#64748b' }}>序号</th>
                                                                        <th style={{ padding: '0.75rem', textAlign: 'left', fontSize: '0.85rem', color: '#64748b' }}>参数名 (Key)</th>
                                                                        <th style={{ padding: '0.75rem', textAlign: 'left', fontSize: '0.85rem', color: '#64748b' }}>参数值 (Value)</th>
                                                                        <th style={{ width: '40px', padding: '0.75rem' }}></th>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {queryRows.map((row, idx) => (
                                                                        <tr key={idx} style={{ borderBottom: '1px solid #f1f5f9' }}>
                                                                            <td style={{ textAlign: 'center', fontSize: '0.8rem', color: '#94a3b8' }}>{idx + 1}</td>
                                                                            <td style={{ padding: '0.5rem' }}>
                                                                                <div style={{ position: 'relative' }}>
                                                                                    <input
                                                                                        className="form-input"
                                                                                        style={{ fontSize: '0.85rem', padding: '0.4rem 2rem 0.4rem 0.6rem' }}
                                                                                        placeholder="参数名"
                                                                                        value={row.key}
                                                                                        onFocus={(e) => setLastFocusedField({
                                                                                            ref: e.target,
                                                                                            setter: (val) => {
                                                                                                const newRows = [...queryRows];
                                                                                                newRows[idx].key = val;
                                                                                                setQueryRows(newRows);
                                                                                            }
                                                                                        })}
                                                                                        onChange={e => {
                                                                                            const newRows = [...queryRows];
                                                                                            newRows[idx].key = e.target.value;
                                                                                            setQueryRows(newRows);
                                                                                        }}
                                                                                    />
                                                                                    <button
                                                                                        onClick={() => setIsVariablePickerOpen(true)}
                                                                                        style={{
                                                                                            position: 'absolute',
                                                                                            right: '0.4rem',
                                                                                            top: '50%',
                                                                                            transform: 'translateY(-50%)',
                                                                                            background: 'none',
                                                                                            border: 'none',
                                                                                            color: '#94a3b8',
                                                                                            cursor: 'pointer',
                                                                                            display: 'flex',
                                                                                            alignItems: 'center'
                                                                                        }}
                                                                                        title="插入变量"
                                                                                    >
                                                                                        <Hash size={14} />
                                                                                    </button>
                                                                                </div>
                                                                            </td>
                                                                            <td style={{ padding: '0.5rem' }}>
                                                                                <div style={{ position: 'relative' }}>
                                                                                    <input
                                                                                        className="form-input"
                                                                                        style={{ fontSize: '0.85rem', padding: '0.4rem 2rem 0.4rem 0.6rem' }}
                                                                                        placeholder="参数值"
                                                                                        value={row.value}
                                                                                        onFocus={(e) => setLastFocusedField({
                                                                                            ref: e.target,
                                                                                            setter: (val) => {
                                                                                                const newRows = [...queryRows];
                                                                                                newRows[idx].value = val;
                                                                                                setQueryRows(newRows);
                                                                                            }
                                                                                        })}
                                                                                        onChange={e => {
                                                                                            const newRows = [...queryRows];
                                                                                            newRows[idx].value = e.target.value;
                                                                                            setQueryRows(newRows);
                                                                                        }}
                                                                                    />
                                                                                    <button
                                                                                        onClick={() => setIsVariablePickerOpen(true)}
                                                                                        style={{
                                                                                            position: 'absolute',
                                                                                            right: '0.4rem',
                                                                                            top: '50%',
                                                                                            transform: 'translateY(-50%)',
                                                                                            background: 'none',
                                                                                            border: 'none',
                                                                                            color: '#94a3b8',
                                                                                            cursor: 'pointer',
                                                                                            display: 'flex',
                                                                                            alignItems: 'center'
                                                                                        }}
                                                                                        title="插入变量"
                                                                                    >
                                                                                        <Hash size={14} />
                                                                                    </button>
                                                                                </div>
                                                                            </td>
                                                                            <td style={{ padding: '0.5rem', textAlign: 'right' }}>
                                                                                <button
                                                                                    onClick={() => setQueryRows(queryRows.filter((_, i) => i !== idx))}
                                                                                    style={{ color: '#ef4444', background: 'none', border: 'none', cursor: 'pointer', padding: '0.4rem' }}
                                                                                >
                                                                                    ✕
                                                                                </button>
                                                                            </td>
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </table>
                                                            <button
                                                                onClick={() => setQueryRows([...queryRows, { key: '', value: '' }])}
                                                                style={{ padding: '0.5rem 1rem', background: '#f1f5f9', border: '1px dashed #cbd5e1', borderRadius: '4px', color: '#2563eb', cursor: 'pointer', fontSize: '0.85rem', fontWeight: '500' }}
                                                            >
                                                                + 添加参数
                                                            </button>
                                                        </div>
                                                    ) : (
                                                        <>
                                                            <JsonEditor
                                                                value={currentApi.request_body || ''}
                                                                onChange={val => setCurrentApi({ ...currentApi, request_body: val })}
                                                                placeholder='{"param1": "value1","param2": "value2"}'
                                                                height="250px"
                                                            />
                                                            <small style={{ color: '#64748b', marginTop: '0.5rem', display: 'block' }}>
                                                                POST/PUT/PATCH 请求的示例 JSON 格式。点击工具栏中的"格式化"按钮可美化代码。
                                                            </small>
                                                        </>
                                                    )}
                                                </div>
                                            )}
                                        </>
                                    )}

                                    {/* Response Example Tab */}
                                    {editTabActive === 'response' && (
                                        <>
                                            <div className="form-group">
                                                <label className="form-label" style={{ marginBottom: '0.75rem', display: 'block' }}>响应示例 (Response)</label>
                                                <JsonEditor
                                                    value={currentApi.response_example || ''}
                                                    onChange={val => setCurrentApi({ ...currentApi, response_example: val })}
                                                    placeholder='{"success": true,"data": [],"message": "操作成功"}'
                                                    height="300px"
                                                />
                                                <small style={{ color: '#64748b', marginTop: '0.5rem', display: 'block' }}>接口成功响应时的 JSON 示例，便于前端集成。支持全屏编辑模式。</small>
                                            </div>
                                        </>
                                    )}

                                    {/* Documentation Tab */}
                                    {editTabActive === 'notes' && (
                                        <>
                                            <div className="form-group" style={{ position: 'relative' }}>
                                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                                                    <label className="form-label" style={{ marginBottom: 0 }}>详细文档说明</label>
                                                    <button
                                                        className="json-action-btn"
                                                        onClick={() => setIsVariablePickerOpen(true)}
                                                        style={{ padding: '0.2rem 0.5rem', background: '#f1f5f9' }}
                                                    >
                                                        <Hash size={12} /> 插入变量
                                                    </button>
                                                </div>
                                                <textarea
                                                    className="form-textarea"
                                                    placeholder="详细说明接口的用途、参数说明、错误码、调用限制、版本变更等..."
                                                    value={currentApi.notes || ''}
                                                    onFocus={(e) => setLastFocusedField({ ref: e.target, setter: (val) => setCurrentApi({ ...currentApi, notes: val }) })}
                                                    onChange={e => setCurrentApi({ ...currentApi, notes: e.target.value })}
                                                    style={{ minHeight: '250px' }}
                                                />
                                                <small style={{ color: '#64748b', marginTop: '0.25rem', display: 'block' }}>支持 Markdown 格式，用于生成 API 文档</small>
                                            </div>
                                        </>
                                    )}

                                    {/* Test Tab */}
                                    {editTabActive === 'test' && (
                                        <div>
                                            <div style={{
                                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                                marginBottom: '1.5rem'
                                            }}>
                                                <div>
                                                    <h4 style={{ margin: 0, color: '#0f172a', fontSize: '1rem' }}>接口测试预览</h4>
                                                    <p style={{ margin: '0.25rem 0 0', color: '#64748b', fontSize: '0.85rem' }}>
                                                        发送真实请求并查看响应结果
                                                    </p>
                                                </div>
                                                <button
                                                    onClick={handleTestApi}
                                                    disabled={isTesting}
                                                    style={{
                                                        padding: '0.6rem 1.2rem',
                                                        background: isTesting ? '#94a3b8' : '#2563eb',
                                                        color: 'white',
                                                        border: 'none',
                                                        borderRadius: '8px',
                                                        cursor: isTesting ? 'not-allowed' : 'pointer',
                                                        display: 'flex', alignItems: 'center', gap: '0.5rem',
                                                        fontSize: '0.9rem', fontWeight: '600'
                                                    }}
                                                >
                                                    <Play size={16} /> {isTesting ? '正在测试...' : '发送请求'}
                                                </button>
                                            </div>

                                            {/* Request Summary */}
                                            <div style={{
                                                background: '#f8fafc', border: '1px solid #e2e8f0',
                                                borderRadius: '8px', padding: '1rem', marginBottom: '1.5rem'
                                            }}>
                                                <div style={{ fontSize: '0.8rem', color: '#64748b', marginBottom: '0.5rem' }}>请求概要</div>
                                                <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
                                                    <span style={{
                                                        padding: '0.2rem 0.6rem', borderRadius: '4px',
                                                        background: '#dbeafe', color: '#1d4ed8',
                                                        fontSize: '0.8rem', fontWeight: '700'
                                                    }}>
                                                        {currentApi.method || 'POST'}
                                                    </span>
                                                    <code style={{ fontSize: '0.85rem', color: '#334155', wordBreak: 'break-all' }}>
                                                        {currentApi.url_path || '未设置接口路径'}
                                                    </code>
                                                </div>
                                            </div>

                                            {/* Test Result */}
                                            {isTesting && (
                                                <div style={{
                                                    display: 'flex', alignItems: 'center', gap: '0.75rem',
                                                    padding: '2rem', justifyContent: 'center', color: '#64748b'
                                                }}>
                                                    <div className="loader small"></div>
                                                    <span>正在发送请求并等待响应...</span>
                                                </div>
                                            )}

                                            {testResult && !isTesting && (
                                                <div>
                                                    <div style={{
                                                        display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem',
                                                        marginBottom: '1.5rem'
                                                    }}>
                                                        <div style={{
                                                            padding: '1rem', borderRadius: '8px',
                                                            background: testResult.success ? '#f0fdf4' : '#fef2f2',
                                                            border: `1px solid ${testResult.success ? '#bbf7d0' : '#fecaca'}`
                                                        }}>
                                                            <div style={{ fontSize: '0.75rem', color: '#64748b', marginBottom: '0.5rem' }}>响应状态</div>
                                                            <div style={{
                                                                display: 'flex', alignItems: 'center', gap: '0.5rem',
                                                                fontSize: '1.1rem', fontWeight: '700',
                                                                color: testResult.success ? '#16a34a' : '#dc2626'
                                                            }}>
                                                                {testResult.success
                                                                    ? <CheckCircle size={18} />
                                                                    : <AlertTriangle size={18} />}
                                                                {testResult.status_code || 'Error'}
                                                            </div>
                                                        </div>
                                                        <div style={{
                                                            padding: '1rem', borderRadius: '8px',
                                                            background: '#f8fafc', border: '1px solid #e2e8f0'
                                                        }}>
                                                            <div style={{ fontSize: '0.75rem', color: '#64748b', marginBottom: '0.5rem' }}>响应耗时</div>
                                                            <div style={{
                                                                display: 'flex', alignItems: 'center', gap: '0.5rem',
                                                                fontSize: '1.1rem', fontWeight: '700', color: '#334155'
                                                            }}>
                                                                <Clock size={18} />
                                                                {testResult.duration_ms || 0} ms
                                                            </div>
                                                        </div>
                                                    </div>

                                                    {!testResult.success && testResult.error && (
                                                        <div style={{
                                                            padding: '1rem', borderRadius: '8px',
                                                            background: '#fef2f2', border: '1px solid #fecaca',
                                                            color: '#dc2626', marginBottom: '1.5rem',
                                                            display: 'flex', alignItems: 'center', gap: '0.75rem',
                                                            fontSize: '0.9rem'
                                                        }}>
                                                            <AlertTriangle size={18} />
                                                            {testResult.error}
                                                        </div>
                                                    )}

                                                    <div>
                                                        <div style={{
                                                            fontSize: '0.85rem', fontWeight: '600', color: '#334155',
                                                            marginBottom: '0.75rem',
                                                            display: 'flex', alignItems: 'center', gap: '0.5rem'
                                                        }}>
                                                            <Activity size={16} /> 响应正文 (JSON)
                                                        </div>
                                                        <pre style={{
                                                            background: '#1e293b', color: '#e2e8f0',
                                                            padding: '1.25rem', borderRadius: '8px',
                                                            fontSize: '0.8rem', lineHeight: '1.6',
                                                            maxHeight: '300px', overflow: 'auto',
                                                            fontFamily: 'monospace', whiteSpace: 'pre-wrap',
                                                            wordBreak: 'break-all'
                                                        }}>
                                                            {JSON.stringify(testResult.data, null, 2)}
                                                        </pre>
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    )}
                                </div>

                                <div className="modal-footer">
                                    <button
                                        className="button button-secondary"
                                        onClick={() => { setIsEditing(false); setEditTabActive('basic'); setRequestSubTab('headers'); }}
                                    >
                                        取消
                                    </button>
                                    <button
                                        className="button button-primary"
                                        onClick={handleSaveApi}
                                    >
                                        💾 保存接口配置
                                    </button>
                                </div>

                                <VariablePicker
                                    isOpen={isVariablePickerOpen}
                                    onClose={() => setIsVariablePickerOpen(false)}
                                    onSelect={handleVariableSelect}
                                />
                            </div>
                        </div>
                    )}

                    {/* Code Snippet Modal */}
                    {showCode && showCode._service && (
                        <div className="modal-overlay">
                            <div className="modal-content" style={{ maxWidth: '700px' }}>
                                <div className="modal-header">
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flex: 1 }}>
                                        <div style={{
                                            width: '40px',
                                            height: '40px',
                                            background: 'linear-gradient(135deg, var(--api-primary-light) 0%, #bfdbfe 100%)',
                                            borderRadius: '8px',
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent: 'center',
                                            color: '#2563eb'
                                        }}>
                                            <Terminal size={20} />
                                        </div>
                                        <div>
                                            <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#0f172a' }}>
                                                调用代码：{showCode.name}
                                            </div>
                                            <div style={{ fontSize: '0.75rem', color: '#64748b', fontFamily: 'monospace', marginTop: '0.25rem' }}>
                                                {showCode.method} {showCode.url_path}
                                            </div>
                                        </div>
                                    </div>
                                    <button
                                        onClick={() => setShowCode(null)}
                                        className="modal-close-btn"
                                        title="关闭"
                                    >
                                        <X size={20} />
                                    </button>
                                </div>
                                <div style={{ position: 'relative' }}>
                                    <button
                                        className="code-copy-btn"
                                        onClick={() => copyToClipboard(generateCodeSnippet(showCode, showCode._service!))}
                                        title={copied ? '已复制' : '复制代码'}
                                    >
                                        {copied ? <Check size={16} className="text-emerald-400" /> : <Copy size={16} />}
                                    </button>
                                    <div className="code-preview">
                                        {generateCodeSnippet(showCode, showCode._service!)}
                                    </div>
                                </div>
                                <div style={{
                                    background: 'rgba(0, 0, 0, 0.3)',
                                    borderTop: '1px solid rgba(0, 0, 0, 0.1)',
                                    padding: '1rem 1.5rem',
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center'
                                }}>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                        <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#10b981' }}></span>
                                        <span style={{ fontSize: '0.75rem', color: '#c9d1d9', textTransform: 'uppercase', letterSpacing: '0.05em', fontWeight: '600' }}>
                                            Python Requests Template
                                        </span>
                                    </div>
                                    <button
                                        className="button button-secondary"
                                        style={{ borderColor: 'rgba(255, 255, 255, 0.2)', background: 'transparent', color: '#c9d1d9' }}
                                        onClick={() => setShowCode(null)}
                                    >
                                        关闭预览
                                    </button>
                                </div>
                            </div>
                        </div>
                    )}

                {/* Import Modal */}
                {showImport && (
                        <div className="modal-overlay">
                            <div className="modal-content" style={{ maxWidth: '500px' }}>
                                <div className="modal-header">
                                    <h3 className="modal-title">批量导入 API 定义</h3>
                                    <button
                                        onClick={() => setShowImport(false)}
                                        className="modal-close-btn"
                                        title="关闭"
                                    >
                                        <X size={20} />
                                    </button>
                                </div>
                                <div className="modal-body">
                                    <div style={{
                                        background: '#fef3c7',
                                        border: '1px solid #fcd34d',
                                        borderRadius: '8px',
                                        padding: '0.75rem 1rem',
                                        display: 'flex',
                                        gap: '0.75rem',
                                        alignItems: 'flex-start',
                                        marginBottom: '1.5rem',
                                        color: '#b45309',
                                        fontSize: '0.875rem'
                                    }}>
                                        <Info size={18} style={{ flexShrink: 0, marginTop: '0.125rem' }} />
                                        <span>支持 OpenAPI / Swagger 2.0/3.0 JSON 格式。</span>
                                    </div>
                                    <div className="form-group">
                                        <label className="form-label">目标凭证服务</label>
                                        <select
                                            className="form-select"
                                            value={importTargetServiceId ?? (services[0]?.id ?? '')}
                                            onChange={e => setImportTargetServiceId(Number(e.target.value))}
                                        >
                                            {services.map(s => (
                                                <option key={s.id} value={s.id}>{s.display_name || s.service_name}</option>
                                            ))}
                                        </select>
                                    </div>

                                    <div className="form-group">
                                        <label className="form-label">JSON 定义内容</label>
                                        <textarea
                                            className="form-textarea"
                                            placeholder='{ "openapi": "3.0.0", "paths": { ... } }'
                                            value={importContent}
                                            onChange={e => setImportContent(e.target.value)}
                                            style={{ minHeight: '300px', fontFamily: 'monospace', fontSize: '0.8rem' }}
                                        />
                                    </div>
                                </div>
                                <div className="modal-footer">
                                    <button
                                        className="button button-secondary"
                                        onClick={() => setShowImport(false)}
                                    >
                                        取消
                                    </button>
                                    <button
                                        className="button button-primary"
                                        onClick={handleImportApis}
                                        disabled={!importContent}
                                    >
                                        开始解析并导入
                                    </button>
                                </div>
                            </div>
                        </div>
                )}
        </div>
    );
};

export default APIManager;
