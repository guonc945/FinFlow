
import { useState, useEffect } from 'react';
import {
    X, Save, Hash,
    Server, ArrowRight, Layout,
    Globe, Activity, Info, Settings,
    FileText, Plus, Play, CheckCircle, AlertTriangle, Clock
} from 'lucide-react';
import axios from 'axios';
import JsonEditor from '../../components/data/JsonEditor';
import { useToast, ToastContainer } from '../../components/Toast';
import './ArchiveAPIs.css';

interface ExternalService {
    id: number;
    service_name: string;
    display_name: string;
    base_url: string;
    auth_type: string;
}

const ArchiveAPIs = () => {
    const [archiveTypes, setArchiveTypes] = useState<any[]>([]);
    const [selectedArchive, setSelectedArchive] = useState('');
    const [services, setServices] = useState<ExternalService[]>([]);
    const [loading, setLoading] = useState(false);
    const [isAddModalOpen, setIsAddModalOpen] = useState(false);
    const [newType, setNewType] = useState({ key: '', label: '', icon: 'FileText' });

    // Config Form State
    const [config, setConfig] = useState<any>({});
    const { toasts, showToast, removeToast } = useToast();
    const [activeTab, setActiveTab] = useState<'basic' | 'request'>('basic');
    const [requestSubTab, setRequestSubTab] = useState<'headers' | 'body'>('headers');
    const [headerRows, setHeaderRows] = useState<Array<{ key: string, value: string }>>([]);

    // Test Statistics
    const [isTesting, setIsTesting] = useState(false);
    const [testResult, setTestResult] = useState<any>(null);
    const [showTestPanel, setShowTestPanel] = useState(false);

    useEffect(() => {
        fetchInitialData();
    }, []);

    const fetchInitialData = async () => {
        try {
            const [typesRes, servicesRes] = await Promise.all([
                axios.get(`${import.meta.env.VITE_API_BASE_URL}/archives/types`),
                axios.get(`${import.meta.env.VITE_API_BASE_URL}/external/services`)
            ]);
            setArchiveTypes(typesRes.data);
            setServices(servicesRes.data);
            if (typesRes.data.length > 0) {
                setSelectedArchive(typesRes.data[0].key);
            }
        } catch (err) { console.error(err); }
    };

    useEffect(() => {
        if (selectedArchive) {
            fetchConfig(selectedArchive);
        }
    }, [selectedArchive]);

    const fetchConfig = async (archiveKey: string) => {
        setLoading(true);
        const endpoint = `${import.meta.env.VITE_API_BASE_URL}/archives/config/${archiveKey}`;

        try {
            const res = await axios.get(endpoint);
            const data = res.data;
            if (Object.keys(data).length > 0) {
                setConfig(data);
                if (data.request_headers) {
                    try {
                        const headers = typeof data.request_headers === 'string'
                            ? JSON.parse(data.request_headers)
                            : data.request_headers;
                        const rows = Object.entries(headers).map(([k, v]) => ({ key: k, value: String(v) }));
                        setHeaderRows(rows);
                    } catch { setHeaderRows([]); }
                } else {
                    setHeaderRows([]);
                }
            } else {
                setConfig({
                    method: 'POST',
                    url_path: '',
                    request_body: '{}'
                });
                setHeaderRows([]);
            }
        } catch (err) {
            console.error(err);
        } finally {
            setTimeout(() => setLoading(false), 300);
        }
    };

    const handleTestConfig = async () => {
        setIsTesting(true);
        setShowTestPanel(true);
        setTestResult(null);

        try {
            const headersObj: Record<string, string> = {};
            headerRows.forEach(row => {
                if (row.key) headersObj[row.key] = row.value;
            });

            const payload = {
                ...config,
                request_headers: headersObj
            };

            const res = await axios.post(`${import.meta.env.VITE_API_BASE_URL}/archives/test`, payload);
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

    const handleSaveConfig = async () => {
        const endpoint = `${import.meta.env.VITE_API_BASE_URL}/archives/config/${selectedArchive}`;

        try {
            const headersObj: Record<string, string> = {};
            headerRows.forEach(row => {
                if (row.key) headersObj[row.key] = row.value;
            });

            const payload = {
                ...config,
                request_headers: headersObj
            };

            await axios.post(endpoint, payload);
            showToast('success', '保存成功', '接口配置已同步至云端');
        } catch (err) {
            showToast('error', '保存失败', '请检查网络连接或参数合规性');
        }
    };

    const handleAddType = async () => {
        if (!newType.key || !newType.label) return;

        // Key validation
        if (!/^[a-z0-9-]+$/.test(newType.key)) {
            showToast('error', '格式错误', 'Key 只能包含小写字母、数字和连字符');
            return;
        }

        const updatedTypes = [...archiveTypes, newType];
        try {
            await axios.post(`${import.meta.env.VITE_API_BASE_URL}/archives/types`, updatedTypes);
            setArchiveTypes(updatedTypes);
            setSelectedArchive(newType.key);
            setIsAddModalOpen(false);
            setNewType({ key: '', label: '', icon: 'FileText' });
            showToast('success', '创建成功', `已添加新档案类型: ${newType.label}`);
        } catch (err) {
            showToast('error', '添加失败', '保存新档案类型时发生错误');
        }
    };

    const renderIcon = (iconName: string) => {
        switch (iconName) {
            case 'FileText': return <FileText size={18} />;
            case 'Layout': return <Layout size={18} />;
            case 'Settings': return <Settings size={18} />;
            case 'Globe': return <Globe size={18} />;
            default: return <FileText size={18} />;
        }
    };

    return (
        <div className="archive-apis-page">
                        <div className="main-container">
                {/* Left Sidebar: Archive Types */}
                <div className="sidebar">
                    <div className="sidebar-header">
                        <Activity size={16} />
                        <span>档案类型列表</span>
                        <button
                            className="add-type-btn"
                            title="添加新接口"
                            onClick={() => setIsAddModalOpen(true)}
                        >
                            <Plus size={16} />
                        </button>
                    </div>
                    <div className="sidebar-content">
                        {archiveTypes.map(type => (
                            <button
                                key={type.key}
                                onClick={() => setSelectedArchive(type.key)}
                                className={`sidebar-item ${selectedArchive === type.key ? 'active' : ''}`}
                            >
                                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                    {renderIcon(type.icon)}
                                    <span>{type.label}</span>
                                </div>
                                {selectedArchive === type.key ? <div className="active-indicator" /> : <ArrowRight size={14} opacity={0.3} />}
                            </button>
                        ))}
                    </div>
                </div>

                {/* Right Content: Config Form */}
                <div className="content-area">
                    {/* Toolbar */}
                    <div className="toolbar">
                        <div className="toolbar-left">
                            <h2 className="toolbar-title">
                                {archiveTypes.find(t => t.key === selectedArchive)?.label}
                                <span className="toolbar-separator">/</span>
                                <span className="toolbar-context">接口配置</span>
                            </h2>
                        </div>
                        <div className="toolbar-actions">
                            <button
                                onClick={handleTestConfig}
                                className="btn-test"
                                disabled={isTesting}
                            >
                                <Play size={16} /> {isTesting ? '正在测试...' : '测试预览'}
                            </button>
                            <button
                                onClick={handleSaveConfig}
                                className="btn-save"
                            >
                                <Save size={18} /> 保存配置
                            </button>
                        </div>
                    </div>

                    {/* Tabs */}
                    <div className="tabs-nav">
                        <button
                            onClick={() => setActiveTab('basic')}
                            className={`tab-button ${activeTab === 'basic' ? 'active' : ''}`}
                        >
                            <Settings size={18} />
                            连接配置
                        </button>
                        <button
                            onClick={() => setActiveTab('request')}
                            className={`tab-button ${activeTab === 'request' ? 'active' : ''}`}
                        >
                            <Globe size={18} />
                            报文定义
                        </button>
                    </div>

                    {/* Form Content */}
                    <div className="form-content">
                        {loading ? (
                            <div className="loading-container">
                                <div className="loader"></div>
                                <p>同步云端配置中...</p>
                            </div>
                        ) : (
                            <div className="form-container">
                                {activeTab === 'basic' && (
                                    <div className="form-sections animate-fade-in">
                                        <div className="section-card">
                                            <div className="section-header">
                                                <div className="section-icon"><Server size={18} /></div>
                                                <div className="section-info">
                                                    <h3 className="section-title">外部集成服务</h3>
                                                    <p className="section-desc">选择所属外部系统以继承认证配置</p>
                                                </div>
                                            </div>

                                            <div className="form-field">
                                                <label className="form-label">目标服务 (Service Provider)</label>
                                                <div className="select-with-status">
                                                    <select
                                                        className="form-select"
                                                        value={config.service_id || ''}
                                                        onChange={e => setConfig({ ...config, service_id: Number(e.target.value) })}
                                                    >
                                                        <option value="">-- 未关联任何服务 --</option>
                                                        {services.map(s => (
                                                            <option key={s.id} value={s.id}>{s.display_name || s.service_name}</option>
                                                        ))}
                                                    </select>
                                                    {config.service_id && (
                                                        <div className="status-badge connected">已关联</div>
                                                    )}
                                                </div>

                                            </div>
                                        </div>

                                        <div className="section-card">
                                            <div className="section-header">
                                                <div className="section-icon"><Layout size={18} /></div>
                                                <div className="section-info">
                                                    <h3 className="section-title">接口地址配置</h3>
                                                    <p className="section-desc">设定完整的 API 请求地址与请求动作</p>
                                                </div>
                                            </div>

                                            <div className="endpoint-compact-row">
                                                <div className="method-picker-col">
                                                    <label className="form-label">动作</label>
                                                    <div className="method-selector">
                                                        {['GET', 'POST', 'PUT'].map(m => (
                                                            <button
                                                                key={m}
                                                                className={`method-btn ${config.method === m || (!config.method && m === 'POST') ? 'active' : ''}`}
                                                                onClick={() => setConfig({ ...config, method: m })}
                                                            >
                                                                {m}
                                                            </button>
                                                        ))}
                                                    </div>
                                                </div>
                                                <div className="url-input-col">
                                                    <label className="form-label">请求地址</label>
                                                    <input
                                                        className="form-input"
                                                        placeholder="例如: https://api.example.com/v1/data"
                                                        value={config.url || config.url_path || ''}
                                                        onChange={e => setConfig({ ...config, url: e.target.value, url_path: undefined })}
                                                    />
                                                </div>
                                            </div>

                                            <div className="compact-url-preview">
                                                <div className="preview-label">
                                                    <div className={`status-dot ${config.url ? 'active' : ''}`} />
                                                    请求就绪
                                                </div>
                                                <div className="preview-url-text">
                                                    {config.url || '等待输入接口地址...'}
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                )}

                                {activeTab === 'request' && (
                                    <div className="form-sections animate-fade-in">
                                        <div className="section-tabs-header">
                                            <div className="sub-tabs">
                                                <button
                                                    onClick={() => setRequestSubTab('headers')}
                                                    className={`sub-tab-button ${requestSubTab === 'headers' ? 'active' : ''}`}
                                                >
                                                    <Hash size={14} /> 请求头 (Headers)
                                                </button>
                                                <button
                                                    onClick={() => setRequestSubTab('body')}
                                                    className={`sub-tab-button ${requestSubTab === 'body' ? 'active' : ''}`}
                                                >
                                                    <FileText size={14} /> 请求体 (JSON Body)
                                                </button>
                                            </div>
                                        </div>

                                        {requestSubTab === 'headers' && (
                                            <div className="section-card no-padding">
                                                <div className="info-strip">
                                                    <Info size={16} />
                                                    <span>支持动态宏: <code>{'{access_token}'}</code> 系统将自动注入已授权令牌</span>
                                                </div>

                                                <div className="params-table-container">
                                                    <table className="params-table">
                                                        <thead>
                                                            <tr>
                                                                <th className="cell-key" style={{ width: '60px', textAlign: 'center' }}>序号</th>
                                                                <th className="cell-key">键名 (Key)</th>
                                                                <th className="cell-value">键值 (Value)</th>
                                                                <th className="cell-action"></th>
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            {headerRows.map((row, idx) => (
                                                                <tr key={idx} className="table-row">
                                                                    <td style={{ textAlign: 'center', color: '#94a3b8' }}>{idx + 1}</td>
                                                                    <td>
                                                                        <input
                                                                            className="table-input"
                                                                            value={row.key}
                                                                            placeholder="例如: Content-Type"
                                                                            onChange={e => {
                                                                                const newRows = [...headerRows];
                                                                                newRows[idx].key = e.target.value;
                                                                                setHeaderRows(newRows);
                                                                            }}
                                                                        />
                                                                    </td>
                                                                    <td>
                                                                        <input
                                                                            className="table-input"
                                                                            value={row.value}
                                                                            placeholder="例如: application/json"
                                                                            onChange={e => {
                                                                                const newRows = [...headerRows];
                                                                                newRows[idx].value = e.target.value;
                                                                                setHeaderRows(newRows);
                                                                            }}
                                                                        />
                                                                    </td>
                                                                    <td>
                                                                        <button
                                                                            onClick={() => setHeaderRows(headerRows.filter((_, i) => i !== idx))}
                                                                            className="row-delete-btn"
                                                                        >
                                                                            <X size={16} />
                                                                        </button>
                                                                    </td>
                                                                </tr>
                                                            ))}
                                                        </tbody>
                                                    </table>

                                                    <div className="table-footer">
                                                        <button
                                                            onClick={() => setHeaderRows([...headerRows, { key: '', value: '' }])}
                                                            className="btn-add-row"
                                                        >
                                                            <Plus size={14} /> 添加自定义 Header
                                                        </button>
                                                    </div>
                                                </div>
                                            </div>
                                        )}

                                        {requestSubTab === 'body' && (
                                            <div className="section-card no-padding">
                                                <div className="info-strip">
                                                    <Info size={16} />
                                                    <span>请使用标准 JSON 格式定义请求体负载</span>
                                                </div>
                                                <div className="json-editor-container">
                                                    <JsonEditor
                                                        value={config.request_body || '{}'}
                                                        height="350px"
                                                        onChange={(val: string) => setConfig({ ...config, request_body: val })}
                                                    />
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                )}

                                {/* Test Result Panel */}
                                {showTestPanel && (
                                    <div className="test-result-panel animate-slide-up">
                                        <div className="panel-header">
                                            <div className="panel-title">
                                                <Activity size={18} />
                                                <h4>接口测试结果</h4>
                                            </div>
                                            <button className="panel-close" onClick={() => setShowTestPanel(false)}>
                                                <X size={18} />
                                            </button>
                                        </div>

                                        <div className="panel-content">
                                            {isTesting ? (
                                                <div className="test-loading">
                                                    <div className="loader small"></div>
                                                    <span>正在发送请求并捕获响应...</span>
                                                </div>
                                            ) : testResult ? (
                                                <div className="test-detail">
                                                    <div className="test-stats-grid">
                                                        <div className={`stat-card ${testResult.success ? 'success' : 'error'}`}>
                                                            <div className="stat-label">响应状态</div>
                                                            <div className="stat-value">
                                                                {testResult.success ? <CheckCircle size={14} /> : <AlertTriangle size={14} />}
                                                                {testResult.status_code || 'Err'}
                                                            </div>
                                                        </div>
                                                        <div className="stat-card">
                                                            <div className="stat-label">响应耗时</div>
                                                            <div className="stat-value">
                                                                <Clock size={14} />
                                                                {testResult.duration_ms || 0} ms
                                                            </div>
                                                        </div>
                                                    </div>

                                                    {!testResult.success && testResult.error && (
                                                        <div className="test-error-msg">
                                                            <AlertTriangle size={16} />
                                                            {testResult.error}
                                                        </div>
                                                    )}

                                                    <div className="test-response-view">
                                                        <div className="view-title">响应正文 (JSON)</div>
                                                        <pre className="response-pre">
                                                            {JSON.stringify(testResult.data, null, 2)}
                                                        </pre>
                                                    </div>
                                                </div>
                                            ) : null}
                                        </div>
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* Add New Archive Type Modal */}
            {isAddModalOpen && (
                <div className="modal-overlay animate-fade-in">
                    <div className="modal-content">
                        <div className="modal-header">
                            <h3 className="modal-title">创建新档案接口</h3>
                            <button className="modal-close" onClick={() => setIsAddModalOpen(false)}>
                                <X size={24} />
                            </button>
                        </div>
                        <div className="modal-body">
                            <div className="form-field">
                                <label className="form-label">档案显示名称 (Label)</label>
                                <input
                                    className="form-input"
                                    placeholder="例如: 供应商档案"
                                    value={newType.label}
                                    onChange={e => setNewType({ ...newType, label: e.target.value })}
                                />
                            </div>
                            <div className="form-field">
                                <label className="form-label">唯一识别码 (Backend Key)</label>
                                <input
                                    className="form-input"
                                    placeholder="例如: vendor-archives"
                                    value={newType.key}
                                    onChange={e => setNewType({ ...newType, key: e.target.value })}
                                />
                                <p className="form-help-text">由小写字母与连字符组成，不可重复</p>
                            </div>
                            <div className="form-field">
                                <label className="form-label">标识图标</label>
                                <select
                                    className="form-select"
                                    value={newType.icon}
                                    onChange={e => setNewType({ ...newType, icon: e.target.value })}
                                >
                                    <option value="FileText">📄 文本列表 (FileText)</option>
                                    <option value="Layout">📊 数据布局 (Layout)</option>
                                    <option value="Globe">🌐 远程同步 (Globe)</option>
                                    <option value="Settings">⚙️ 配置项 (Settings)</option>
                                </select>
                            </div>
                        </div>
                        <div className="modal-footer">
                            <button className="btn-cancel" onClick={() => setIsAddModalOpen(false)}>取消</button>
                            <button className="btn-save" onClick={handleAddType}>确认创建</button>
                        </div>
                    </div>
                </div>
            )}

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

export default ArchiveAPIs;
