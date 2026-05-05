import { useState, useEffect } from 'react';
import { X, Hash, Database, Settings, Code2, Eye, EyeOff } from 'lucide-react';


import type { ExternalService } from './types';
import VariablePicker from '../../settings/VariablePicker';
import KeyValueEditor from './KeyValueEditor';
import './Credentials.css';
import './CredentialTabs.css';
import type { ToastType } from '../../../components/Toast';
import { API_BASE_URL } from '../../../services/apiBase';

interface CredentialModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSave: (service: Partial<ExternalService>) => Promise<void>;
    initialData: Partial<ExternalService>;
    showToast: (type: ToastType, message: string, description?: string) => void;
}

type VariableOption = {
    insert_text?: string;
    key?: string;
};

const CredentialModal = ({ isOpen, onClose, onSave, initialData, showToast }: CredentialModalProps) => {
    const [formData, setFormData] = useState<Partial<ExternalService>>(initialData);
    const [isSaving, setIsSaving] = useState(false);
    const [isTesting, setIsTesting] = useState(false);
    const [activeSectionTab, setActiveSectionTab] = useState<'headers' | 'body'>('headers');
    const [showSecret, setShowSecret] = useState(false);


    // Variable Picker State
    const [pickerOpen, setPickerOpen] = useState(false);
    const [activePickerField, setActivePickerField] = useState<string | null>(null);

    const openPicker = (field: string) => {
        setActivePickerField(field);
        setPickerOpen(true);
    };

    const handleSelectVar = (variable: VariableOption | string) => {
        if (!activePickerField) return;
        const insertion = typeof variable === 'string'
            ? variable
            : variable?.insert_text || (variable?.key ? `{${variable.key}}` : String(variable || ''));

        // Try to find element by ID (for KeyValueEditor or specific fields)
        const el = document.getElementById(activePickerField) as HTMLInputElement | HTMLTextAreaElement ||
            (document.querySelector(`input[name="${activePickerField}"]`) as HTMLInputElement) ||
            (activePickerField.startsWith('kv-') ? null : document.activeElement);

        if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) {
            const start = (el as HTMLInputElement).selectionStart || 0;
            const end = (el as HTMLInputElement).selectionEnd || 0;
            const text = (el as HTMLInputElement).value;
            const before = text.substring(0, start);
            const after = text.substring(end, text.length);
            const newValue = before + insertion + after;

            // Mapping picker field to state
            if (activePickerField.startsWith('kv-input-')) {
                // KeyValueEditor handles its own internal state via onChange, 
                // so we need to trigger a manual change event or use callback
                // However, since KeyValueEditor is a controlled component, 
                // we should let it handle insertion if possible.
                // But for now, since we're using ID based selection, we'll manually trigger change
                const event = new Event('input', { bubbles: true });
                (el as HTMLInputElement).value = newValue;
                el.dispatchEvent(event);

                // Set cursor position back
                setTimeout(() => {
                    el.focus();
                    const newPos = start + insertion.length;
                    (el as HTMLInputElement).setSelectionRange(newPos, newPos);
                }, 10);
            } else {
                setFormData(prev => ({ ...prev, [activePickerField]: newValue }));
                setTimeout(() => {
                    (el as HTMLInputElement).focus();
                    const newPos = start + insertion.length;
                    (el as HTMLInputElement).setSelectionRange(newPos, newPos);
                }, 10);
            }
        } else {
            // Fallback: append if element not found/focused
            const fieldName = activePickerField.startsWith('kv-') ? null : activePickerField;
            if (fieldName) {
                const currentVal = (formData[fieldName as keyof ExternalService] as string) || '';
                setFormData(prev => ({ ...prev, [fieldName]: currentVal + insertion }));
            }
        }
    };

    useEffect(() => {
        if (isOpen) {
            setFormData({
                ...initialData,
                app_secret: '',
            });
            setShowSecret(false);
        }
    }, [isOpen, initialData]);

    const handleTestConnection = async () => {
        setIsTesting(true);
        try {
            const res = await fetch(`${API_BASE_URL}/external/services/test-connection`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(formData)
            });
            const result = await res.json();
            if (result.success) {
                showToast('success', '连接测试成功', `认证状态: ${result.message}`);
            } else {
                showToast('error', '连接测试失败', `原因: ${result.message}`);
            }
        } catch (_error) {
            showToast('error', '测试失败', '无法连接到后端服务器');
        } finally {
            setIsTesting(false);
        }
    };

    const handleSave = async () => {
        if (!formData.service_name || !formData.display_name || !formData.auth_type) {
            showToast('error', '校验失败', '请填写所有必填字段');
            return;
        }
        setIsSaving(true);
        try {
            await onSave(formData);
            onClose();
        } catch (_error) {
            // Error handled by onSave parent if needed, but we show toast here too
            showToast('error', '保存失败', '请检查配置或稍后再试');
        } finally {
            setIsSaving(false);
        }
    };

    if (!isOpen) return null;

    return (
        <div className="modal-overlay">
            <div className="modal-content-refined">
                {/* Header */}
                <header className="modal-header-refined">
                    <div className="flex items-center gap-3">
                        <div className="w-10 h-10 bg-blue-50 text-blue-600 rounded-xl flex items-center justify-center">
                            <Database size={20} />
                        </div>
                        <div>
                            <h2 className="text-lg font-bold text-slate-900 leading-tight">
                                {formData.id ? '编辑服务配置' : '集成新业务系统'}
                            </h2>
                            <p className="text-[11px] text-slate-400 font-medium">外部业务凭证托管与通信配置</p>
                        </div>
                    </div>
                    <button onClick={onClose} className="p-2 hover:bg-slate-100 rounded-full transition-colors text-slate-400">
                        <X size={20} />
                    </button>
                </header>

                <div className="modal-body-refined">
                    {/* Section 1: Basic Identity */}
                    <div className="dash-section">
                        <div className="dash-section-header">
                            <h3 className="dash-section-title">基础配置</h3>
                        </div>
                        <div className="dash-row-3">
                            <div className="field-container">
                                <label className="modern-label text-[11px]">
                                    服务名称<span className="required-mark">*</span>
                                </label>
                                <input
                                    className="modern-input-pro font-mono text-sm"
                                    value={formData.service_name || ''}
                                    onChange={e => setFormData({ ...formData, service_name: e.target.value })}
                                    placeholder="e.g. odoo_prod"
                                    disabled={!!formData.id}
                                />
                            </div>
                            <div className="field-container">
                                <label className="modern-label text-[11px]">
                                    显示名称<span className="required-mark">*</span>
                                </label>
                                <input
                                    className="modern-input-pro text-sm"
                                    value={formData.display_name || ''}
                                    onChange={e => setFormData({ ...formData, display_name: e.target.value })}
                                    placeholder="e.g. O ERP 生产系统"
                                />
                            </div>
                            <div className="field-container">
                                <label className="modern-label text-[11px]">启用状态</label>
                                <div className="h-[42px] flex items-center">
                                    <label className="ff-switch">
                                        <input
                                            type="checkbox"
                                            className="hidden"
                                            checked={formData.is_active ?? true}
                                            onChange={e => setFormData({ ...formData, is_active: e.target.checked })}
                                        />
                                        <div className="ff-switch-track">
                                            <div className="ff-switch-handle"></div>
                                        </div>
                                        <span className="text-xs font-semibold text-slate-500">
                                            {formData.is_active ? 'ENABLED' : 'DISABLED'}
                                        </span>
                                    </label>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Section 2: Auth Configuration */}
                    <div className="dash-section mt-4">
                        <div className="dash-section-header">
                            <h3 className="dash-section-title">安全与鉴权</h3>
                        </div>
                        <div className="grid grid-cols-12 gap-5">
                            <div className="col-span-4 field-container">
                                <label className="modern-label text-[11px]">鉴权方式<span className="required-mark">*</span></label>
                                <select
                                    className="modern-input-pro text-sm"
                                    value={formData.auth_type || ''}
                                    onChange={e => setFormData({ ...formData, auth_type: e.target.value })}
                                >
                                    <option value="oauth2">OAuth 2.0</option>
                                    <option value="basic">Basic Auth</option>
                                    <option value="api_key">API Key</option>
                                    <option value="bearer">Bearer Token</option>
                                </select>
                            </div>

                            {formData.auth_type !== 'bearer' && (
                                <div className="col-span-4 field-container">
                                    <label className="modern-label text-[11px]">
                                        {formData.auth_type === 'basic' ? '用户名' :
                                            formData.auth_type === 'api_key' ? '参数名' :
                                                'Client ID'}
                                    </label>
                                    <input
                                        className="modern-input-pro font-mono text-sm"
                                        value={formData.app_id || ''}
                                        onChange={e => setFormData({ ...formData, app_id: e.target.value })}
                                        placeholder={formData.auth_type === 'api_key' ? 'e.g. x-api-key' : ''}
                                    />
                                </div>
                            )}

                            <div className={formData.auth_type === 'bearer' ? 'col-span-8 field-container' : 'col-span-4 field-container'}>
                                <div className="flex justify-between">
                                    <label className="modern-label text-[11px]">
                                        {formData.auth_type === 'basic' ? '密码' :
                                            formData.auth_type === 'api_key' ? '参数值' :
                                                formData.auth_type === 'bearer' ? 'Token 令牌' :
                                                    'Client Secret'}
                                    </label>
                                    <div className="flex items-center gap-2">
                                        <button
                                            onClick={() => setShowSecret(!showSecret)}
                                            className="text-slate-300 hover:text-slate-500 transition-colors"
                                            title={showSecret ? "隐藏密码" : "显示密码"}
                                        >
                                            {showSecret ? <EyeOff size={12} /> : <Eye size={12} />}
                                        </button>
                                        <button onClick={() => openPicker('app_secret')} className="text-slate-300 hover:text-blue-500"><Hash size={12} /></button>
                                    </div>
                                </div>
                                <input
                                    className="modern-input-pro font-mono text-sm"
                                    type={showSecret ? "text" : "password"}
                                    id="app_secret"
                                    value={formData.app_secret || ''}
                                    onChange={e => setFormData({ ...formData, app_secret: e.target.value })}
                                    placeholder={initialData.has_app_secret ? '已配置密钥，留空则保持不变' : '请输入密钥'}
                                />
                            </div>

                            {formData.auth_type === 'oauth2' && (
                                <div className="col-span-12 field-container">
                                    <div className="flex justify-between">
                                        <label className="modern-label text-[11px]">Refresh Token (可选)</label>
                                        <button onClick={() => openPicker('refresh_token')} className="text-slate-300 hover:text-blue-500"><Hash size={12} /></button>
                                    </div>
                                    <input
                                        className="modern-input-pro font-mono text-sm"
                                        id="refresh_token"
                                        value={formData.refresh_token || ''}
                                        onChange={e => setFormData({ ...formData, refresh_token: e.target.value })}
                                        placeholder="提供初始刷新令牌（如适用）"
                                    />
                                </div>
                            )}
                        </div>
                    </div>


                    {/* Section 3: API Endpoints */}
                    <div className="dash-section mt-6">
                        <div className="dash-section-header">
                            <h3 className="dash-section-title">连接地址</h3>
                        </div>
                        <div className="dash-row-2">
                            <div className="field-container">
                                <div className="flex justify-between">
                                    <label className="modern-label text-[11px]">Base URL<span className="required-mark">*</span></label>
                                    <button onClick={() => openPicker('base_url')} className="text-slate-300 hover:text-blue-500"><Hash size={12} /></button>
                                </div>
                                <input
                                    className="modern-input-pro font-mono text-sm"
                                    id="base_url"
                                    value={formData.base_url || ''}
                                    onChange={e => setFormData({ ...formData, base_url: e.target.value })}
                                    placeholder="https://api.example.com"
                                />
                            </div>

                            {formData.auth_type === 'oauth2' && (
                                <div className="grid grid-cols-12 gap-4">
                                    <div className="col-span-8 field-container">
                                        <div className="flex justify-between">
                                            <label className="modern-label text-[11px]">Auth URL</label>
                                            <button onClick={() => openPicker('auth_url')} className="text-slate-300 hover:text-blue-500"><Hash size={12} /></button>
                                        </div>
                                        <input
                                            className="modern-input-pro font-mono text-sm"
                                            id="auth_url"
                                            value={formData.auth_url || ''}
                                            onChange={e => setFormData({ ...formData, auth_url: e.target.value })}
                                            placeholder="OAuth 授权地址"
                                        />
                                    </div>
                                    <div className="col-span-4 field-container">
                                        <label className="modern-label text-[11px]">请求方法</label>
                                        <select
                                            className="modern-input-pro text-sm"
                                            value={formData.auth_method || 'POST'}
                                            onChange={e => setFormData({ ...formData, auth_method: e.target.value })}
                                        >
                                            <option value="POST">POST</option>
                                            <option value="GET">GET</option>
                                        </select>
                                    </div>
                                </div>
                            )}

                        </div>
                    </div>


                    {/* Section 4: Advanced Tabs (Header & Body) */}
                    <div className="dash-section mt-8">
                        <div className="minimal-tabs-nav">
                            <button
                                className={`minimal-tab-trigger ${activeSectionTab === 'headers' ? 'active' : ''}`}
                                onClick={() => setActiveSectionTab('headers')}
                            >
                                <Settings size={14} className="tab-icon-small" />
                                Header
                            </button>
                            <button
                                className={`minimal-tab-trigger ${activeSectionTab === 'body' ? 'active' : ''}`}
                                onClick={() => setActiveSectionTab('body')}
                            >
                                <Code2 size={14} className="tab-icon-small" />
                                Body
                            </button>
                        </div>

                        <div className="tab-content-panel min-h-[220px]">
                            {activeSectionTab === 'headers' ? (
                                <KeyValueEditor
                                    title="Custom Execution Headers"
                                    jsonString={formData.auth_headers}
                                    onChange={(val) => setFormData({ ...formData, auth_headers: val })}
                                    onOpenPicker={(id) => openPicker(id)}
                                />
                            ) : (
                                <div className="field-container h-full">
                                    <div className="flex justify-between mb-2">
                                        <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">Custom JSON body</label>
                                        <button
                                            onClick={() => openPicker('auth_body')}
                                            className="text-slate-300 hover:text-blue-500 flex items-center gap-1 text-[10px] font-bold"
                                        >
                                            <Hash size={12} /> 插入变量
                                        </button>
                                    </div>
                                    <textarea
                                        id="auth_body"
                                        className="modern-textarea-pro w-full h-[180px] font-mono text-xs leading-relaxed"
                                        value={formData.auth_body || ''}
                                        onChange={(e) => setFormData({ ...formData, auth_body: e.target.value })}
                                        placeholder='{ "key": "value" }'
                                    ></textarea>
                                </div>
                            )}
                        </div>
                    </div>


                </div>

                <footer className="modal-footer-refined bg-slate-50/50">
                    <div className="flex gap-3">
                        <button
                            onClick={handleTestConnection}
                            disabled={isTesting || isSaving}
                            className="btn-secondary-clean px-6 flex items-center gap-2"
                        >
                            {isTesting && <div className="w-3.5 h-3.5 border-2 border-slate-300 border-t-slate-600 rounded-full animate-spin"></div>}
                            测试连接
                        </button>
                    </div>
                    <div className="flex gap-3">
                        <button onClick={onClose} className="btn-secondary-clean px-6">取消</button>
                        <button
                            onClick={handleSave}
                            disabled={isSaving}
                            className="btn-primary-clean px-8"
                        >
                            {isSaving ? '保存中...' : '提交配置'}
                        </button>
                    </div>
                </footer>

                <VariablePicker
                    isOpen={pickerOpen}
                    onClose={() => setPickerOpen(false)}
                    onSelect={handleSelectVar}
                />
            </div>
        </div>
    );
};

export default CredentialModal;
