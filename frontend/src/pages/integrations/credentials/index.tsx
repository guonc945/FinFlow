import { useState, useEffect } from 'react';
import { Plus, Edit2, Trash2, Key, Eye, EyeOff, RefreshCw } from 'lucide-react';

import axios from 'axios';
import './Credentials.css';
import type { ExternalService } from './types';
import CredentialModal from './CredentialModal';
import { useToast, ToastContainer } from '../../../components/Toast';

interface CredentialsManagerProps {
    hideHeader?: boolean;
}

const CredentialsManager: React.FC<CredentialsManagerProps> = ({ hideHeader = false }) => {
    const { toasts, showToast, removeToast } = useToast();
    const [services, setServices] = useState<ExternalService[]>([]);
    const [isEditingService, setIsEditingService] = useState(false);
    const [currentService, setCurrentService] = useState<Partial<ExternalService>>({});
    const [showSecret, setShowSecret] = useState<Record<number, boolean>>({});

    useEffect(() => {
        fetchServices();
    }, []);

    const fetchServices = async () => {
        try {
            const res = await axios.get(`${import.meta.env.VITE_API_BASE_URL}/external/services`);
            setServices(res.data);
        } catch (error) {
            console.error('Failed to fetch services:', error);
        }
    };

    const handleDeleteService = async (id: number) => {
        if (!confirm('确定要删除此凭证配置吗？删除后相关集成将和服务断开。')) return;
        try {
            await axios.delete(`${import.meta.env.VITE_API_BASE_URL}/external/services/${id}`);
            showToast('success', '删除成功', '凭证配置已移除');
            fetchServices();
        } catch (error) {
            showToast('error', '删除失败', '无法移除该配置，请稍后重试');
        }
    };

    const handleSaveService = async (serviceData: Partial<ExternalService>) => {
        try {
            if (serviceData.id) {
                await axios.put(`${import.meta.env.VITE_API_BASE_URL}/external/services/${serviceData.id}`, serviceData);
            } else {
                await axios.post(`${import.meta.env.VITE_API_BASE_URL}/external/services`, serviceData);
            }
            setIsEditingService(false);
            showToast('success', '保存成功', '凭证配置已更新');
            fetchServices();
        } catch (error) {
            showToast('error', '保存失败', '请检查必填项是否完整');
            throw error;
        }
    };

    const handleRefreshToken = async (id: number) => {
        try {
            const res = await axios.post(`${import.meta.env.VITE_API_BASE_URL}/external/services/${id}/token`);
            if (res.data.success) {
                showToast('success', '凭证刷新成功', `有效期至: ${new Date(res.data.expires_at).toLocaleString()}`);
                fetchServices();
            }
        } catch (error: any) {
            showToast('error', '刷新失败', error.response?.data?.detail || '无法连接到远程服务');
        }
    };

    const getServiceIcon = (name: string) => {
        return name.substring(0, 2).toUpperCase();
    };

    return (
        <div className="credentials-page">
            <ToastContainer toasts={toasts} removeToast={removeToast} />

            {/* Page Header */}
            {!hideHeader && (
                <header className="page-header-modern">
                    <div className="header-title-group">
                        <h1>凭证中心</h1>
                        <p>集成外部业务系统的安全身份验证与通信配置托管</p>
                    </div>
                    <button
                        className="btn-primary-clean flex items-center gap-2 h-[42px] shadow-sm"
                        onClick={() => { setCurrentService({ auth_type: 'oauth2', is_active: true }); setIsEditingService(true); }}
                    >
                        <Plus size={18} /> 新增配置
                    </button>
                </header>
            )}

            {hideHeader && (
                <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }}>
                    <button
                        className="btn-primary-clean flex items-center gap-2 h-[36px] shadow-sm"
                        onClick={() => { setCurrentService({ auth_type: 'oauth2', is_active: true }); setIsEditingService(true); }}
                    >
                        <Plus size={16} /> 新增配置
                    </button>
                </div>
            )}

            {/* Services Grid */}
            <div className="credentials-grid">
                {services.map(service => (
                    <div
                        key={service.id}
                        className="credential-card cursor-pointer group"
                        onClick={() => { setCurrentService(service); setIsEditingService(true); }}
                    >
                        <div className="card-header">
                            <div className="service-identity">
                                <div className="service-icon">
                                    {getServiceIcon(service.service_name || 'ER')}
                                </div>
                                <div className="service-info">
                                    <h3>
                                        {service.display_name || service.service_name}
                                        <span className="auth-badge">{service.auth_type}</span>
                                    </h3>
                                    <p>{service.service_name}</p>
                                </div>
                            </div>
                        </div>

                        <div className="card-body">
                            <div className="info-row">
                                <span className="info-label">App ID</span>
                                <span className="info-value">
                                    {service.app_id || '-'}
                                </span>
                            </div>
                            <div className="info-row">
                                <span className="info-label">Secret</span>
                                <div className="info-value">
                                    <span>
                                        {showSecret[service.id] ? service.app_secret : '••••••••'}
                                    </span>
                                    <button
                                        onClick={(e) => { e.stopPropagation(); setShowSecret(prev => ({ ...prev, [service.id]: !prev[service.id] })); }}
                                        className="text-slate-400 hover:text-slate-600 p-1"
                                    >
                                        {showSecret[service.id] ? <EyeOff size={12} /> : <Eye size={12} />}
                                    </button>
                                </div>
                            </div>
                            <div className="info-row mt-3 pt-3 border-t border-slate-100">
                                <span className="info-label">Base URL</span>
                                <span className="info-value text-[10px] truncate max-w-[180px]" title={service.base_url}>
                                    {service.base_url || 'N/A'}
                                </span>
                            </div>
                        </div>

                        <div className="card-actions-row">
                            <button
                                className="action-btn-clean"
                                onClick={(e) => { e.stopPropagation(); handleRefreshToken(service.id); }}
                                title="刷新/测试连接"
                            >
                                <RefreshCw size={14} />
                                <span>刷新</span>
                            </button>
                            <button
                                className="action-btn-clean"
                                onClick={(e) => { e.stopPropagation(); setCurrentService(service); setIsEditingService(true); }}
                                title="编辑配置"
                            >
                                <Edit2 size={14} />
                                <span>编辑</span>
                            </button>
                            <button
                                className="action-btn-clean danger"
                                onClick={(e) => { e.stopPropagation(); handleDeleteService(service.id); }}
                                title="删除配置"
                            >
                                <Trash2 size={14} />
                                <span>删除</span>
                            </button>
                        </div>

                        <div className="card-footer">
                            <div className={`status-indicator ${service.is_active ? 'status-active' : 'status-inactive'}`}>
                                <div className="status-dot"></div>
                                <span>{service.is_active ? '运行中' : '已停用'}</span>
                            </div>
                            <span className="update-time">
                                更新于 {new Date(service.updated_at || Date.now()).toLocaleDateString()}
                            </span>
                        </div>
                    </div>
                ))}

                {services.length === 0 && (
                    <div className="empty-state-card">
                        <div className="w-16 h-16 bg-white border border-slate-200 rounded-2xl flex items-center justify-center text-slate-300 mb-4 shadow-sm">
                            <Key size={32} />
                        </div>
                        <h3 className="text-slate-600 font-bold mb-1">暂无凭证</h3>
                        <p className="text-slate-400 text-sm mb-6">配置您的第一个外部系统连接凭证</p>
                        <button
                            className="btn-primary-clean"
                            onClick={() => { setCurrentService({ auth_type: 'oauth2', is_active: true }); setIsEditingService(true); }}
                        >
                            创建配置
                        </button>
                    </div>
                )}
            </div>

            <CredentialModal
                key={currentService.id || 'new'}
                isOpen={isEditingService}
                onClose={() => setIsEditingService(false)}
                onSave={handleSaveService}
                initialData={currentService}
                showToast={showToast}
            />

        </div>
    );
};

export default CredentialsManager;
