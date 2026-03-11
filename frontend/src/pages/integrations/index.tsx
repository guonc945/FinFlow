
import { useState, useEffect } from 'react';
import { RefreshCw, CheckCircle, AlertTriangle, Link2, ShieldCheck, Clock, Save, Edit3, Key, Trash2 } from 'lucide-react';
import axios from 'axios';
import { API_BASE_URL } from '../../services/apiBase';
import './Integrations.css';

interface ExtStatus {
    status: 'connected' | 'not_connected' | 'expired';
    message: string;
    expires_at: string | null;
    has_refresh_token: boolean;
    last_updated: string | null;
}

const Integrations = () => {
    const [status, setStatus] = useState<ExtStatus | null>(null);
    const [refreshing, setRefreshing] = useState(false);

    const [markiStatus, setMarkiStatus] = useState<ExtStatus | null>(null);
    const [markiRefreshing, setMarkiRefreshing] = useState(false);

    // Marki Config UI states
    const [isEditingMarki, setIsEditingMarki] = useState(false);
    const [markiUser, setMarkiUser] = useState('');
    const [markiPassword, setMarkiPassword] = useState('');

    const fetchStatuses = async () => {
        try {
            const [kdRes, mkRes] = await Promise.all([
                axios.get(`${API_BASE_URL}/external/kingdee/status`),
                axios.get(`${API_BASE_URL}/external/marki/status`)
            ]);
            setStatus(kdRes.data);
            setMarkiStatus(mkRes.data);
            if (mkRes.data.status === 'connected') {
                setMarkiUser('********'); // hide real username or show placeholder
                setMarkiPassword('********');
            }
        } catch (error) {
            console.error('Failed to fetch statuses:', error);
        }
    };

    const handleRefreshKingdee = async () => {
        setRefreshing(true);
        try {
            await axios.post(`${API_BASE_URL}/external/kingdee/refresh`);
            await fetchStatuses();
            alert('刷新成功');
        } catch (error) {
            console.error('Refresh failed:', error);
            alert('刷新失败，请检查后台日志');
        } finally {
            setRefreshing(false);
        }
    };

    const handleRefreshMarki = async () => {
        setMarkiRefreshing(true);
        try {
            await axios.post(`${API_BASE_URL}/external/marki/refresh`);
            await fetchStatuses();
            alert('马克联同步凭证已更新');
        } catch (error) {
            console.error('Marki refresh failed:', error);
            alert('获取凭证失败，请确认账号密码是否正确');
        } finally {
            setMarkiRefreshing(false);
        }
    };

    const handleSaveMarkiConfig = async () => {
        try {
            await axios.post(`${API_BASE_URL}/external/marki/config`, {
                app_id: markiUser,
                app_secret: markiPassword
            });
            setIsEditingMarki(false);
            alert('马克联配置保存成功，建议您立即手动刷新一次测试连通性！');
        } catch (error) {
            console.error('Save failed:', error);
            alert('保存失败，请重试');
        }
    };

    const handleDeleteMarki = async () => {
        if (!confirm('确定要删除马克联系统集成吗？这将移除所有关联的 API 凭证。')) return;
        try {
            // Find the ID for 'marki' service first or use a known endpoint
            const res = await axios.get(`${API_BASE_URL}/external/services`);
            const markiService = res.data.find((s: any) => s.service_name === 'marki');
            if (markiService) {
                await axios.delete(`${API_BASE_URL}/external/services/${markiService.id}`);
                alert('马克联配置已删除');
                fetchStatuses();
            }
        } catch (error) {
            console.error('Delete failed:', error);
            alert('删除失败');
        }
    };

    useEffect(() => {
        fetchStatuses();
    }, []);

    const getStatusBadge = (st: ExtStatus | null) => {
        if (!st) return null;
        if (st.status === 'connected') {
            return <div className="badge success"><CheckCircle size={14} /> 已连接</div>;
        } else if (st.status === 'expired') {
            return <div className="badge warning"><AlertTriangle size={14} /> 已过期</div>;
        } else {
            return <div className="badge error"><AlertTriangle size={14} /> 未连接</div>;
        }
    };

    return (
        <div className="page-container fade-in">
            <h2 className="page-title mb-6">外部集成管理</h2>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {/* Kingdee K3/Cloud Integration Card */}
                <div className="card glass integration-card">
                    <div className="card-header flex justify-between items-start">
                        <div className="flex gap-4">
                            <div className="service-icon bg-blue-100 text-blue-600 p-3 rounded-xl">
                                <Link2 size={24} />
                            </div>
                            <div>
                                <h3 className="text-lg font-bold">金蝶云星空</h3>
                                <p className="text-secondary text-sm">财务凭证对接与同步</p>
                            </div>
                        </div>
                        {status && getStatusBadge(status)}
                    </div>

                    <div className="card-body mt-4">
                        <div className="status-grid">
                            <div className="status-item">
                                <div className="label"><ShieldCheck size={14} /> 认证方式</div>
                                <div className="value">OAuth2 (Client Credentials)</div>
                            </div>
                            <div className="status-item">
                                <div className="label"><Clock size={14} /> 过期时间</div>
                                <div className="value">
                                    {status?.expires_at ? new Date(status.expires_at).toLocaleString() : '-'}
                                </div>
                            </div>
                            <div className="status-item">
                                <div className="label"><RefreshCw size={14} /> 最后更新</div>
                                <div className="value">
                                    {status?.last_updated ? new Date(status.last_updated).toLocaleString() : '-'}
                                </div>
                            </div>
                        </div>

                        {status?.status === 'not_connected' && (
                            <div className="mt-4 p-3 bg-gray-50 rounded text-sm text-secondary">
                                请确保后台 .env 文件中已配置 KINGDEE_APP_ID 和 KINGDEE_APP_SECRET。
                            </div>
                        )}
                    </div>

                    <div className="card-footer mt-6 flex justify-end gap-3 border-t border-gray-100 pt-4">
                        <button
                            className="btn-outline flex items-center gap-2"
                            onClick={handleRefreshKingdee}
                            disabled={refreshing}
                        >
                            <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
                            {refreshing ? '正在刷新...' : '强制刷新凭证'}
                        </button>
                    </div>
                </div>

                {/* Marki Integration Card */}
                <div className="card glass integration-card">
                    <div className="card-header flex justify-between items-start">
                        <div className="flex gap-4">
                            <div className="service-icon bg-green-100 text-green-600 p-3 rounded-xl">
                                <Key size={24} />
                            </div>
                            <div>
                                <h3 className="text-lg font-bold">马克联物业系统</h3>
                                <p className="text-secondary text-sm">基础业务数据同步</p>
                            </div>
                        </div>
                        {markiStatus && getStatusBadge(markiStatus)}
                    </div>

                    <div className="card-body mt-4">
                        {!isEditingMarki ? (
                            <div className="status-grid">
                                <div className="status-item">
                                    <div className="label"><ShieldCheck size={14} /> 认证账号</div>
                                    <div className="value">{markiUser || '未配置'}</div>
                                </div>
                                <div className="status-item">
                                    <div className="label"><Clock size={14} /> 凭证状态</div>
                                    <div className="value">{markiStatus?.status === 'connected' ? '有效' : '需刷新/配置'}</div>
                                </div>
                                <div className="status-item">
                                    <div className="label"><RefreshCw size={14} /> 最后更新</div>
                                    <div className="value">
                                        {markiStatus?.last_updated ? new Date(markiStatus.last_updated).toLocaleString() : '-'}
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="space-y-4">
                                <div>
                                    <label className="block text-sm text-secondary mb-1">账号 (MARKI_USER)</label>
                                    <input
                                        type="text"
                                        className="w-full px-3 py-2 border border-gray-200 rounded focus:border-green-500 focus:outline-none"
                                        value={markiUser}
                                        onChange={e => setMarkiUser(e.target.value)}
                                        placeholder="请输入马克系统账号"
                                    />
                                </div>
                                <div>
                                    <label className="block text-sm text-secondary mb-1">密码 (MARKI_PASSWORD)</label>
                                    <input
                                        type="password"
                                        className="w-full px-3 py-2 border border-gray-200 rounded focus:border-green-500 focus:outline-none"
                                        value={markiPassword}
                                        onChange={e => setMarkiPassword(e.target.value)}
                                        placeholder="请输入密码"
                                    />
                                </div>
                            </div>
                        )}

                        {markiStatus?.status === 'not_connected' && !isEditingMarki && (
                            <div className="mt-4 p-3 bg-gray-50 rounded text-sm text-secondary">
                                请点击下面的"配置"按钮完善账号和密码，以启用数据同步。
                            </div>
                        )}
                    </div>

                    <div className="card-footer mt-6 flex justify-between gap-3 border-t border-gray-100 pt-4">
                        {isEditingMarki ? (
                            <div className="flex gap-2 w-full justify-end">
                                <button className="btn-outline px-4 py-2" onClick={() => setIsEditingMarki(false)}>取消</button>
                                <button className="btn-primary flex items-center gap-2 px-4 py-2" onClick={handleSaveMarkiConfig}>
                                    <Save size={16} /> 保存配置
                                </button>
                            </div>
                        ) : (
                            <>
                                <button className="btn-outline flex items-center gap-2" onClick={() => setIsEditingMarki(true)}>
                                    <Edit3 size={16} /> 配置
                                </button>
                                <button className="btn-outline text-red-500 hover:bg-red-50 flex items-center gap-2" onClick={handleDeleteMarki}>
                                    <Trash2 size={16} /> 删除
                                </button>
                                <button
                                    className="btn-primary flex items-center gap-2"
                                    onClick={handleRefreshMarki}
                                    disabled={markiRefreshing || !markiUser}
                                >
                                    <RefreshCw size={16} className={markiRefreshing ? 'animate-spin' : ''} />
                                    {markiRefreshing ? '请求中...' : '测试抓取凭证'}
                                </button>
                            </>
                        )}
                    </div>
                </div>

                {/* Placeholder for future integrations */}
                <div className="card glass integration-card opacity-60">
                    <div className="card-header flex gap-4">
                        <div className="service-icon bg-gray-100 text-gray-400 p-3 rounded-xl">
                            <Link2 size={24} />
                        </div>
                        <div>
                            <h3 className="text-lg font-bold text-gray-400">泛微 OA (Coming Soon)</h3>
                            <p className="text-secondary text-sm">审批流对接</p>
                        </div>
                    </div>
                    <div className="card-body mt-4">
                        <p className="text-sm text-secondary">集成开发中...</p>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Integrations;
