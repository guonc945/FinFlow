import { useState, useEffect, useCallback } from 'react';
import {
    RefreshCw,
    Search,
    Filter, ChevronDown, ChevronUp
} from 'lucide-react';
import Select from '../../components/common/Select';
import axios from 'axios';
import ConfirmModal from '../../components/common/ConfirmModal';
import FinanceSyncStatus, { FINANCE_SYNC_FINISHED_EVENT, notifyFinanceSyncStarted } from '../../components/common/FinanceSyncStatus';
import { useToast, ToastContainer } from '../../components/Toast';
import { API_BASE_URL } from '../../services/apiBase';
import './AccountingSubjects.css';
import '../bills/Bills.css';

interface KingdeeHouse {
    id: string;
    number: string;
    wtw8_number: string;
    name: string;
    tzqslx: string;
    splx: string;
    createorg_name?: string;
    createorg_number?: string;
}

type ApiErrorLike = {
    response?: {
        data?: {
            error?: string;
            detail?: string;
        };
    };
    message?: string;
};

const KingdeeHouses = () => {
    const [kdHouses, setKdHouses] = useState<KingdeeHouse[]>([]);
    const [total, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(100);
    const [loading, setLoading] = useState(false);
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearchTerm, setDebouncedSearchTerm] = useState('');
    const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);
    const { toasts, showToast, removeToast } = useToast();

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchTerm(searchTerm);
            setCurrentPage(1);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    const fetchKdHouses = useCallback(async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await axios.get(`${API_BASE_URL}/finance/kd-houses`, {
                params: {
                    skip,
                    limit: pageSize,
                    search: debouncedSearchTerm || undefined
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                setKdHouses(res.data.items);
                setTotal(res.data.total);
            } else {
                setKdHouses([]);
                setTotal(0);
            }
        } catch (err) {
            console.error(err);
            showToast('error', '加载失败', '无法获取房号列表');
        } finally {
            setLoading(false);
        }
    }, [currentPage, debouncedSearchTerm, pageSize, showToast]);

    useEffect(() => {
        void fetchKdHouses();
    }, [fetchKdHouses]);

    useEffect(() => {
        const handleSyncFinished = (event: Event) => {
            const customEvent = event as CustomEvent<{ moduleCode?: string }>;
            if (customEvent.detail?.moduleCode !== 'kd-houses') {
                return;
            }
            void fetchKdHouses();
        };

        window.addEventListener(FINANCE_SYNC_FINISHED_EVENT, handleSyncFinished as EventListener);
        return () => window.removeEventListener(FINANCE_SYNC_FINISHED_EVENT, handleSyncFinished as EventListener);
    }, [fetchKdHouses]);

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await axios.post(`${API_BASE_URL}/finance/kd-houses/sync`, {});
            notifyFinanceSyncStarted('kd-houses');
            showToast('success', '同步任务已提交', '系统正在后台处理房号同步，完成后将自动刷新水位和列表');
        } catch (err: unknown) {
            const apiError = err as ApiErrorLike;
            showToast('error', '同步异常', apiError.response?.data?.error || apiError.response?.data?.detail || apiError.message || '由于网络或授权原因，同步请求未能成功发起');
        } finally {
            setLoading(false);
        }
    };

    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    return (
        <div className="page-container fade-in kd-houses-page">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">房号同步与筛选</h4>
                    </div>
                    <button className="collapse-toggle" onClick={() => setIsFilterCollapsed(!isFilterCollapsed)}>
                        {isFilterCollapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                    </button>
                </div>

                {!isFilterCollapsed && (
                    <div className="filter-content-wrapper fade-in">
                        <div className="action-row">
                            <div className="search-group flex-1">
                                <Search size={14} className="search-icon" />
                                <input
                                    type="text"
                                    placeholder="搜索原始编码、房号或名称..."
                                    value={searchTerm}
                                    onChange={e => setSearchTerm(e.target.value)}
                                />
                            </div>
                            <div className="divider-v"></div>
                            <button onClick={() => setIsConfirmModalOpen(true)} disabled={loading} className="btn-primary">
                                <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
                                {loading ? '同步中...' : '立即同步'}
                            </button>
                        </div>
                        <FinanceSyncStatus moduleCode="kd-houses" />
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <div className="table-container custom-scrollbar">
                    <table className="data-table min-w-max">
                        <thead className="table-header">
                            <tr>
                                <th className="w-16 text-center">序号</th>
                                <th className="w-32">原始编码</th>
                                <th className="w-32">房号(编号)</th>
                                <th className="w-64">房号名称</th>
                                <th className="w-32">投资权属类型</th>
                                <th className="w-32">商铺类型</th>
                                <th className="w-48">创建组织</th>
                            </tr>
                        </thead>
                        <tbody className="table-body">
                            {kdHouses.length > 0 ? (
                                kdHouses.map((kh, index) => (
                                    <tr key={kh.id} className="table-row">
                                        <td className="table-cell text-center font-medium text-slate-400">{(currentPage - 1) * pageSize + index + 1}</td>
                                        <td className="table-cell font-mono text-slate-500">{kh.number || '-'}</td>
                                        <td className="table-cell font-mono font-medium text-slate-700">{kh.wtw8_number || '-'}</td>
                                        <td className="table-cell font-bold text-slate-900">{kh.name}</td>
                                        <td className="table-cell"><span className="text-xs font-medium text-slate-600 bg-slate-100 px-2 py-0.5 rounded">{kh.tzqslx || '未知'}</span></td>
                                        <td className="table-cell"><span className="text-xs font-medium text-indigo-700 bg-indigo-50 px-2 py-0.5 rounded border border-indigo-100">{kh.splx || '未指定'}</span></td>
                                        <td className="table-cell text-slate-600">{kh.createorg_name || kh.createorg_number || '-'}</td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan={7} className="empty-state">{loading ? '加载中...' : '暂无数据，请尝试同步'}</td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>

                <div className="pagination-footer">
                    <div className="pagination-info">
                        共 <span className="text-primary font-bold">{total}</span> 条记录
                        <span className="mx-2 text-slate-300">|</span>
                        第 {currentPage} / {totalPages} 页
                    </div>
                    <div className="pagination-controls">
                        <div className="page-size-selector">
                            <span>每页显示:</span>
                            <Select
                                value={String(pageSize)}
                                onChange={(v) => { setPageSize(Number(v)); setCurrentPage(1); }}
                                className="page-size-select"
                                options={[
                                    { value: '20', label: '20' },
                                    { value: '50', label: '50' },
                                    { value: '100', label: '100' },
                                    { value: '500', label: '500' },
                                ]}
                            />
                        </div>
                        <div className="page-buttons">
                            <button onClick={() => setCurrentPage(p => Math.max(1, p - 1))} disabled={currentPage === 1 || loading} className="page-btn">上一页</button>
                            <div className="current-page-display">{currentPage}</div>
                            <button onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))} disabled={currentPage >= totalPages || loading} className="page-btn">下一页</button>
                        </div>
                    </div>
                </div>
            </div>
            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <ConfirmModal
                isOpen={isConfirmModalOpen}
                title="同步房号信息"
                message="确定要从金蝶星空系统同步房号信息档案吗？这将通过已配置的接口拉取最新数据，并在后台更新本地房号档案。"
                confirmText="确定同步"
                loading={loading}
                onCancel={() => setIsConfirmModalOpen(false)}
                onConfirm={handleSync}
            />
        </div>
    );
};

export default KingdeeHouses;
