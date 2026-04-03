import { useState, useEffect } from 'react';
import {
    RefreshCw, Tags,
    Search, Filter, ChevronDown, ChevronUp
} from 'lucide-react';
import axios from 'axios';
import ConfirmModal from '../../components/common/ConfirmModal';
import { useToast, ToastContainer } from '../../components/Toast';
import { API_BASE_URL } from '../../services/apiBase';
import './AccountingSubjects.css';
import '../bills/Bills.css';

interface AuxiliaryDataCategory {
    id: string;
    number: string;
    name: string;
    fissyspreset: boolean;
    description: string;
    ctrlstrategy: string;
    createorg_name: string;
}

const CTRL_STRATEGY_MAP: Record<string, string> = {
    '7': '私有',
    '5': '全局共享',
    '6': '分级管控'
};

const AuxiliaryDataCategoriesPage = () => {
    const [categories, setCategories] = useState<AuxiliaryDataCategory[]>([]);
    const [total, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize] = useState(100);
    const [loading, setLoading] = useState(false);
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);
    const { toasts, showToast, removeToast } = useToast();

    useEffect(() => {
        void fetchCategories();
    }, [currentPage, pageSize]);

    useEffect(() => {
        const timer = setTimeout(() => {
            setCurrentPage(1);
            void fetchCategories();
        }, 300);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    const fetchCategories = async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await axios.get(`${API_BASE_URL}/finance/auxiliary-data-categories`, {
                params: {
                    skip,
                    limit: pageSize,
                    search: searchTerm || undefined
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                setCategories(res.data.items);
                setTotal(res.data.total);
            } else {
                setCategories([]);
                setTotal(0);
            }
        } catch (err) {
            console.error(err);
            showToast('error', '加载失败', '无法获取辅助资料类别列表');
        } finally {
            setLoading(false);
        }
    };

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await axios.post(`${API_BASE_URL}/finance/auxiliary-data-categories/sync`, {});
            showToast('success', '同步任务已提交', '系统正在后台同步辅助资料分类数据，请稍后刷新');
            setTimeout(() => {
                void fetchCategories();
            }, 3000);
        } catch (err: any) {
            showToast('error', '同步失败', err.response?.data?.detail || '无法启动同步任务');
        } finally {
            setLoading(false);
        }
    };

    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    return (
        <div className="page-container fade-in">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">辅助核算类别同步与筛选</h4>
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
                                    placeholder="搜索分类编码或名称..."
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
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <div className="table-container custom-scrollbar">
                    <table className="data-table min-w-max w-full text-left border-collapse">
                        <thead className="table-header bg-slate-50/80 backdrop-blur-sm border-b border-slate-200">
                            <tr>
                                <th className="w-16 py-4 px-6 font-semibold text-slate-600 text-center">序号</th>
                                <th className="w-48 py-4 px-6 font-semibold text-slate-600">分类编码</th>
                                <th className="w-64 py-4 px-6 font-semibold text-slate-600">分类名称</th>
                                <th className="py-4 px-6 font-semibold text-slate-600">描述</th>
                                <th className="w-48 py-4 px-6 font-semibold text-slate-600">创建组织</th>
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600 text-center">系统预置</th>
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600 text-center">控制策略</th>
                            </tr>
                        </thead>
                        <tbody className="table-body">
                            {categories.length > 0 ? (
                                categories.map((item, index) => (
                                    <tr key={item.id} className="table-row hover:bg-slate-50/50 transition-colors border-b border-slate-50 group">
                                        <td className="table-cell py-3 px-6 text-center font-medium text-slate-400">{(currentPage - 1) * pageSize + index + 1}</td>
                                        <td className="table-cell py-3 px-6 font-mono text-slate-500 text-sm">{item.number}</td>
                                        <td className="table-cell py-3 px-6 font-medium text-slate-800">{item.name}</td>
                                        <td className="table-cell py-3 px-6 text-slate-500 text-sm italic">{item.description || '-'}</td>
                                        <td className="table-cell py-3 px-6 text-slate-600 text-sm">{item.createorg_name || '-'}</td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            {item.fissyspreset ?
                                                <span className="text-xs font-medium text-amber-600 bg-amber-50 px-2.5 py-1 rounded-full border border-amber-200">是</span> :
                                                <span className="text-xs font-medium text-slate-500 bg-slate-50 px-2.5 py-1 rounded-full border border-slate-200">否</span>
                                            }
                                        </td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            <span className={`px-2.5 py-1 rounded-md text-xs font-medium ${item.ctrlstrategy === '5' ? 'bg-sky-50 text-sky-600 border border-sky-200' : 'bg-fuchsia-50 text-fuchsia-600 border border-fuchsia-200'}`}>
                                                {CTRL_STRATEGY_MAP[item.ctrlstrategy] || item.ctrlstrategy || '未知'}
                                            </span>
                                        </td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan={7} className="empty-state py-16 text-center text-slate-500 bg-slate-50/30">
                                        {loading ? (
                                            <div className="flex flex-col items-center justify-center gap-3">
                                                <RefreshCw size={24} className="animate-spin text-orange-400" />
                                                <span>正在拉取分类数据...</span>
                                            </div>
                                        ) : (
                                            <div className="flex flex-col items-center justify-center gap-3">
                                                <Tags size={32} className="text-slate-300" />
                                                <span>暂无分类数据，请点击右上方按钮同步</span>
                                            </div>
                                        )}
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>

                <div className="pagination-footer p-4 bg-white/60 border-t border-slate-100 flex items-center justify-between text-sm">
                    <div className="pagination-info text-slate-600">共 <span className="text-orange-600 font-bold mx-1">{total}</span> 个类别</div>
                    <div className="pagination-controls flex items-center gap-4">
                        <div className="page-buttons flex items-center gap-2">
                            <button onClick={() => setCurrentPage(p => Math.max(1, p - 1))} disabled={currentPage === 1 || loading} className="page-btn px-3 py-1.5 rounded border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-orange-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">上一页</button>
                            <div className="current-page-display bg-orange-50 text-orange-700 w-8 h-8 rounded flex items-center justify-center font-medium border border-orange-100">{currentPage}</div>
                            <button onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))} disabled={currentPage >= totalPages || loading} className="page-btn px-3 py-1.5 rounded border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-orange-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">下一页</button>
                        </div>
                    </div>
                </div>
            </div>

            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <ConfirmModal
                isOpen={isConfirmModalOpen}
                title="同步分类列表"
                message="确定要从金蝶星空系统同步辅助资料分类吗？这将更新现有的所有分类定义。"
                confirmText="执行同步"
                loading={loading}
                onCancel={() => setIsConfirmModalOpen(false)}
                onConfirm={handleSync}
            />
        </div>
    );
};

export default AuxiliaryDataCategoriesPage;
