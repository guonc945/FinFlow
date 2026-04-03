import { useEffect, useState } from 'react';
import { RefreshCw, BookOpen, Search, Filter, ChevronDown, ChevronUp } from 'lucide-react';
import ConfirmModal from '../../components/common/ConfirmModal';
import { useToast, ToastContainer } from '../../components/Toast';
import { getAccountBooks, syncAccountBooks } from '../../api/accountBook';
import type { AccountBook } from '../../types/accountBook';
import './AccountingSubjects.css';
import '../bills/Bills.css';

const AccountBookPage = () => {
    const [accountBooks, setAccountBooks] = useState<AccountBook[]>([]);
    const [total, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(100);
    const [loading, setLoading] = useState(false);
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);
    const { toasts, showToast, removeToast } = useToast();

    useEffect(() => {
        void fetchAccountBooks();
    }, [currentPage, pageSize]);

    useEffect(() => {
        const timer = setTimeout(() => {
            setCurrentPage(1);
            void fetchAccountBooks();
        }, 300);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    const fetchAccountBooks = async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await getAccountBooks(skip, pageSize, searchTerm);
            if (res && Array.isArray(res.items)) {
                setAccountBooks(res.items);
                setTotal(res.total);
            } else {
                setAccountBooks([]);
                setTotal(0);
            }
        } catch (err) {
            console.error(err);
            showToast('error', '加载失败', '无法获取账簿列表');
        } finally {
            setLoading(false);
        }
    };

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await syncAccountBooks();
            showToast('success', '同步任务已提交', '系统正在后台同步账簿数据，请稍后刷新列表查看结果');
            setTimeout(() => {
                void fetchAccountBooks();
            }, 3000);
        } catch (err: any) {
            showToast('error', '同步异常', err.response?.data?.error || err.message || '同步请求未能成功发起');
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
                        <h4 className="text-sm font-semibold">账簿同步与筛选</h4>
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
                                    placeholder="搜索账簿编码或名称..."
                                    value={searchTerm}
                                    onChange={e => setSearchTerm(e.target.value)}
                                />
                            </div>
                            <div className="divider-v"></div>
                            <button
                                onClick={() => setIsConfirmModalOpen(true)}
                                disabled={loading}
                                className="btn-primary"
                            >
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
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600">账簿编码</th>
                                <th className="w-48 py-4 px-6 font-semibold text-slate-600">账簿名称</th>
                                <th className="w-40 py-4 px-6 font-semibold text-slate-600">核算组织</th>
                                <th className="w-40 py-4 px-6 font-semibold text-slate-600">核算体系</th>
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600 text-center">账簿类型</th>
                                <th className="w-40 py-4 px-6 font-semibold text-slate-600">科目表</th>
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600 text-center">本位币</th>
                                <th className="w-24 py-4 px-6 font-semibold text-slate-600 text-center">状态</th>
                                <th className="w-24 py-4 px-6 font-semibold text-slate-600 text-center">使用状态</th>
                            </tr>
                        </thead>
                        <tbody className="table-body">
                            {accountBooks.length > 0 ? (
                                accountBooks.map((item, index) => (
                                    <tr key={item.id} className="table-row hover:bg-slate-50/50 transition-colors border-b border-slate-50 group">
                                        <td className="table-cell py-3 px-6 text-center font-medium text-slate-400">{(currentPage - 1) * pageSize + index + 1}</td>
                                        <td className="table-cell py-3 px-6 font-mono text-slate-500 text-sm">{item.number}</td>
                                        <td className="table-cell py-3 px-6 font-medium text-slate-800">{item.name}</td>
                                        <td className="table-cell py-3 px-6 text-slate-600 text-sm">{item.org_name || '-'}</td>
                                        <td className="table-cell py-3 px-6 text-slate-600 text-sm">{item.accountingsys_name || '-'}</td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            {item.booknature === '1' ?
                                                <span className="text-xs font-medium text-indigo-600 bg-indigo-50 px-2.5 py-1 rounded-full border border-indigo-200">主账簿</span> :
                                                item.booknature === '0' ?
                                                    <span className="text-xs font-medium text-slate-500 bg-slate-50 px-2.5 py-1 rounded-full border border-slate-200">副账簿</span> :
                                                    <span className="text-xs font-medium text-slate-500 bg-slate-50 px-2.5 py-1 rounded-full border border-slate-200">{item.booknature || '-'}</span>
                                            }
                                        </td>
                                        <td className="table-cell py-3 px-6 text-slate-600 text-sm">{item.accounttable_name || '-'}</td>
                                        <td className="table-cell py-3 px-6 text-center text-slate-600 text-sm">{item.basecurrency_name || '-'}</td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            {item.status === 'C' ?
                                                <span className="text-xs font-medium text-emerald-600 bg-emerald-50 px-2.5 py-1 rounded-full border border-emerald-200">已审核</span> :
                                                item.status === 'B' ?
                                                    <span className="text-xs font-medium text-blue-600 bg-blue-50 px-2.5 py-1 rounded-full border border-blue-200">已提交</span> :
                                                    item.status === 'A' ?
                                                        <span className="text-xs font-medium text-amber-600 bg-amber-50 px-2.5 py-1 rounded-full border border-amber-200">暂存</span> :
                                                        <span className="text-xs font-medium text-slate-500 bg-slate-50 px-2.5 py-1 rounded-full border border-slate-200">{item.status || '-'}</span>
                                            }
                                        </td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            {item.enable === '1' || item.enable === '可用' ?
                                                <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-50 text-emerald-700 border border-emerald-200"><span className="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>可用</span> :
                                                <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-rose-50 text-rose-700 border border-rose-200"><span className="w-1.5 h-1.5 rounded-full bg-rose-500"></span>禁用</span>
                                            }
                                        </td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan={10} className="empty-state py-16 text-center text-slate-500 bg-slate-50/30">
                                        {loading ? (
                                            <div className="flex flex-col items-center justify-center gap-3">
                                                <RefreshCw size={24} className="animate-spin text-indigo-400" />
                                                <span>数据加载中...</span>
                                            </div>
                                        ) : (
                                            <div className="flex flex-col items-center justify-center gap-3">
                                                <BookOpen size={32} className="text-slate-300" />
                                                <span>暂无账簿记录，请尝试一键同步</span>
                                            </div>
                                        )}
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>

                <div className="pagination-footer p-4 bg-white/60 border-t border-slate-100 flex items-center justify-between text-sm">
                    <div className="pagination-info text-slate-600">
                        共 <span className="text-indigo-600 font-bold mx-1">{total}</span> 条记录
                        <span className="mx-3 text-slate-300">|</span>
                        当前第 {currentPage} 页 / 共 {totalPages} 页
                    </div>
                    <div className="pagination-controls flex items-center gap-4">
                        <div className="page-size-selector flex items-center gap-2 text-slate-600">
                            <span>每页显示:</span>
                            <select value={pageSize} onChange={e => { setPageSize(Number(e.target.value)); setCurrentPage(1); }} className="page-size-select bg-white border border-slate-200 rounded px-2 py-1 outline-none focus:ring-1 focus:ring-indigo-500">
                                <option value={20}>20</option>
                                <option value={50}>50</option>
                                <option value={100}>100</option>
                                <option value={500}>500</option>
                            </select>
                        </div>
                        <div className="page-buttons flex items-center gap-2">
                            <button onClick={() => setCurrentPage(p => Math.max(1, p - 1))} disabled={currentPage === 1 || loading} className="page-btn px-3 py-1.5 rounded border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-indigo-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">上一页</button>
                            <div className="current-page-display bg-indigo-50 text-indigo-700 w-8 h-8 rounded flex items-center justify-center font-medium border border-indigo-100">{currentPage}</div>
                            <button onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))} disabled={currentPage >= totalPages || loading} className="page-btn px-3 py-1.5 rounded border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-indigo-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">下一页</button>
                        </div>
                    </div>
                </div>
            </div>

            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <ConfirmModal
                isOpen={isConfirmModalOpen}
                title="同步系统账簿"
                message="确定要从金蝶星空系统拉取最新的账簿信息吗？这将请求接口并在后台更新全量账簿数据。"
                confirmText="确定同步"
                loading={loading}
                onCancel={() => setIsConfirmModalOpen(false)}
                onConfirm={handleSync}
            />
        </div>
    );
};

export default AccountBookPage;
