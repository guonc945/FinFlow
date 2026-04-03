import { useEffect, useState } from 'react';
import {
    RefreshCw,
    Search,
    Filter, ChevronDown, ChevronUp
} from 'lucide-react';
import axios from 'axios';
import ConfirmModal from '../../components/common/ConfirmModal';
import { useToast, ToastContainer } from '../../components/Toast';
import { API_BASE_URL } from '../../services/apiBase';
import './AccountingSubjects.css';
import '../bills/Bills.css';

interface Customer {
    id: string;
    number: string;
    name: string;
    status: string;
    enable: string;
    type: string;
    linkman?: string;
    bizpartner_phone?: string;
    bizpartner_address?: string;
    societycreditcode?: string;
    org_name?: string;
    createorg_name?: string;
}

const PARTNER_TYPE_MAP: Record<string, string> = {
    '1': '法人企业',
    '2': '非法人企业',
    '3': '非企业单位',
    '4': '个人',
    '5': '个体户'
};

const Customers = () => {
    const [customers, setCustomers] = useState<Customer[]>([]);
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

    useEffect(() => {
        void fetchCustomers();
    }, [currentPage, pageSize, debouncedSearchTerm]);

    const fetchCustomers = async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await axios.get(`${API_BASE_URL}/finance/customers`, {
                params: {
                    skip,
                    limit: pageSize,
                    search: debouncedSearchTerm || undefined
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                setCustomers(res.data.items);
                setTotal(res.data.total);
            } else {
                setCustomers([]);
                setTotal(0);
            }
        } catch (err) {
            console.error(err);
            showToast('error', '加载失败', '无法获取客户列表');
        } finally {
            setLoading(false);
        }
    };

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await axios.post(`${API_BASE_URL}/finance/customers/sync`, {});
            showToast('success', '同步任务已提交', '系统正在后台处理客户同步，请在 1-2 分钟后刷新列表查看结果');
            setTimeout(() => {
                void fetchCustomers();
            }, 3000);
        } catch (err: any) {
            showToast('error', '同步异常', err.response?.data?.error || err.response?.data?.detail || err.message || '由于网络或授权原因，同步请求未能成功发起');
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
                        <h4 className="text-sm font-semibold">客户档案同步与筛选</h4>
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
                                    placeholder="搜索客户编码或名称..."
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
                    <table className="data-table min-w-max">
                        <thead className="table-header">
                            <tr>
                                <th className="w-16 text-center">序号</th>
                                <th className="w-32">客户编码</th>
                                <th className="w-64">客户名称</th>
                                <th className="w-32">伙伴类型</th>
                                <th className="w-32">联系人</th>
                                <th className="w-32">联系电话</th>
                                <th className="w-48">统一信用代码</th>
                                <th className="w-24">数据状态</th>
                                <th className="w-24">使用状态</th>
                            </tr>
                        </thead>
                        <tbody className="table-body">
                            {customers.length > 0 ? (
                                customers.map((customer, index) => (
                                    <tr key={customer.id} className="table-row">
                                        <td className="table-cell text-center font-medium text-slate-400">
                                            {(currentPage - 1) * pageSize + index + 1}
                                        </td>
                                        <td className="table-cell font-mono text-slate-500">
                                            {customer.number}
                                        </td>
                                        <td className="table-cell font-medium">
                                            {customer.name}
                                        </td>
                                        <td className="table-cell">
                                            <span className="text-xs font-medium text-slate-500 bg-slate-100 px-2 py-0.5 rounded">
                                                {PARTNER_TYPE_MAP[customer.type] || customer.type || '未知'}
                                            </span>
                                        </td>
                                        <td className="table-cell text-slate-600">
                                            {customer.linkman || '-'}
                                        </td>
                                        <td className="table-cell text-slate-600">
                                            {customer.bizpartner_phone || '-'}
                                        </td>
                                        <td className="table-cell text-slate-600 font-mono text-sm">
                                            {customer.societycreditcode || '-'}
                                        </td>
                                        <td className="table-cell">
                                            <span className={`px-2 py-1 rounded text-xs font-medium ${customer.status === 'C' ? 'bg-green-100 text-green-700' :
                                                customer.status === 'B' ? 'bg-blue-100 text-blue-700' :
                                                    'bg-yellow-100 text-yellow-700'
                                                }`}>
                                                {customer.status === 'C' ? '已审核' : customer.status === 'B' ? '已提交' : '暂存'}
                                            </span>
                                        </td>
                                        <td className="table-cell">
                                            {customer.enable === '1' ?
                                                <span className="status-badge status-badge-active">启用</span> :
                                                <span className="status-badge status-badge-inactive">禁用</span>
                                            }
                                        </td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan={9} className="empty-state">
                                        {loading ? '加载中...' : '暂无数据，请尝试同步'}
                                    </td>
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
                            <select
                                value={pageSize}
                                onChange={e => {
                                    setPageSize(Number(e.target.value));
                                    setCurrentPage(1);
                                }}
                                className="page-size-select"
                            >
                                <option value={20}>20</option>
                                <option value={50}>50</option>
                                <option value={100}>100</option>
                                <option value={500}>500</option>
                            </select>
                        </div>
                        <div className="page-buttons">
                            <button
                                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                                disabled={currentPage === 1 || loading}
                                className="page-btn"
                            >
                                上一页
                            </button>
                            <div className="current-page-display">
                                {currentPage}
                            </div>
                            <button
                                onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                                disabled={currentPage >= totalPages || loading}
                                className="page-btn"
                            >
                                下一页
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <ConfirmModal
                isOpen={isConfirmModalOpen}
                title="同步客户档案"
                message="确定要从金蝶星空系统同步客户信息吗？这将通过已配置的接口拉取最新数据，并在后台更新本地客户档案。"
                confirmText="确定同步"
                loading={loading}
                onCancel={() => setIsConfirmModalOpen(false)}
                onConfirm={handleSync}
            />
        </div>
    );
};

export default Customers;
