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

const ACCTTYPE_MAP: Record<string, { label: string; color: string }> = {
    in_out: { label: '收支账户', color: '#2563eb' },
    in: { label: '收入账户', color: '#10b981' },
    out: { label: '支出账户', color: '#f59e0b' },
};

const ACCTSTYLE_MAP: Record<string, string> = {
    basic: '基本存款账户',
    normal: '一般存款账户',
    temp: '临时存款账户',
    spcl: '专用存款账户',
    fgn_curr: '经常项目外汇账户',
    fng_fin: '资本项目外汇账户',
};

const ACCTSTATUS_MAP: Record<string, { label: string; bg: string; color: string; border: string }> = {
    normal: { label: '正常', bg: '#dcfce7', color: '#15803d', border: '#86efac' },
    closing: { label: '销户中', bg: '#fef3c7', color: '#b45309', border: '#fcd34d' },
    changing: { label: '变更中', bg: '#dbeafe', color: '#1d4ed8', border: '#93c5fd' },
    closed: { label: '已销户', bg: '#fee2e2', color: '#b91c1c', border: '#fca5a5' },
    freeze: { label: '冻结', bg: '#ede9fe', color: '#6d28d9', border: '#d8b4fe' },
};

interface KingdeeBankAccount {
    id: string;
    bankaccountnumber: string;
    name: string;
    acctname: string;
    company_number?: string;
    company_name?: string;
    openorg_number?: string;
    openorg_name?: string;
    defaultcurrency_number?: string;
    defaultcurrency_name?: string;
    accttype?: string;
    acctstyle?: string;
    finorgtype?: string;
    banktype_number?: string;
    banktype_name?: string;
    bank_number?: string;
    bank_name?: string;
    acctproperty_number?: string;
    acctproperty_name?: string;
    status?: string;
    acctstatus?: string;
    isdefaultrec?: boolean;
    isdefaultpay?: boolean;
    comment?: string;
}

const BankAccounts = () => {
    const [accounts, setAccounts] = useState<KingdeeBankAccount[]>([]);
    const [total, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(50);
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
        void fetchAccounts();
    }, [currentPage, pageSize, debouncedSearchTerm]);

    const fetchAccounts = async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await axios.get(`${API_BASE_URL}/finance/kd-bank-accounts`, {
                params: {
                    skip,
                    limit: pageSize,
                    search: debouncedSearchTerm || undefined
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                setAccounts(res.data.items);
                setTotal(res.data.total);
            } else {
                setAccounts([]);
                setTotal(0);
            }
        } catch (err) {
            console.error(err);
            showToast('error', '加载失败', '无法获取银行账户列表');
        } finally {
            setLoading(false);
        }
    };

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await axios.post(`${API_BASE_URL}/finance/kd-bank-accounts/sync`, {});
            showToast('success', '同步任务已提交', '系统正在后台处理银行账户同步，请在 1-2 分钟后刷新列表查看结果');
            setTimeout(() => {
                void fetchAccounts();
            }, 3000);
        } catch (err: any) {
            showToast('error', '同步异常', err.response?.data?.error || err.response?.data?.detail || err.message || '由于网络或授权原因，同步请求未能成功发起');
        } finally {
            setLoading(false);
        }
    };

    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    const getAcctStatusBadge = (acctstatus?: string) => {
        const info = ACCTSTATUS_MAP[acctstatus || ''] || { label: acctstatus || '未知', bg: '#f1f5f9', color: '#64748b', border: '#e2e8f0' };
        return (
            <span style={{
                display: 'inline-flex', alignItems: 'center', gap: '0.25rem',
                padding: '0.2rem 0.6rem', borderRadius: '6px', fontSize: '0.75rem', fontWeight: 600,
                background: info.bg, color: info.color, border: `1px solid ${info.border}`
            }}>
                {info.label}
            </span>
        );
    };

    const getAcctTypeBadge = (accttype?: string) => {
        const info = ACCTTYPE_MAP[accttype || ''];
        if (!info) return <span className="text-xs text-slate-400">{accttype || '-'}</span>;
        return (
            <span style={{
                padding: '0.2rem 0.5rem', borderRadius: '4px', fontSize: '0.7rem', fontWeight: 600,
                color: info.color, background: `${info.color}12`, border: `1px solid ${info.color}30`
            }}>
                {info.label}
            </span>
        );
    };

    return (
        <div className="page-container fade-in">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">银行账户同步与筛选</h4>
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
                                    placeholder="搜索银行账号、账户简称或开户行..."
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
                                <th className="w-12 text-center">序号</th>
                                <th className="w-40">银行账号</th>
                                <th className="w-36">账户简称</th>
                                <th className="w-48">账户名称</th>
                                <th className="w-32">账户性质</th>
                                <th className="w-36">账户类型</th>
                                <th className="w-40">开户行</th>
                                <th className="w-32">币别</th>
                                <th className="w-36">开户组织</th>
                                <th className="w-20">状态</th>
                                <th className="w-20 text-center">默认</th>
                            </tr>
                        </thead>
                        <tbody className="table-body">
                            {accounts.length > 0 ? (
                                accounts.map((acct, index) => (
                                    <tr key={acct.id} className="table-row">
                                        <td className="table-cell text-center font-medium text-slate-400">
                                            {(currentPage - 1) * pageSize + index + 1}
                                        </td>
                                        <td className="table-cell font-mono font-medium text-slate-700" style={{ fontSize: '0.8rem' }}>
                                            {acct.bankaccountnumber || '-'}
                                        </td>
                                        <td className="table-cell font-bold text-slate-900">
                                            {acct.name || '-'}
                                        </td>
                                        <td className="table-cell text-slate-700" style={{ fontSize: '0.8rem' }}>
                                            {acct.acctname || '-'}
                                        </td>
                                        <td className="table-cell">
                                            {getAcctTypeBadge(acct.accttype)}
                                        </td>
                                        <td className="table-cell">
                                            <span className="text-xs text-slate-600">
                                                {ACCTSTYLE_MAP[acct.acctstyle || ''] || acct.acctstyle || '-'}
                                            </span>
                                        </td>
                                        <td className="table-cell text-slate-600" style={{ fontSize: '0.8rem' }}>
                                            {acct.bank_name || '-'}
                                        </td>
                                        <td className="table-cell">
                                            <span className="text-xs font-medium text-amber-700 bg-amber-50 px-2 py-0.5 rounded border border-amber-100">
                                                {acct.defaultcurrency_name || acct.defaultcurrency_number || '-'}
                                            </span>
                                        </td>
                                        <td className="table-cell text-slate-600" style={{ fontSize: '0.8rem' }}>
                                            {acct.openorg_name || acct.openorg_number || '-'}
                                        </td>
                                        <td className="table-cell">
                                            {getAcctStatusBadge(acct.acctstatus)}
                                        </td>
                                        <td className="table-cell text-center">
                                            <div style={{ display: 'flex', gap: '0.25rem', justifyContent: 'center', flexDirection: 'column', alignItems: 'center' }}>
                                                {acct.isdefaultrec && (
                                                    <span style={{ fontSize: '0.65rem', padding: '0.1rem 0.35rem', borderRadius: '3px', background: '#dcfce7', color: '#15803d', fontWeight: 600 }}>
                                                        收
                                                    </span>
                                                )}
                                                {acct.isdefaultpay && (
                                                    <span style={{ fontSize: '0.65rem', padding: '0.1rem 0.35rem', borderRadius: '3px', background: '#dbeafe', color: '#1d4ed8', fontWeight: 600 }}>
                                                        付
                                                    </span>
                                                )}
                                                {!acct.isdefaultrec && !acct.isdefaultpay && (
                                                    <span className="text-slate-300">-</span>
                                                )}
                                            </div>
                                        </td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan={11} className="empty-state">
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
                title="同步银行账户信息"
                message="确定要从金蝶星空系统同步银行账户信息吗？这将通过已配置的“金蝶银行账户查询”接口拉取最新数据，并在后台更新本地银行账户档案。"
                confirmText="确定同步"
                loading={loading}
                onCancel={() => setIsConfirmModalOpen(false)}
                onConfirm={handleSync}
            />
        </div>
    );
};

export default BankAccounts;
