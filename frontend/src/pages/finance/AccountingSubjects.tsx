import { useState, useEffect, useMemo } from 'react';
import {
    RefreshCw, Check,
    Search,
    ChevronRight, ChevronDown, ChevronUp, Filter
} from 'lucide-react';
import axios from 'axios';
import ConfirmModal from '../../components/common/ConfirmModal';
import { useToast, ToastContainer } from '../../components/Toast';
import { API_BASE_URL } from '../../services/apiBase';
import '../bills/Bills.css';
import './AccountingSubjects.css';
import './AccountingSubjectsNav.css';

interface AccountingSubject {
    id: string;
    number: string;
    name: string;
    fullname: string;
    level: number;
    is_leaf: boolean;
    long_number: string;
    direction: string;
    is_active: boolean;
    is_cash: boolean;
    is_bank: boolean;
    is_cash_equivalent: boolean;
    check_items: string;
    account_type_number: string;
}

const ACCOUNT_TYPE_MAP: Record<string, string> = {
    '0': '资产',
    '1': '负债',
    '2': '权益',
    '3': '成本',
    '4': '损益',
    '5': '表外',
    '6': '共同',
    '7': '其他',
    'A': '预算收入',
    'B': '预算支出',
    'C': '预算结余'
};

const AccountingSubjects = () => {
    const [subjects, setSubjects] = useState<AccountingSubject[]>([]);
    const [total, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(100);
    const [loading, setLoading] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearchTerm, setDebouncedSearchTerm] = useState('');
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);
    const [expandedKeys, setExpandedKeys] = useState<Set<string>>(new Set());
    const [selectedType, setSelectedType] = useState<string>('all');
    const { toasts, showToast, removeToast } = useToast();

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchTerm(searchTerm);
            setCurrentPage(1);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    useEffect(() => {
        void fetchSubjects();
    }, [currentPage, pageSize, selectedType, debouncedSearchTerm]);

    const fetchSubjects = async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await axios.get(`${API_BASE_URL}/finance/accounting-subjects`, {
                params: {
                    skip,
                    limit: pageSize,
                    search: debouncedSearchTerm || undefined,
                    account_type: selectedType === 'all' ? undefined : selectedType
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                setSubjects(res.data.items);
                setTotal(res.data.total);
            } else {
                setSubjects([]);
                setTotal(0);
            }
        } catch (err) {
            console.error(err);
            showToast('error', '加载失败', '无法获取会计科目列表');
        } finally {
            setLoading(false);
        }
    };

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await axios.post(`${API_BASE_URL}/finance/accounting-subjects/sync`, {});
            showToast('success', '同步任务已提交', '系统正在后台处理会计科目同步，请在 1-2 分钟后刷新列表查看结果');
            setTimeout(() => {
                void fetchSubjects();
            }, 3000);
        } catch (err: any) {
            showToast('error', '同步异常', err.response?.data?.detail || err.message || '由于网络或授权原因，同步请求未能成功发起');
        } finally {
            setLoading(false);
        }
    };

    const handleToggleExpand = (id: string, e: React.MouseEvent) => {
        e.stopPropagation();
        const newExpandedKeys = new Set(expandedKeys);
        if (newExpandedKeys.has(id)) {
            newExpandedKeys.delete(id);
        } else {
            newExpandedKeys.add(id);
        }
        setExpandedKeys(newExpandedKeys);
    };

    const sortedSubjects = useMemo(() => {
        return [...subjects].sort((a, b) => {
            const valA = a.long_number || a.number;
            const valB = b.long_number || b.number;
            return valA.localeCompare(valB);
        });
    }, [subjects]);

    const filteredSubjects = useMemo(() => {
        if (searchTerm) {
            return sortedSubjects;
        }

        const isParentExpanded = (subject: AccountingSubject): boolean => {
            if (subject.level === 1) return true;
            const index = sortedSubjects.findIndex(s => s.id === subject.id);
            if (index <= 0) return true;

            let parent = null;
            for (let i = index - 1; i >= 0; i--) {
                if (sortedSubjects[i].level === subject.level - 1) {
                    parent = sortedSubjects[i];
                    break;
                }
            }

            if (parent) {
                return expandedKeys.has(parent.id) && isParentExpanded(parent);
            }
            return true;
        };

        return sortedSubjects.filter(s => isParentExpanded(s));
    }, [sortedSubjects, searchTerm, expandedKeys]);

    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    return (
        <div className="page-container fade-in accounting-subjects-page">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">会计科目同步与筛选</h4>
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
                                <input type="text" placeholder="搜索编码或名称..." value={searchTerm} onChange={e => setSearchTerm(e.target.value)} />
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

            <div className="flex gap-4 mt-2 flex-1 overflow-hidden min-h-0">
                <div className="type-nav-sidebar glass-effect card-shadow min-w-[200px] flex flex-col h-full overflow-hidden">
                    <div className="nav-header px-4 py-3 border-b border-slate-100/50 flex-shrink-0">
                        <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">科目分类导航</span>
                    </div>
                    <div className="nav-list p-2 overflow-y-auto flex-1 custom-scrollbar">
                        <div className={`nav-item-custom ${selectedType === 'all' ? 'active' : ''}`} onClick={() => { setSelectedType('all'); setCurrentPage(1); }}>
                            <span className="nav-label">全部科目</span>
                        </div>
                        {Object.entries(ACCOUNT_TYPE_MAP).map(([key, name]) => (
                            <div key={key} className={`nav-item-custom ${selectedType === key ? 'active' : ''}`} onClick={() => { setSelectedType(key); setCurrentPage(1); }}>
                                <span className="nav-label">{name}</span>
                            </div>
                        ))}
                    </div>
                </div>

                <div className="table-area-wrapper flex-1 overflow-hidden flex flex-col">
                    <div className="table-container custom-scrollbar">
                        <table className="data-table">
                            <thead className="table-header">
                                <tr>
                                    <th className="w-16 text-center">序号</th>
                                    <th className="w-48">科目编码</th>
                                    <th className="w-64">科目名称</th>
                                    <th className="w-32">类型/属性</th>
                                    <th className="w-24">方向</th>
                                    <th>核算维度</th>
                                    <th className="w-24">状态</th>
                                    <th className="w-20">明细</th>
                                </tr>
                            </thead>
                            <tbody className="table-body">
                                {filteredSubjects.length > 0 ? (
                                    filteredSubjects.map(subject => {
                                        let parsedCheckItems: any[] = [];
                                        try {
                                            if (subject.check_items) {
                                                const parsed = JSON.parse(subject.check_items);
                                                parsedCheckItems = Array.isArray(parsed) ? parsed : [];
                                            }
                                        } catch (e) {
                                            console.error('Failed to parse check items', e);
                                            parsedCheckItems = [];
                                        }

                                        return (
                                            <tr key={subject.id} className="table-row">
                                                <td className="table-cell text-center font-medium text-slate-400">{(currentPage - 1) * pageSize + filteredSubjects.findIndex(s => s.id === subject.id) + 1}</td>
                                                <td className="table-cell">
                                                    <div className="subject-cell-content" style={{ paddingLeft: searchTerm ? 0 : (subject.level - 1) * 24 }}>
                                                        {!subject.is_leaf && !searchTerm && (
                                                            <button className="expand-toggle" onClick={(e) => handleToggleExpand(subject.id, e)}>
                                                                {expandedKeys.has(subject.id) ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                                                            </button>
                                                        )}
                                                        {subject.is_leaf && !searchTerm && <div className="expand-placeholder" />}
                                                        <span className="subject-code">{subject.number}</span>
                                                    </div>
                                                </td>
                                                <td className="table-cell">
                                                    <div className="subject-info-cell">
                                                        <span className="subject-name">{subject.name}</span>
                                                        <span className="subject-fullname-hint">{subject.fullname}</span>
                                                    </div>
                                                </td>
                                                <td className="table-cell">
                                                    <div className="flex flex-col gap-1">
                                                        {subject.account_type_number && ACCOUNT_TYPE_MAP[subject.account_type_number] && (
                                                            <span className="text-xs font-medium text-slate-500 bg-slate-100 px-2 py-0.5 rounded w-fit">{ACCOUNT_TYPE_MAP[subject.account_type_number]}</span>
                                                        )}
                                                        <div className="attr-badges">
                                                            {subject.is_cash && <span className="badge badge-cash">现</span>}
                                                            {subject.is_bank && <span className="badge badge-bank">银</span>}
                                                            {subject.is_cash_equivalent && <span className="badge badge-eq">等</span>}
                                                        </div>
                                                    </div>
                                                </td>
                                                <td className="table-cell">
                                                    <span className={`direction-tag ${subject.direction === '1' ? 'dr' : 'cr'}`}>{subject.direction === '1' ? '借' : '贷'}</span>
                                                </td>
                                                <td className="table-cell">
                                                    <div className="check-items-tags">
                                                        {parsedCheckItems.length > 0 ? parsedCheckItems.map((item: any, idx: number) => (
                                                            <span key={idx} className="check-item-tag">{item.asstactitem_name}</span>
                                                        )) : <span className="text-slate-300">-</span>}
                                                    </div>
                                                </td>
                                                <td className="table-cell">
                                                    {subject.is_active ? <span className="status-badge status-badge-active">启用</span> : <span className="status-badge status-badge-inactive">禁用</span>}
                                                </td>
                                                <td className="table-cell">
                                                    {subject.is_leaf ? <Check size={16} className="leaf-icon" /> : <span className="text-slate-300">-</span>}
                                                </td>
                                            </tr>
                                        );
                                    })
                                ) : (
                                    <tr>
                                        <td colSpan={8} className="empty-state">{loading ? '加载中...' : '暂无数据，请尝试同步'}</td>
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
                                <select value={pageSize} onChange={e => { setPageSize(Number(e.target.value)); setCurrentPage(1); }} className="page-size-select">
                                    <option value={20}>20</option>
                                    <option value={50}>50</option>
                                    <option value={100}>100</option>
                                    <option value={500}>500</option>
                                </select>
                            </div>
                            <div className="page-buttons">
                                <button onClick={() => setCurrentPage(p => Math.max(1, p - 1))} disabled={currentPage === 1 || loading} className="page-btn">上一页</button>
                                <div className="current-page-display">{currentPage}</div>
                                <button onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))} disabled={currentPage >= totalPages || loading} className="page-btn">下一页</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <ConfirmModal
                isOpen={isConfirmModalOpen}
                title="同步会计科目"
                message="确定要从外部系统同步会计科目吗？这将更新现有的科目数据，同步过程将在后台异步进行。"
                confirmText="确定同步"
                loading={loading}
                onCancel={() => setIsConfirmModalOpen(false)}
                onConfirm={handleSync}
            />
        </div>
    );
};

export default AccountingSubjects;
