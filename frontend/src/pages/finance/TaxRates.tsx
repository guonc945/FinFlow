import { useEffect, useMemo, useState, useCallback } from 'react';
import { ChevronDown, ChevronUp, Filter, RefreshCw, Search } from 'lucide-react';
import Select from '../../components/common/Select';
import axios from 'axios';
import ConfirmModal from '../../components/common/ConfirmModal';
import FinanceSyncStatus, { FINANCE_SYNC_FINISHED_EVENT, notifyFinanceSyncStarted } from '../../components/common/FinanceSyncStatus';
import DataTable from '../../components/data/DataTable';
import { useToast, ToastContainer } from '../../components/Toast';
import { API_BASE_URL } from '../../services/apiBase';
import '../bills/Bills.css';
import './AccountingSubjects.css';
import './AccountingSubjectsNav.css';

interface TaxRate {
    id: string;
    number: string;
    name: string;
    enable?: string;
    enable_title?: string;
    status?: string;
    source_created_time?: string;
    source_modified_time?: string;
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

const STATUS_MAP: Record<string, string> = {
    A: '创建',
    B: '提交',
    C: '已审核',
};

const ENABLE_FILTERS = [
    { value: 'all', label: '全部启用状态' },
    { value: '1', label: '启用' },
    { value: '0', label: '禁用' },
];

const STATUS_FILTERS = [
    { value: 'all', label: '全部数据状态' },
    { value: 'A', label: '创建' },
    { value: 'B', label: '提交' },
    { value: 'C', label: '已审核' },
];

const FETCH_LIMIT = 1000;

const TaxRates = () => {
    const [allItems, setAllItems] = useState<TaxRate[]>([]);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(100);
    const [loading, setLoading] = useState(false);
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearchTerm, setDebouncedSearchTerm] = useState('');
    const [selectedEnable, setSelectedEnable] = useState('all');
    const [selectedStatus, setSelectedStatus] = useState('all');
    const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);
    const { toasts, showToast, removeToast } = useToast();

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchTerm(searchTerm);
            setCurrentPage(1);
        }, 400);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    useEffect(() => {
        setCurrentPage(1);
    }, [selectedEnable, selectedStatus]);

    const fetchTaxRates = useCallback(async () => {
        setLoading(true);
        try {
            const res = await axios.get(`${API_BASE_URL}/finance/tax-rates`, {
                params: {
                    skip: 0,
                    limit: FETCH_LIMIT,
                    search: debouncedSearchTerm || undefined,
                },
            });

            setAllItems(Array.isArray(res.data?.items) ? res.data.items : []);
        } catch (error) {
            console.error(error);
            setAllItems([]);
            showToast('error', '加载失败', '无法获取税率档案列表');
        } finally {
            setLoading(false);
        }
    }, [debouncedSearchTerm, showToast]);

    useEffect(() => {
        void fetchTaxRates();
    }, [fetchTaxRates]);

    useEffect(() => {
        const handleSyncFinished = (event: Event) => {
            const customEvent = event as CustomEvent<{ moduleCode?: string }>;
            if (customEvent.detail?.moduleCode !== 'tax-rates') {
                return;
            }
            void fetchTaxRates();
        };

        window.addEventListener(FINANCE_SYNC_FINISHED_EVENT, handleSyncFinished as EventListener);
        return () => window.removeEventListener(FINANCE_SYNC_FINISHED_EVENT, handleSyncFinished as EventListener);
    }, [fetchTaxRates]);

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await axios.post(`${API_BASE_URL}/finance/tax-rates/sync`, {});
            notifyFinanceSyncStarted('tax-rates');
            showToast('success', '同步任务已提交', '系统正在后台同步税率档案，完成后将自动刷新水位和列表');
        } catch (error: unknown) {
            const apiError = error as ApiErrorLike;
            showToast(
                'error',
                '同步异常',
                apiError.response?.data?.error || apiError.response?.data?.detail || apiError.message || '税率同步请求未能成功发起',
            );
        } finally {
            setLoading(false);
        }
    };

    const filteredItems = useMemo(() => {
        return allItems.filter((item) => {
            const enableMatched = selectedEnable === 'all' || (selectedEnable === '1' ? item.enable === '1' : item.enable !== '1');
            const statusMatched = selectedStatus === 'all' || item.status === selectedStatus;
            return enableMatched && statusMatched;
        });
    }, [allItems, selectedEnable, selectedStatus]);

    const total = filteredItems.length;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const pagedItems = useMemo(() => {
        const start = (currentPage - 1) * pageSize;
        return filteredItems.slice(start, start + pageSize);
    }, [filteredItems, currentPage, pageSize]);

    const columns = useMemo(() => ([
        {
            key: 'number' as keyof TaxRate,
            title: '税率编码',
            width: 180,
            render: (value: string) => <span className="font-mono text-slate-700">{value || '-'}</span>,
        },
        {
            key: 'name' as keyof TaxRate,
            title: '税率名称',
            width: 260,
            render: (value: string) => <span className="font-medium text-slate-800">{value || '-'}</span>,
        },
        {
            key: 'enable' as keyof TaxRate,
            title: '启用状态',
            width: 140,
            render: (_value: string, record: TaxRate) => (
                record.enable === '1' ? (
                    <span className="status-badge status-badge-active">{record.enable_title || '启用'}</span>
                ) : (
                    <span className="status-badge status-badge-inactive">{record.enable_title || '禁用'}</span>
                )
            ),
        },
        {
            key: 'status' as keyof TaxRate,
            title: '数据状态',
            width: 140,
            render: (value: string) => (
                <span
                    className={`px-2 py-1 rounded text-xs font-medium ${
                        value === 'C'
                            ? 'bg-green-100 text-green-700'
                            : value === 'B'
                                ? 'bg-blue-100 text-blue-700'
                                : 'bg-yellow-100 text-yellow-700'
                    }`}
                >
                    {STATUS_MAP[value || ''] || value || '未知'}
                </span>
            ),
        },
        {
            key: 'source_created_time' as keyof TaxRate,
            title: '创建时间',
            width: 180,
            render: (value: string) => <span className="text-slate-600">{value || '-'}</span>,
        },
        {
            key: 'source_modified_time' as keyof TaxRate,
            title: '修改时间',
            width: 180,
            render: (value: string) => <span className="text-slate-600">{value || '-'}</span>,
        },
    ]), []);

    return (
        <div className="page-container fade-in tax-rates-page">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">税率档案筛选</h4>
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
                                    placeholder="搜索税率编码或税率名称..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                />
                            </div>
                            <div className="divider-v"></div>
                            <div className="page-size-selector flex items-center gap-2 text-slate-600">
                                <span>启用状态</span>
                                <Select
                                    value={selectedEnable}
                                    onChange={(v) => setSelectedEnable(v)}
                                    className="page-size-select"
                                    options={ENABLE_FILTERS}
                                />
                            </div>
                            <div className="page-size-selector flex items-center gap-2 text-slate-600">
                                <span>数据状态</span>
                                <Select
                                    value={selectedStatus}
                                    onChange={(v) => setSelectedStatus(v)}
                                    className="page-size-select"
                                    options={STATUS_FILTERS}
                                />
                            </div>
                            <button onClick={() => void fetchTaxRates()} disabled={loading} className="btn btn-outline">
                                <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
                                刷新
                            </button>
                            <button onClick={() => setIsConfirmModalOpen(true)} disabled={loading} className="btn-primary">
                                <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
                                {loading ? '同步中...' : '立即同步'}
                            </button>
                        </div>
                        <FinanceSyncStatus moduleCode="tax-rates" />
                    </div>
                )}
            </div>

            <div className="table-area-wrapper flex-1 overflow-hidden flex flex-col">
                <DataTable
                    columns={columns}
                    data={pagedItems}
                    loading={loading}
                    tableId="tax-rates-list"
                    serialStart={(currentPage - 1) * pageSize + 1}
                    title={(
                        <div className="flex items-center justify-between w-full">
                            <span>税率档案列表</span>
                            <span className="text-xs font-normal text-secondary">
                                {total === 0 ? '暂无数据' : `当前页 ${pagedItems.length} 条，共 ${total} 条`}
                            </span>
                        </div>
                    )}
                />

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
                                onChange={(v) => {
                                    setPageSize(Number(v));
                                    setCurrentPage(1);
                                }}
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
                            <button
                                onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                                disabled={currentPage === 1 || loading}
                                className="page-btn"
                            >
                                上一页
                            </button>
                            <div className="current-page-display">{currentPage}</div>
                            <button
                                onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
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
                title="同步税率档案"
                message="确定要从金蝶星空系统同步税率档案吗？这将通过已配置的“获取税率”接口拉取最新数据，并在后台更新本地税率档案。"
                confirmText="确定同步"
                loading={loading}
                onCancel={() => setIsConfirmModalOpen(false)}
                onConfirm={handleSync}
            />
        </div>
    );
};

export default TaxRates;
