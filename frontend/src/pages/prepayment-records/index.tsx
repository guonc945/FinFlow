import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
    ChevronDown,
    ChevronLeft,
    ChevronRight,
    ChevronUp,
    ChevronsLeft,
    ChevronsRight,
    Info,
    RefreshCw,
    Search,
    X,
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import { ToastContainer, useToast } from '../../components/Toast';
import type { PrepaymentRecord, Project } from '../../types';
import {
    getPrepaymentRecordSyncStatus,
    getPrepaymentRecords,
    getProjects,
    syncPrepaymentRecords,
} from '../../services/api';
import '../bills/Bills.css';

const SyncProgressModal = ({
    isOpen,
    onClose,
    total,
    current,
    logs,
    status,
}: {
    isOpen: boolean;
    onClose: () => void;
    total: number;
    current: number;
    logs: { message: string; type: 'success' | 'error' | 'info'; time: string }[];
    status: string;
}) => {
    const logRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (logRef.current) {
            logRef.current.scrollTop = logRef.current.scrollHeight;
        }
    }, [logs]);

    if (!isOpen) return null;

    const isCompleted = ['completed', 'failed', 'partially_completed'].includes(status);
    const isFailed = status === 'failed';
    const percentage = Math.round((current / Math.max(total, 1)) * 100) || 0;

    return (
        <div className="sync-overlay">
            <div className="sync-modal">
                <div className="sync-header">
                    <div className="sync-title">
                        {isCompleted ? (
                            <div className={`status-icon-${isFailed ? 'error' : 'success'} ${isFailed ? 'text-error' : 'text-success'}`}>
                                <RefreshCw size={24} />
                            </div>
                        ) : (
                            <div className="status-icon-rotating text-primary">
                                <RefreshCw size={24} />
                            </div>
                        )}
                        <div>
                            <h3>{isCompleted ? (isFailed ? '同步失败' : '同步完成') : '正在同步预存款记录'}</h3>
                            <p className="text-secondary text-sm">
                                {isCompleted ? '预存款记录同步任务已结束' : '正在后台拉取接口中心的预存款变动记录'}
                            </p>
                        </div>
                    </div>
                </div>

                <div className="sync-content">
                    <div className="progress-container">
                        <div className="progress-info">
                            <span className="font-bold text-primary">{percentage}%</span>
                            <span className="text-secondary text-sm">
                                {current} / {Math.max(total, 1)}
                            </span>
                        </div>
                        <div className="progress-bar-bg">
                            <div className={`progress-bar-fill ${isFailed ? 'bg-error' : ''}`} style={{ width: `${percentage}%` }}></div>
                        </div>
                    </div>

                    <div className="log-container" ref={logRef}>
                        {logs.map((log, index) => (
                            <div key={`${log.time}-${index}`} className={`log-item ${log.type}`}>
                                <span className="log-time">[{log.time}]</span>
                                <span className="log-msg">{log.message}</span>
                            </div>
                        ))}
                        {!isCompleted && <div className="log-cursor"></div>}
                    </div>
                </div>

                <div className="sync-footer">
                    {isCompleted ? (
                        <button className="btn btn-primary w-full" onClick={onClose}>
                            关闭
                        </button>
                    ) : (
                        <div className="flex items-center gap-2 text-warning text-sm bg-warning-bg p-3 rounded-lg border border-warning-border">
                            <Info size={16} />
                            <span>同步正在后台运行，请勿刷新或关闭页面。</span>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

const formatTimestamp = (value?: number | null) => {
    const ts = Number(value || 0);
    if (!ts) return '-';
    return new Date(ts * 1000).toLocaleString();
};

const PrepaymentRecords = () => {
    const { toasts, showToast, removeToast } = useToast();

    const [items, setItems] = useState<PrepaymentRecord[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [totalRecords, setTotalRecords] = useState(0);
    const [totalAmount, setTotalAmount] = useState(0);
    const [isLoading, setIsLoading] = useState(true);

    const [searchQuery, setSearchQuery] = useState('');
    const [communityFilter, setCommunityFilter] = useState<string[]>([]);
    const [operateType, setOperateType] = useState<string>('');
    const [operateDateStart, setOperateDateStart] = useState('');
    const [operateDateEnd, setOperateDateEnd] = useState('');

    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [isCommunityDropdownOpen, setIsCommunityDropdownOpen] = useState(false);
    const communityDropdownRef = useRef<HTMLDivElement>(null);

    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);

    const [isSyncing, setIsSyncing] = useState(false);
    const [syncState, setSyncState] = useState({
        taskId: '',
        total: 0,
        current: 0,
        logs: [] as { message: string; type: 'success' | 'error' | 'info'; time: string }[],
        status: 'idle',
    });
    const pollingTimer = useRef<number | null>(null);

    const totalPages = useMemo(() => Math.max(1, Math.ceil(totalRecords / pageSize)), [totalRecords, pageSize]);

    const fetchProjects = useCallback(async () => {
        const response = await getProjects({ skip: 0, limit: 2000, current_account_book_only: true });
        setProjects(response?.items || response || []);
    }, []);

    const fetchPrepaymentList = useCallback(async () => {
        setIsLoading(true);
        try {
            const response = await getPrepaymentRecords({
                search: searchQuery || undefined,
                community_ids: communityFilter.length > 0 ? communityFilter.join(',') : undefined,
                operate_type: operateType ? Number(operateType) : undefined,
                operate_date_start: operateDateStart || undefined,
                operate_date_end: operateDateEnd || undefined,
                skip: (page - 1) * pageSize,
                limit: pageSize,
            });
            setItems(response.items || []);
            setTotalRecords(response.total || 0);
            setTotalAmount(response.total_amount || 0);
        } catch (error: any) {
            const message = error?.response?.data?.detail || error?.message || '加载失败';
            showToast('error', '预存款记录加载失败', typeof message === 'string' ? message : JSON.stringify(message));
        } finally {
            setIsLoading(false);
        }
    }, [communityFilter, operateDateEnd, operateDateStart, operateType, page, pageSize, searchQuery, showToast]);

    useEffect(() => {
        void fetchProjects();
    }, [fetchProjects]);

    useEffect(() => {
        void fetchPrepaymentList();
    }, [fetchPrepaymentList]);

    useEffect(() => {
        const validProjectIds = new Set(projects.map((project) => String(project.proj_id)));
        setCommunityFilter((prev) => {
            const next = prev.filter((id) => validProjectIds.has(String(id)));
            return next.length === prev.length ? prev : next;
        });
    }, [projects]);

    useEffect(() => {
        const handleOutsideClick = (event: MouseEvent) => {
            if (communityDropdownRef.current?.contains(event.target as Node)) return;
            setIsCommunityDropdownOpen(false);
        };
        document.addEventListener('mousedown', handleOutsideClick);
        return () => document.removeEventListener('mousedown', handleOutsideClick);
    }, []);

    const stopPolling = useCallback(() => {
        if (pollingTimer.current) {
            window.clearInterval(pollingTimer.current);
            pollingTimer.current = null;
        }
    }, []);

    const startPolling = useCallback((taskId: string) => {
        stopPolling();
        pollingTimer.current = window.setInterval(async () => {
            try {
                const status = await getPrepaymentRecordSyncStatus(taskId);
                setSyncState((prev) => ({
                    ...prev,
                    total: status.total_communities || 0,
                    current: status.current_community_index || 0,
                    logs: status.logs || [],
                    status: status.status || 'running',
                }));

                if (['completed', 'failed', 'partially_completed'].includes(status.status)) {
                    stopPolling();
                    await fetchPrepaymentList();
                }
            } catch {
                stopPolling();
            }
        }, 1000);
    }, [fetchPrepaymentList, stopPolling]);

    const handleSync = useCallback(async () => {
        try {
            setIsSyncing(true);
            setSyncState({
                taskId: '',
                total: communityFilter.length || projects.length || 1,
                current: 0,
                logs: [],
                status: 'pending',
            });
            const ids = communityFilter.length > 0 ? communityFilter.map((id) => Number(id)) : undefined;
            const result = await syncPrepaymentRecords(ids);
            setSyncState({
                taskId: result.task_id,
                total: result.community_ids?.length || communityFilter.length || projects.length || 1,
                current: 0,
                logs: [{ message: '同步任务已启动', type: 'info', time: new Date().toLocaleTimeString() }],
                status: 'running',
            });
            startPolling(result.task_id);
        } catch (error: any) {
            setIsSyncing(false);
            const message = error?.response?.data?.detail || error?.message || '启动同步失败';
            showToast('error', '同步失败', typeof message === 'string' ? message : JSON.stringify(message));
        }
    }, [communityFilter, projects.length, showToast, startPolling]);

    const columns = [
        { key: 'id', title: '记录ID' },
        { key: 'community_name', title: '园区' },
        { key: 'house_name', title: '房号' },
        { key: 'resident_name', title: '住户', render: (value: string) => value || '-' },
        { key: 'category_name', title: '预存款类别', render: (value: string) => value || '-' },
        {
            key: 'amount',
            title: '变动金额',
            render: (value: number) => `￥${Number(value || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        {
            key: 'balance_after_change',
            title: '变动后余额',
            render: (value: number) => `￥${Number(value || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        {
            key: 'operate_type_label',
            title: '变动类型',
            render: (_: any, row: PrepaymentRecord) => {
                const isRefund = Number(row.operate_type || 0) === 2;
                return (
                    <span
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            padding: '0.2rem 0.55rem',
                            borderRadius: '999px',
                            background: isRefund ? 'rgba(239, 68, 68, 0.12)' : 'rgba(22, 163, 74, 0.12)',
                            color: isRefund ? '#dc2626' : '#15803d',
                            fontSize: '0.75rem',
                            fontWeight: 600,
                        }}
                    >
                        {row.operate_type_label || '-'}
                    </span>
                );
            },
        },
        { key: 'operator_name', title: '操作人', render: (value: string) => value || '-' },
        {
            key: 'operate_time',
            title: '操作时间',
            render: (value: number) => formatTimestamp(value),
        },
        {
            key: 'pay_time',
            title: '支付时间',
            render: (value: number) => formatTimestamp(value),
        },
        {
            key: 'payment_id',
            title: '缴费ID',
            render: (value: number) => value || '-',
        },
        { key: 'pay_channel_str', title: '支付渠道', render: (value: string) => value || '-' },
        {
            key: 'has_refund_receipt',
            title: '退款收据',
            render: (value: boolean) => (value ? '是' : '否'),
        },
        {
            key: 'refund_receipt_id',
            title: '退款收款单ID',
            render: (value: number) => value || '-',
        },
        {
            key: 'remark',
            title: '备注',
            render: (value: string) => value || '-',
        },
    ];

    return (
        <div className="page-container">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <h3 className="font-bold text-slate-800">预存款管理</h3>
                        <span className="text-xs text-secondary">共 {totalRecords} 条</span>
                    </div>
                    <button className="collapse-toggle" onClick={() => setIsFilterCollapsed((value) => !value)}>
                        {isFilterCollapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                    </button>
                </div>

                {!isFilterCollapsed && (
                    <div className="filter-content-wrapper">
                        <div className="action-row flex-wrap">
                            <div className="flex items-center gap-2 flex-1 flex-wrap">
                                <div className="search-group" style={{ maxWidth: '260px' }}>
                                    <Search size={14} className="search-icon" />
                                    <input
                                        type="text"
                                        placeholder="搜索记录ID/缴费ID/房号/操作人/预存款类别..."
                                        value={searchQuery}
                                        onChange={(event) => {
                                            setSearchQuery(event.target.value);
                                            setPage(1);
                                        }}
                                    />
                                </div>

                                <div
                                    className="selection-group"
                                    ref={communityDropdownRef}
                                    style={{ maxWidth: '200px', minWidth: '180px' }}
                                >
                                    <div
                                        className={`custom-select-trigger ${isCommunityDropdownOpen ? 'active' : ''}`}
                                        onClick={() => setIsCommunityDropdownOpen((value) => !value)}
                                    >
                                        <div className="trigger-content">
                                            <span
                                                className={communityFilter.length === 0 ? 'placeholder' : 'text-xs truncate'}
                                                style={{ maxWidth: '130px' }}
                                            >
                                                {communityFilter.length === 0 ? '选择园区...' : `已选 ${communityFilter.length} 个`}
                                            </span>
                                        </div>
                                        <ChevronDown size={14} className={`arrow ${isCommunityDropdownOpen ? 'rotate' : ''}`} />
                                    </div>
                                    {isCommunityDropdownOpen && (
                                        <div className="custom-dropdown card-shadow slide-up" style={{ zIndex: 100, width: '240px' }}>
                                            <div className="dropdown-list custom-scrollbar" style={{ maxHeight: '220px' }}>
                                                {projects.map((project) => (
                                                    <div
                                                        key={project.proj_id}
                                                        className={`dropdown-item ${communityFilter.includes(String(project.proj_id)) ? 'selected' : ''}`}
                                                        onClick={(event) => {
                                                            event.stopPropagation();
                                                            setCommunityFilter((prev) =>
                                                                prev.includes(String(project.proj_id))
                                                                    ? prev.filter((value) => value !== String(project.proj_id))
                                                                    : [...prev, String(project.proj_id)],
                                                            );
                                                            setPage(1);
                                                        }}
                                                    >
                                                        <div className="checkbox">
                                                            {communityFilter.includes(String(project.proj_id)) && <div className="check-dot"></div>}
                                                        </div>
                                                        <div className="item-info">
                                                            <span className="name">{project.proj_name}</span>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                            <div className="p-1 flex justify-between bg-gray-50/50 border-t border-gray-100">
                                                <button className="btn-text text-xs" onClick={() => setCommunityFilter(projects.map((project) => String(project.proj_id)))}>
                                                    全选
                                                </button>
                                                <button className="btn-text text-xs" onClick={() => setCommunityFilter([])}>
                                                    清空
                                                </button>
                                            </div>
                                        </div>
                                    )}
                                </div>

                                <select
                                    className="enhanced-select"
                                    style={{ width: '120px' }}
                                    value={operateType}
                                    onChange={(event) => {
                                        setOperateType(event.target.value);
                                        setPage(1);
                                    }}
                                >
                                    <option value="">全部类型</option>
                                    <option value="1">充值</option>
                                    <option value="2">退款</option>
                                </select>

                                <div className="flex items-center gap-1">
                                    <span className="text-secondary text-xs" style={{ whiteSpace: 'nowrap' }}>
                                        操作日期:
                                    </span>
                                    <input
                                        type="date"
                                        className="enhanced-select text-xs"
                                        style={{ width: '140px' }}
                                        value={operateDateStart}
                                        onChange={(event) => {
                                            setOperateDateStart(event.target.value);
                                            setPage(1);
                                        }}
                                    />
                                    <span className="text-secondary">-</span>
                                    <input
                                        type="date"
                                        className="enhanced-select text-xs"
                                        style={{ width: '140px' }}
                                        value={operateDateEnd}
                                        onChange={(event) => {
                                            setOperateDateEnd(event.target.value);
                                            setPage(1);
                                        }}
                                    />
                                </div>
                            </div>

                            <button className="btn-primary btn-refresh-list" onClick={() => void handleSync()}>
                                <RefreshCw size={14} /> 同步预存款记录
                            </button>

                            <button
                                className="btn-outline"
                                style={{ color: '#ef4444' }}
                                onClick={() => {
                                    setSearchQuery('');
                                    setCommunityFilter([]);
                                    setOperateType('');
                                    setOperateDateStart('');
                                    setOperateDateEnd('');
                                    setPage(1);
                                }}
                            >
                                <X size={14} /> 重置筛选
                            </button>
                        </div>
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns as any}
                    data={items}
                    loading={isLoading}
                    serialStart={(page - 1) * pageSize + 1}
                    tableId="prepayment-records-list"
                />

                <div className="pagination-footer">
                    <div className="pagination-info" style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
                        <span>
                            显示 {(page - 1) * pageSize + 1} 到 {Math.min(page * pageSize, totalRecords)} 条，共 {totalRecords} 条
                        </span>
                        <span className="text-secondary">|</span>
                        <span>
                            金额合计:
                            <strong style={{ color: '#2563eb', marginLeft: '0.25rem' }}>
                                ￥{totalAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                            </strong>
                        </span>
                    </div>

                    <div className="pagination-controls">
                        <select
                            className="page-select"
                            value={pageSize}
                            onChange={(event) => {
                                setPageSize(Number(event.target.value));
                                setPage(1);
                            }}
                        >
                            <option value={10}>10 条/页</option>
                            <option value={25}>25 条/页</option>
                            <option value={50}>50 条/页</option>
                            <option value={100}>100 条/页</option>
                        </select>

                        <div className="flex gap-1 ml-2">
                            <button className="page-btn" disabled={page === 1} onClick={() => setPage(1)}>
                                <ChevronsLeft size={16} />
                            </button>
                            <button className="page-btn" disabled={page === 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>
                                <ChevronLeft size={16} />
                            </button>

                            <button className="page-btn active">{page}</button>
                            {page < totalPages && <button className="page-btn" onClick={() => setPage((value) => value + 1)}>{page + 1}</button>}
                            {page + 1 < totalPages && <span className="px-2 text-secondary">...</span>}
                            {page + 1 < totalPages && <button className="page-btn" onClick={() => setPage(totalPages)}>{totalPages}</button>}

                            <button
                                className="page-btn"
                                disabled={page === totalPages}
                                onClick={() => setPage((value) => Math.min(totalPages, value + 1))}
                            >
                                <ChevronRight size={16} />
                            </button>
                            <button className="page-btn" disabled={page === totalPages} onClick={() => setPage(totalPages)}>
                                <ChevronsRight size={16} />
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            <SyncProgressModal
                isOpen={isSyncing}
                onClose={() => {
                    setIsSyncing(false);
                    stopPolling();
                }}
                total={syncState.total}
                current={syncState.current}
                logs={syncState.logs}
                status={syncState.status}
            />

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

export default PrepaymentRecords;
