import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import {
    ChevronDown,
    ChevronLeft,
    ChevronRight,
    ChevronsLeft,
    ChevronsRight,
    Search,
    X,
    Download,
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import { ToastContainer, useToast } from '../../components/Toast';
import type { DepositRecord, Project } from '../../types';
import { getDepositRecords, getProjects } from '../../services/api';
import '../bills/Bills.css';
import { exportTableToCsv } from '../../utils/export';

const formatTimestamp = (value?: number | null) => {
    const ts = Number(value || 0);
    if (!ts) return '-';
    return new Date(ts * 1000).toLocaleString();
};

type DepositRecordColumn = {
    key: keyof DepositRecord | string;
    title: string;
    render?: (value: unknown, row: DepositRecord) => ReactNode;
};

const DepositRecords = () => {
    const { toasts, showToast, removeToast } = useToast();

    const [items, setItems] = useState<DepositRecord[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [totalRecords, setTotalRecords] = useState(0);
    const [totalAmount, setTotalAmount] = useState(0);
    const [isLoading, setIsLoading] = useState(true);

    const [searchQuery, setSearchQuery] = useState('');
    const [communityFilter, setCommunityFilter] = useState<string[]>([]);
    const [operateType, setOperateType] = useState<string>('');
    const [operateDateStart, setOperateDateStart] = useState('');
    const [operateDateEnd, setOperateDateEnd] = useState('');

    const [isCommunityDropdownOpen, setIsCommunityDropdownOpen] = useState(false);
    const [isExportMenuOpen, setIsExportMenuOpen] = useState(false);
    const communityDropdownRef = useRef<HTMLDivElement>(null);

    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);

    const totalPages = useMemo(() => Math.max(1, Math.ceil(totalRecords / pageSize)), [totalRecords, pageSize]);

    const fetchProjects = useCallback(async () => {
        const response = await getProjects({ skip: 0, limit: 2000, current_account_book_only: true });
        setProjects(response?.items || response || []);
    }, []);

    const fetchDepositList = useCallback(async () => {
        setIsLoading(true);
        try {
            const response = await getDepositRecords({
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
        } catch (error: unknown) {
            const errorRecord = error as { response?: { data?: { detail?: unknown } }; message?: unknown };
            const message = errorRecord?.response?.data?.detail || errorRecord?.message || '加载失败';
            showToast('error', '押金记录加载失败', typeof message === 'string' ? message : JSON.stringify(message));
        } finally {
            setIsLoading(false);
        }
    }, [communityFilter, operateDateEnd, operateDateStart, operateType, page, pageSize, searchQuery, showToast]);

    useEffect(() => {
        void fetchProjects();
    }, [fetchProjects]);

    useEffect(() => {
        void fetchDepositList();
    }, [fetchDepositList]);

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
            if ((event.target as HTMLElement)?.closest('.deposit-export-action')) return;
            setIsCommunityDropdownOpen(false);
            setIsExportMenuOpen(false);
        };
        document.addEventListener('mousedown', handleOutsideClick);
        return () => document.removeEventListener('mousedown', handleOutsideClick);
    }, []);

    const columns = useMemo<DepositRecordColumn[]>(() => [
        { key: 'id', title: '记录ID' },
        { key: 'community_name', title: '园区' },
        { key: 'house_name', title: '房号' },
        { key: 'resident_name', title: '住户', render: (value) => String(value || '-') },
        { key: 'cash_pledge_name', title: '押金类型' },
        {
            key: 'amount',
            title: '金额',
            render: (value) => `¥${Number(value || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        {
            key: 'operate_type_label',
            title: '变动类型',
            render: (_value, row) => {
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
        { key: 'operator_name', title: '操作人' },
        {
            key: 'operate_time',
            title: '操作时间',
            render: (value) => formatTimestamp(value as number | null | undefined),
        },
        {
            key: 'pay_time',
            title: '支付时间',
            render: (value) => formatTimestamp(value as number | null | undefined),
        },
        {
            key: 'payment_id',
            title: '缴费ID',
            render: (value) => String(value || '-'),
        },
        { key: 'pay_channel_str', title: '支付渠道', render: (value) => String(value || '-') },
        {
            key: 'has_refund_receipt',
            title: '退款收据',
            render: (value) => (value ? '是' : '否'),
        },
        {
            key: 'refund_receipt_id',
            title: '退款收据ID',
            render: (value) => String(value || '-'),
        },
        {
            key: 'remark',
            title: '备注',
            render: (value) => String(value || '-'),
        },
    ], []);

    const handleExportDepositRecords = useCallback(async (mode: 'current' | 'all') => {
        try {
            let exportData = items;
            
            if (mode === 'all') {
                showToast('info', '正在导出', '正在加载全部数据...');
                const allItems: DepositRecord[] = [];
                const batchSize = 100;
                let skip = 0;
                let hasMore = true;
                
                while (hasMore) {
                    const params = {
                        search: searchQuery || undefined,
                        community_ids: communityFilter.length > 0 ? communityFilter.join(',') : undefined,
                        operate_type: operateType ? Number(operateType) : undefined,
                        operate_date_start: operateDateStart || undefined,
                        operate_date_end: operateDateEnd || undefined,
                        skip,
                        limit: batchSize,
                    };
                    const resp = await getDepositRecords(params);
                    allItems.push(...(resp.items || []));
                    skip += batchSize;
                    hasMore = resp.items.length === batchSize;
                }
                exportData = allItems;
            }
            
            const exportColumns = columns.filter((col) => !String(col.key).startsWith('_'));
            exportTableToCsv(exportColumns, exportData as unknown as Array<Record<string, unknown>>, '押金管理');
            showToast('success', '导出成功', `已导出 ${exportData.length} 条数据`);
        } catch (error) {
            console.error('导出失败:', error);
            showToast('error', '导出失败', '请稍后重试');
        }
    }, [columns, communityFilter, operateDateEnd, operateDateStart, operateType, searchQuery, showToast, items]);

    return (
        <div className="page-container deposit-records-page">
            <div className="bills-filter-section">
                <div className="action-row flex-wrap">
                            <div className="flex items-center gap-2 flex-1 flex-wrap">
                                <div className="search-group" style={{ maxWidth: '260px' }}>
                                    <Search size={14} className="search-icon" />
                                    <input
                                        type="text"
                                        placeholder="搜索记录ID/缴费ID/房号/操作人/押金类型..."
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
                                    <option value="1">收取</option>
                                    <option value="2">退还</option>
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

                            <div className="deposit-export-action" style={{ position: 'relative' }}>
                                <button
                                    className={`btn-outline ${isExportMenuOpen ? 'open' : ''}`}
                                    onClick={() => setIsExportMenuOpen(v => !v)}
                                    style={{ height: '34px', padding: '0 0.75rem', fontSize: '0.8rem' }}
                                >
                                    <Download size={14} /> 导出
                                    <ChevronDown size={12} style={{ marginLeft: '0.25rem' }} className={isExportMenuOpen ? 'rotate' : ''} />
                                </button>

                                {isExportMenuOpen && (
                                    <div
                                        className="custom-dropdown card-shadow slide-up"
                                        style={{
                                            position: 'absolute',
                                            top: 'calc(100% + 0.35rem)',
                                            right: 0,
                                            width: '140px',
                                            zIndex: 130,
                                            padding: '0.35rem 0',
                                        }}
                                        onClick={(e) => e.stopPropagation()}
                                    >
                                        <button
                                            type="button"
                                            className="dropdown-item"
                                            style={{
                                                width: '100%',
                                                border: 'none',
                                                background: 'transparent',
                                                textAlign: 'left',
                                                fontSize: '0.8rem',
                                                color: '#334155',
                                                cursor: 'pointer',
                                            }}
                                            onClick={() => {
                                                setIsExportMenuOpen(false);
                                                void handleExportDepositRecords('current');
                                            }}
                                        >
                                            导出当前页
                                        </button>
                                        <button
                                            type="button"
                                            className="dropdown-item"
                                            style={{
                                                width: '100%',
                                                border: 'none',
                                                background: 'transparent',
                                                textAlign: 'left',
                                                fontSize: '0.8rem',
                                                color: '#334155',
                                                cursor: 'pointer',
                                            }}
                                            onClick={() => {
                                                setIsExportMenuOpen(false);
                                                void handleExportDepositRecords('all');
                                            }}
                                        >
                                            导出全部
                                        </button>
                                    </div>
                                )}
                            </div>

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

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns}
                    data={items}
                    loading={isLoading}
                    serialStart={(page - 1) * pageSize + 1}
                    tableId="deposit-records-list"
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
                                ¥{totalAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}
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

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

export default DepositRecords;
