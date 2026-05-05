import { useState, useEffect, useRef, useCallback } from 'react';
import {
    RefreshCw,
    ChevronDown,
    X,
    Download,
    ChevronLeft,
    ChevronRight,
    ChevronsLeft,
    ChevronsRight
} from 'lucide-react';
import Select from '../../components/common/Select';
import DataTable from '../../components/data/DataTable';
import type { Bill, Project } from '../../types';
import './Bills.css';
import { getProjects, getBills, getBillChargeItems, exportBills } from '../../services/api';
import { useToast, ToastContainer } from '../../components/Toast';
import { useLocation } from 'react-router-dom';

type BillQueryParams = {
    status?: string;
    include_deleted?: boolean;
    community_ids?: string;
    charge_items?: string;
    search?: string;
    in_month_start?: string;
    in_month_end?: string;
    pay_date_start?: string;
    pay_date_end?: string;
    skip?: number;
    limit?: number;
};

const Bills = () => {
    const location = useLocation();
    const { toasts, showToast, removeToast } = useToast();
    const [bills, setBills] = useState<Bill[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [totalRecords, setTotalRecords] = useState(0);
    const [totalAmount, setTotalAmount] = useState(0);

    const [selectedBillIds, setSelectedBillIds] = useState<Set<string>>(new Set());
    const [selectedBillAmounts, setSelectedBillAmounts] = useState<Map<string, number>>(new Map());
    const [selectedBillRefs, setSelectedBillRefs] = useState<Map<string, { bill_id: number; community_id: number }>>(new Map());

    const [isLoading, setIsLoading] = useState(true);
    const [isExporting, setIsExporting] = useState(false);
    const [statusFilter, setStatusFilter] = useState('全部状态');
    const [includeDeleted, setIncludeDeleted] = useState(false);
    const [communityFilter, setCommunityFilter] = useState<string[]>([]);
    const [chargeItemFilter, setChargeItemFilter] = useState<string[]>([]);
    const [availableChargeItems, setAvailableChargeItems] = useState<{ value: string, label: string }[]>([]);
    const [keywordFilter, setKeywordFilter] = useState('');
    const [isFilterDropdownOpen, setIsFilterDropdownOpen] = useState(false);
    const [isChargeItemDropdownOpen, setIsChargeItemDropdownOpen] = useState(false);
    const filterDropdownRef = useRef<HTMLDivElement>(null);
    const chargeItemDropdownRef = useRef<HTMLDivElement>(null);

    // Date & Month filters
    const [inMonthStart, setInMonthStart] = useState('');
    const [inMonthEnd, setInMonthEnd] = useState('');
    const [payTimeStart, setPayTimeStart] = useState('');
    const [payTimeEnd, setPayTimeEnd] = useState('');
    // Quick selection state for UI
    const [quickInMonth, setQuickInMonth] = useState('');
    const [quickPayTime, setQuickPayTime] = useState('');

    const handleQuickDate = (
        option: string,
        setStart: (v: string) => void,
        setEnd: (v: string) => void,
        formatLevel: 'month' | 'date',
        setQuickState: (v: string) => void
    ) => {
        setQuickState(option);
        if (option === 'custom') return; // User inputs manually

        const now = new Date();
        const year = now.getFullYear();
        const month = now.getMonth();

        const formatDate = (date: Date) => {
            const m = String(date.getMonth() + 1).padStart(2, '0');
            if (formatLevel === 'month') return `${date.getFullYear()}-${m}`;
            const d = String(date.getDate()).padStart(2, '0');
            return `${date.getFullYear()}-${m}-${d}`;
        };

        if (option === 'today') {
            setStart(formatDate(new Date(year, month, now.getDate())));
            setEnd(formatDate(new Date(year, month, now.getDate())));
        } else if (option === 'this_month') {
            setStart(formatDate(new Date(year, month, 1)));
            setEnd(formatDate(new Date(year, month + 1, 0)));
        } else if (option === 'last_month') {
            setStart(formatDate(new Date(year, month - 1, 1)));
            setEnd(formatDate(new Date(year, month, 0)));
        } else if (option === 'this_quarter') {
            const qMonth = Math.floor(month / 3) * 3;
            setStart(formatDate(new Date(year, qMonth, 1)));
            setEnd(formatDate(new Date(year, qMonth + 3, 0)));
        } else if (option === 'this_year') {
            setStart(formatDate(new Date(year, 0, 1)));
            setEnd(formatDate(new Date(year, 11, 31)));
        } else if (option === '') {
            setStart('');
            setEnd('');
        }
    };

    // Allow deep-link from receipt bills: /bills?deal_log_id=...&community_ids=...
    useEffect(() => {
        const sp = new URLSearchParams(location.search || '');
        const dealLogId = (sp.get('deal_log_id') || '').trim();
        const communityIds = (sp.get('community_ids') || '').trim();

        if (dealLogId && dealLogId !== keywordFilter) {
            setKeywordFilter(dealLogId);
        }
        if (!dealLogId && keywordFilter) {
            setKeywordFilter('');
        }

        if (communityIds) {
            const ids = communityIds.split(',').map(s => s.trim()).filter(Boolean);
            const normalized = ids.sort().join(',');
            const current = [...communityFilter].sort().join(',');
            if (normalized !== current) {
                setCommunityFilter(ids);
            }
        }
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location.search]);


    // UI state

    // Pagination State
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);

    const buildBillSelectionKey = (billId: string | number, communityId?: number) => `${communityId ?? ''}|${billId}`;

    const getBillQueryParams = useCallback(() => {
        const params: BillQueryParams = {
            /*
        status: statusFilter !== '鍏ㄩ儴鐘舵€? ? statusFilter : undefined,
        status: statusFilter !== '全部状态' ? statusFilter : undefined,
            */
            status: statusFilter !== '全部状态' ? statusFilter : undefined,
        include_deleted: includeDeleted || undefined,
        community_ids: communityFilter.length > 0 ? communityFilter.join(',') : undefined,
        charge_items: chargeItemFilter.length > 0 ? chargeItemFilter.join(',') : undefined,
        search: keywordFilter || undefined,
        in_month_start: inMonthStart || undefined,
        in_month_end: inMonthEnd || undefined,
        pay_date_start: payTimeStart || undefined,
        pay_date_end: payTimeEnd || undefined,
        };
        return params;
    }, [statusFilter, includeDeleted, communityFilter, chargeItemFilter, keywordFilter, inMonthStart, inMonthEnd, payTimeStart, payTimeEnd]);

    const fetchBillsList = useCallback(async () => {
        setIsLoading(true);
        try {
            const params: BillQueryParams = {
                ...getBillQueryParams(),
                skip: (page - 1) * pageSize,
                limit: pageSize,
                status: statusFilter !== '全部状态' ? statusFilter : undefined,
                include_deleted: includeDeleted || undefined,
                community_ids: communityFilter.length > 0 ? communityFilter.join(',') : undefined,
                charge_items: chargeItemFilter.length > 0 ? chargeItemFilter.join(',') : undefined,
                search: keywordFilter || undefined,
                in_month_start: inMonthStart || undefined,
                in_month_end: inMonthEnd || undefined,
                pay_date_start: payTimeStart || undefined,
                pay_date_end: payTimeEnd || undefined,
            };
            const res = await getBills(params);
            if (res && res.items) {
                setBills(res.items);
                setTotalRecords(res.total);
                setTotalAmount(res.total_amount || 0);
            } else if (Array.isArray(res)) {
                setBills(res);
                setTotalRecords(res.length);
                setTotalAmount(0);
            }
        } catch (error) {
            console.error('Failed to fetch bills:', error);
            setBills([]);
        } finally {
            setIsLoading(false);
        }
    }, [page, pageSize, keywordFilter, statusFilter, includeDeleted, communityFilter, chargeItemFilter, inMonthStart, inMonthEnd, payTimeStart, payTimeEnd, getBillQueryParams]);

    useEffect(() => {
        fetchBillsList();
        loadProjects();
        loadChargeItems();

        const handleClickOutside = (event: MouseEvent) => {
            if (filterDropdownRef.current && !filterDropdownRef.current.contains(event.target as Node)) {
                setIsFilterDropdownOpen(false);
            }
            if (chargeItemDropdownRef.current && !chargeItemDropdownRef.current.contains(event.target as Node)) {
                setIsChargeItemDropdownOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [fetchBillsList]);

    const loadProjects = async () => {
        try {
            const data = await getProjects({ skip: 0, limit: 2000, current_account_book_only: true });
            setProjects(data.items || data);
        } catch (e) {
            console.error('Failed to load projects:', e);
        }
    };

    useEffect(() => {
        const validProjectIds = new Set(projects.map(project => String(project.proj_id)));

        setCommunityFilter(prev => {
            const next = prev.filter(id => validProjectIds.has(String(id)));
            return next.length === prev.length ? prev : next;
        });
    }, [projects]);

    const loadChargeItems = async () => {
        try {
            const items = await getBillChargeItems();
            setAvailableChargeItems(items);
        } catch (e) {
            console.error('Failed to load charge items:', e);
        }
    };

    const handleExport = async () => {
        setIsExporting(true);
        try {
            const { blob, filename } = await exportBills(getBillQueryParams());
            const downloadUrl = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = downloadUrl;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
            showToast('success', '导出成功', '运营账单已开始下载');
        } catch (error) {
            console.error('Failed to export bills:', error);
            showToast('error', '导出失败', '运营账单导出失败，请稍后重试');
        } finally {
            setIsExporting(false);
        }
    };

    const filteredBills = bills; // Filtering is now handled by backend

    const columns = [
        {
            key: '_selection',
            width: 40,
            title: (
                <input
                    type="checkbox"
                    checked={bills.length > 0 && bills.every(b => selectedBillIds.has(buildBillSelectionKey(b.id, b.community_id)))}
                    onChange={(e) => {
                        const checked = e.target.checked;
                        const newIds = new Set(selectedBillIds);
                        const newAmounts = new Map(selectedBillAmounts);
                        const newRefs = new Map(selectedBillRefs);
                        bills.forEach(b => {
                            const key = buildBillSelectionKey(b.id, b.community_id);
                            const communityId = Number(b.community_id);
                            const billId = Number(b.id);
                            if (checked) {
                                newIds.add(key);
                                newAmounts.set(key, Number(b.amount || 0));
                                if (Number.isFinite(communityId) && Number.isFinite(billId)) {
                                    newRefs.set(key, { bill_id: billId, community_id: communityId });
                                }
                            } else {
                                newIds.delete(key);
                                newAmounts.delete(key);
                                newRefs.delete(key);
                            }
                        });
                        setSelectedBillIds(newIds);
                        setSelectedBillAmounts(newAmounts);
                        setSelectedBillRefs(newRefs);
                    }}
                />
            ),
            render: (_value: unknown, row: Bill) => (
                <input
                    type="checkbox"
                    checked={selectedBillIds.has(buildBillSelectionKey(row.id, row.community_id))}
                    onChange={(e) => {
                        const checked = e.target.checked;
                        const newIds = new Set(selectedBillIds);
                        const newAmounts = new Map(selectedBillAmounts);
                        const newRefs = new Map(selectedBillRefs);
                        const key = buildBillSelectionKey(row.id, row.community_id);
                        const communityId = Number(row.community_id);
                        const billId = Number(row.id);
                        if (checked) {
                            newIds.add(key);
                            newAmounts.set(key, Number(row.amount || 0));
                            if (Number.isFinite(communityId) && Number.isFinite(billId)) {
                                newRefs.set(key, { bill_id: billId, community_id: communityId });
                            }
                        } else {
                            newIds.delete(key);
                            newAmounts.delete(key);
                            newRefs.delete(key);
                        }
                        setSelectedBillIds(newIds);
                        setSelectedBillAmounts(newAmounts);
                        setSelectedBillRefs(newRefs);
                    }}
                />
            )
        },
        {
            key: 'no' as keyof Bill,
            title: '序号',
            width: 50,
            render: (_value: unknown, _record: Bill, index: number) => (page - 1) * pageSize + index + 1
        },
        { key: 'id' as keyof Bill, title: '账单ID', width: 120 },
        {
            key: 'deal_log_id' as keyof Bill,
            title: '缴费ID',
            width: 120,
            render: (val: unknown) => <span className="text-secondary text-sm">{val == null ? '-' : String(val)}</span>,
        },
        { key: 'community_name' as keyof Bill, title: '园区', width: 150 },
        { key: 'asset_name' as keyof Bill, title: '资产名称' },
        { key: 'customer_name' as keyof Bill, title: '客户名称' },

        { key: 'charge_item_name' as keyof Bill, title: '收费项目' },
        { key: 'in_month' as keyof Bill, title: '所属月份' },
        {
            key: 'amount' as keyof Bill,
            title: '收款金额',
            render: (val: unknown) => <span className="font-medium">¥{Number(val).toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>,
        },
        {
            key: 'pay_status_str' as keyof Bill,
            title: '收费状态',
            render: (val: unknown) => (
                <span className={`badge ${val === '已缴' ? 'success' : 'warning'}`}>{String(val ?? '')}</span>
            ),
        },
        {
            key: 'source_deleted' as keyof Bill,
            title: '源端状态',
            width: 110,
            render: (_value: unknown, row: Bill) => (
                <span className={`badge ${row.source_deleted ? 'warning' : 'success'}`}>
                    {row.source_deleted ? '已源端删除' : '正常'}
                </span>
            ),
        },
        {
            key: 'receive_date' as keyof Bill,
            title: '支付日期',
            render: (val: unknown) => <span className="text-secondary text-sm">{String(val || '-')}</span>,
        },
        {
            key: 'last_seen_at' as keyof Bill,
            title: '最后见到时间',
            render: (val: unknown) => <span className="text-secondary text-sm">{val ? new Date(String(val)).toLocaleString() : '-'}</span>,
        },
        {
            key: 'created_at' as keyof Bill,
            title: '创建时间',
            render: (val: unknown) => <span className="text-secondary text-sm">{val ? new Date(String(val)).toLocaleDateString() : '-'}</span>,
        },
    ];

    const totalPages = Math.ceil(totalRecords / pageSize);

    const currentPageAmount = bills.reduce((sum, bill) => sum + Number(bill.amount || 0), 0);
    const selectedTotalAmount = Array.from(selectedBillAmounts.values()).reduce((a, b) => a + b, 0);

    return (
        <div className="page-container fade-in bills-page">
            {/* Filter Section - Collapsible */}
            <div className="bills-filter-section">
                <div className="filter-content-wrapper">
                    <div className="filter-row">
                        <div className="flex items-center gap-2 flex-1 flex-wrap">
                            <div className="selection-group" ref={filterDropdownRef} style={{ maxWidth: '160px', minWidth: '150px' }}>
                                <div className={`custom-select-trigger ${isFilterDropdownOpen ? 'active' : ''}`} onClick={() => setIsFilterDropdownOpen(!isFilterDropdownOpen)}>
                                    <div className="trigger-content">
                                        <span className={communityFilter.length === 0 ? 'placeholder' : 'text-xs truncate'} style={{ maxWidth: '100px' }}>
                                            {communityFilter.length === 0 ? '选择园区...' : `已选 ${communityFilter.length} 个`}
                                        </span>
                                    </div>
                                    <ChevronDown size={14} className={`arrow ${isFilterDropdownOpen ? 'rotate' : ''}`} />
                                </div>
                                {isFilterDropdownOpen && (
                                    <div className="custom-dropdown card-shadow slide-up" style={{ zIndex: 100, width: '200px' }}>
                                        <div className="dropdown-list custom-scrollbar" style={{ maxHeight: '200px' }}>
                                            {projects.map(p => (
                                                <div key={p.proj_id} className={`dropdown-item ${communityFilter.includes(String(p.proj_id)) ? 'selected' : ''}`} onClick={(e) => {
                                                    e.stopPropagation();
                                                    setCommunityFilter(prev => prev.includes(String(p.proj_id)) ? prev.filter(x => x !== String(p.proj_id)) : [...prev, String(p.proj_id)]);
                                                }}>
                                                    <div className="checkbox">{communityFilter.includes(String(p.proj_id)) && <div className="check-dot"></div>}</div>
                                                    <div className="item-info"><span className="name">{p.proj_name}</span></div>
                                                </div>
                                            ))}
                                        </div>
                                        <div className="p-1 flex justify-between bg-gray-50/50 border-t border-gray-100">
                                            <button className="btn-text text-xs" onClick={() => setCommunityFilter(projects.map(p => String(p.proj_id)))}>全选</button>
                                            <button className="btn-text text-xs" onClick={() => setCommunityFilter([])}>清空</button>
                                        </div>
                                    </div>
                                )}
                            </div>

                            <div className="search-group" style={{ maxWidth: '360px', minWidth: '260px' }}>
                                <input
                                    type="text"
                                    placeholder="搜索账单ID/缴费ID/收据ID/房号/客户名称..."
                                    className="filter-input"
                                    value={keywordFilter}
                                    onChange={(e) => {
                                        setKeywordFilter(e.target.value);
                                        setPage(1);
                                    }}
                                />
                            </div>

                            <div className="selection-group" ref={chargeItemDropdownRef} style={{ maxWidth: '160px', minWidth: '150px' }}>
                                <div className={`custom-select-trigger ${isChargeItemDropdownOpen ? 'active' : ''}`} onClick={() => setIsChargeItemDropdownOpen(!isChargeItemDropdownOpen)}>
                                    <div className="trigger-content">
                                        <span className={chargeItemFilter.length === 0 ? 'placeholder' : 'text-xs truncate'} style={{ maxWidth: '100px' }}>
                                            {chargeItemFilter.length === 0 ? '选择收费项目...' : `已选 ${chargeItemFilter.length} 项`}
                                        </span>
                                    </div>
                                    <ChevronDown size={14} className={`arrow ${isChargeItemDropdownOpen ? 'rotate' : ''}`} />
                                </div>
                                {isChargeItemDropdownOpen && (
                                    <div className="custom-dropdown card-shadow slide-up" style={{ zIndex: 100, width: '300px' }}>
                                        <div className="dropdown-list custom-scrollbar" style={{ maxHeight: '200px' }}>
                                            {availableChargeItems.map(item => (
                                                <div key={item.value} className={`dropdown-item ${chargeItemFilter.includes(item.value) ? 'selected' : ''}`} onClick={(e) => {
                                                    e.stopPropagation();
                                                    setChargeItemFilter(prev => prev.includes(item.value) ? prev.filter(x => x !== item.value) : [...prev, item.value]);
                                                }}>
                                                    <div className="checkbox">{chargeItemFilter.includes(item.value) && <div className="check-dot"></div>}</div>
                                                    <div className="item-info"><span className="name" style={{ fontSize: '0.75rem' }}>{item.label}</span></div>
                                                </div>
                                            ))}
                                            {availableChargeItems.length === 0 && (
                                                <div className="p-3 text-center text-secondary text-xs">暂无可用收费项目</div>
                                            )}
                                        </div>
                                        {availableChargeItems.length > 0 && (
                                            <div className="p-1 flex justify-between bg-gray-50/50 border-t border-gray-100">
                                                <button className="btn-text text-xs" onClick={() => setChargeItemFilter(availableChargeItems.map(i => i.value))}>全选</button>
                                                <button className="btn-text text-xs" onClick={() => setChargeItemFilter([])}>清空</button>
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>

                            <select
                                className="enhanced-select"
                                style={{ width: '120px' }}
                                value={statusFilter}
                                onChange={(e) => setStatusFilter(e.target.value)}
                            >
                                <option>全部状态</option>
                                <option>已缴</option>
                                <option>待缴</option>
                            </select>

                            <label className="flex items-center gap-2 text-xs text-secondary" style={{ whiteSpace: 'nowrap' }}>
                                <input
                                    type="checkbox"
                                    checked={includeDeleted}
                                    onChange={(e) => {
                                        setIncludeDeleted(e.target.checked);
                                        setPage(1);
                                    }}
                                />
                                显示源端已删
                            </label>

                            <div className="flex items-center gap-1">
                                <span className="text-secondary text-xs" style={{ whiteSpace: 'nowrap' }}>所属月份:</span>
                                <Select
                                    className="enhanced-select text-xs"
                                    style={{ width: '90px' }}
                                    value={quickInMonth}
                                    onChange={(v) => handleQuickDate(v, setInMonthStart, setInMonthEnd, 'month', setQuickInMonth)}
                                    options={[
                                        { value: '', label: '全部' },
                                        { value: 'this_month', label: '本月' },
                                        { value: 'last_month', label: '上月' },
                                        { value: 'this_quarter', label: '本季度' },
                                        { value: 'this_year', label: '本年' },
                                        { value: 'custom', label: '范围' },
                                    ]}
                                />
                                {quickInMonth === 'custom' && (
                                    <div className="flex items-center gap-1">
                                        <input type="month" className="enhanced-select text-xs" style={{ width: '110px' }} value={inMonthStart} onChange={(e) => setInMonthStart(e.target.value)} />
                                        <span className="text-secondary">-</span>
                                        <input type="month" className="enhanced-select text-xs" style={{ width: '110px' }} value={inMonthEnd} onChange={(e) => setInMonthEnd(e.target.value)} />
                                    </div>
                                )}
                            </div>

                            <div className="flex items-center gap-1">
                                <span className="text-secondary text-xs" style={{ whiteSpace: 'nowrap' }}>支付日期:</span>
                                <Select
                                    className="enhanced-select text-xs"
                                    style={{ width: '90px' }}
                                    value={quickPayTime}
                                    onChange={(v) => handleQuickDate(v, setPayTimeStart, setPayTimeEnd, 'date', setQuickPayTime)}
                                    options={[
                                        { value: '', label: '全部' },
                                        { value: 'today', label: '今日' },
                                        { value: 'this_month', label: '本月' },
                                        { value: 'last_month', label: '上月' },
                                        { value: 'this_quarter', label: '本季度' },
                                        { value: 'this_year', label: '本年' },
                                        { value: 'custom', label: '范围' },
                                    ]}
                                />
                                {quickPayTime === 'custom' && (
                                    <div className="flex items-center gap-1">
                                        <input type="date" className="enhanced-select text-xs" style={{ width: '120px' }} value={payTimeStart} onChange={(e) => setPayTimeStart(e.target.value)} />
                                        <span className="text-secondary">-</span>
                                        <input type="date" className="enhanced-select text-xs" style={{ width: '120px' }} value={payTimeEnd} onChange={(e) => setPayTimeEnd(e.target.value)} />
                                    </div>
                                )}
                                <button className="btn-outline" style={{ color: '#ef4444' }} onClick={() => {
                                    setCommunityFilter([]);
                                    setStatusFilter('全部状态');
                                    setIncludeDeleted(false);
                                    setChargeItemFilter([]);
                                    setKeywordFilter('');
                                    setQuickInMonth('');
                                    setInMonthStart('');
                                    setInMonthEnd('');
                                    setQuickPayTime('');
                                    setPayTimeStart('');
                                    setPayTimeEnd('');
                                }}>
                                    <X size={14} /> 重置
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Table Area with Pagination */}
            <div className="table-area-wrapper">
                <DataTable
                        columns={columns}
                        data={filteredBills}
                        loading={isLoading}
                        tableId="bills-main-list"
                        enableColumnSettings={true}
                        toolbar={
                            <>
                                <button className={`btn-outline ${isExporting ? 'disabled' : ''}`} onClick={handleExport} disabled={isExporting}>
                                    <Download size={14} /> {isExporting ? '导出中...' : '导出账单'}
                                </button>
                                <button className="btn-outline btn-refresh-list" onClick={fetchBillsList}>
                                    <RefreshCw size={14} /> 刷新列表
                                </button>
                            </>
                        }
                    />

                <div className="pagination-footer">
                    <div className="pagination-info" style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
                        <span>
                            显示 {(page - 1) * pageSize + 1} 到 {Math.min(page * pageSize, totalRecords)} 条，共 {totalRecords} 条记录
                        </span>
                        <span className="text-secondary">|</span>
                        <span>本页小计: <strong style={{ color: '#8b5cf6' }}>¥{currentPageAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}</strong></span>
                        <span className="text-secondary">|</span>
                        <span>列表金额总计: <strong style={{ color: '#2563eb' }}>¥{totalAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}</strong></span>
                        {selectedBillIds.size > 0 && (
                            <>
                                <span className="text-secondary">|</span>
                                <span>已选({selectedBillIds.size}): <strong style={{ color: '#10b981' }}>¥{selectedTotalAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}</strong></span>
                            </>
                        )}
                    </div>

                    <div className="pagination-controls">

                        <Select
                            className="page-select"
                            value={String(pageSize)}
                            onChange={(v) => { setPageSize(Number(v)); setPage(1); }}
                            options={[
                                { value: '10', label: '10 条/页' },
                                { value: '25', label: '25 条/页' },
                                { value: '50', label: '50 条/页' },
                                { value: '100', label: '100 条/页' },
                            ]}
                        />

                        <div className="flex gap-1 ml-2">
                            <button className="page-btn" disabled={page === 1} onClick={() => setPage(1)}><ChevronsLeft size={16} /></button>
                            <button className="page-btn" disabled={page === 1} onClick={() => setPage(p => Math.max(1, p - 1))}><ChevronLeft size={16} /></button>

                            <button className="page-btn active">{page}</button>
                            {page < totalPages && <button className="page-btn" onClick={() => setPage(p => p + 1)}>{page + 1}</button>}
                            {page + 1 < totalPages && <span className="px-2 text-secondary">...</span>}
                            {page + 1 < totalPages && <button className="page-btn" onClick={() => setPage(totalPages)}>{totalPages}</button>}

                            <button className="page-btn" disabled={page === totalPages} onClick={() => setPage(p => Math.min(totalPages, p + 1))}><ChevronRight size={16} /></button>
                            <button className="page-btn" disabled={page === totalPages} onClick={() => setPage(totalPages)}><ChevronsRight size={16} /></button>
                        </div>
                    </div>
                </div>
            </div>

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

export default Bills;


