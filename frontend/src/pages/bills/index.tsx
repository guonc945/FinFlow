import { useState, useEffect, useRef, useCallback } from 'react';
import {
    RefreshCw,
    Search,
    ChevronDown,
    X,
    Filter,
    Info,
    ChevronLeft,
    ChevronRight,
    ChevronsLeft,
    ChevronsRight,
    ChevronUp
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import type { Bill, Project } from '../../types';
import './Bills.css';
import { syncBills, getProjects, getSyncStatus, getBills, getBillChargeItems } from '../../services/api';
import { useToast, ToastContainer } from '../../components/Toast';
import { useLocation } from 'react-router-dom';

// --- Sub-component: SyncProgressModal ---
const SyncProgressModal = ({
    isOpen,
    onClose,
    total,
    current,
    logs,
    status
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

    const isCompleted = ["completed", "failed", "partially_completed"].includes(status);
    const isFailed = status === "failed";
    const percentage = Math.round((current / total) * 100) || 0;

    return (
        <div className="sync-overlay">
            <div className="sync-modal">
                <div className="sync-header">
                    <div className="sync-title">
                        {isCompleted ? (
                            isFailed ? (
                                <div className="status-icon-error text-error"><X size={24} /></div>
                            ) : (
                                <div className="status-icon-success text-success"><CheckCircle size={24} /></div>
                            )
                        ) : (
                            <div className="status-icon-rotating text-primary"><RefreshCw size={24} /></div>
                        )}
                        <div>
                            <h3>{isCompleted ? (isFailed ? '同步失败' : '同步完成') : '正在同步账单数据'}</h3>
                            <p className="text-secondary text-sm">
                                {isCompleted
                                    ? (isFailed ? '同步过程中发生错误' : `已成功完成 ${total} 个园区的同步`)
                                    : `正在处理园区数据（共 ${total} 个）`}
                            </p>
                        </div>
                    </div>
                </div>

                <div className="sync-content">
                    <div className="progress-container">
                        <div className="progress-info">
                            <span className="font-bold text-primary">{percentage}%</span>
                            <span className="text-secondary text-sm">{current} / {total} 园区</span>
                        </div>
                        <div className="progress-bar-bg">
                            <div className={`progress-bar-fill ${isFailed ? 'bg-error' : ''}`} style={{ width: `${percentage}%` }}></div>
                        </div>
                    </div>

                    <div className="log-container" ref={logRef}>
                        {logs.map((log, idx) => (
                            <div key={idx} className={`log-item ${log.type}`}>
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
                            {isFailed ? '关闭' : '完成并关闭'}
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

// Internal CheckCircle for the modal since I removed it from main imports partially
const CheckCircle = ({ size, className }: { size: number, className?: string }) => (
    <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className || ''}><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>
);

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
    const [statusFilter, setStatusFilter] = useState('全部状态');
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
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [isConditionCollapsed, setIsConditionCollapsed] = useState(false);

    // Pagination State
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);

    // Selection State
    const [selectedProjectIds, setSelectedProjectIds] = useState<string[]>([]);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [projectSearch, setProjectSearch] = useState('');
    const dropdownRef = useRef<HTMLDivElement>(null);

    // Sync Progress State
    const [isSyncing, setIsSyncing] = useState(false);
    const [syncState, setSyncState] = useState({
        taskId: '',
        total: 0,
        current: 0,
        logs: [] as { message: string; type: 'success' | 'error' | 'info'; time: string }[],
        status: 'idle'
    });
    const pollingTimer = useRef<any>(null);

    const buildBillSelectionKey = (billId: string | number, communityId?: number) => `${communityId ?? ''}|${billId}`;

    const fetchBillsList = useCallback(async () => {
        setIsLoading(true);
        try {
            const params: any = {
                skip: (page - 1) * pageSize,
                limit: pageSize,
                status: statusFilter !== '全部状态' ? statusFilter : undefined,
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
    }, [page, pageSize, keywordFilter, statusFilter, communityFilter, chargeItemFilter, inMonthStart, inMonthEnd, payTimeStart, payTimeEnd]);

    useEffect(() => {
        fetchBillsList();
        loadProjects();
        loadChargeItems();

        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsDropdownOpen(false);
            }
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
            if (pollingTimer.current) clearTimeout(pollingTimer.current);
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

        setSelectedProjectIds(prev => {
            const next = prev.filter(id => validProjectIds.has(String(id)));
            return next.length === prev.length ? prev : next;
        });

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

    const startPolling = (taskId: string) => {
        const poll = async () => {
            try {
                const statusData = await getSyncStatus(taskId);
                setSyncState(prev => ({
                    ...prev,
                    current: statusData.current_community_index,
                    logs: statusData.logs,
                    status: statusData.status
                }));

                if (["completed", "failed", "partially_completed"].includes(statusData.status)) {
                    if (pollingTimer.current) clearTimeout(pollingTimer.current);
                    fetchBillsList();
                } else {
                    pollingTimer.current = setTimeout(poll, 1500);
                }
            } catch (e) {
                console.error('Polling error:', e);
                if (pollingTimer.current) clearTimeout(pollingTimer.current);
            }
        };
        poll();
    };

    const handleSync = async () => {
        if (selectedProjectIds.length === 0) {
            showToast('info', '提示', '请至少选择一个园区进行同步');
            return;
        }
        const ids = selectedProjectIds.map(id => parseInt(id, 10));
        setIsSyncing(true);
        try {
            const result = await syncBills(ids);
            setSyncState({
                taskId: result.task_id,
                total: ids.length,
                current: 0,
                logs: [{ message: '正在初始化同步任务...', type: 'info', time: new Date().toLocaleTimeString() }],
                status: 'pending'
            });
            startPolling(result.task_id);
        } catch (e) {
            console.error('Sync trigger failed:', e);
            setIsSyncing(false);
            showToast('error', '同步失败', '启动同步任务失败');
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
            render: (_: any, row: Bill) => (
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
            render: (_Value: any, _Record: any, index: number) => (page - 1) * pageSize + index + 1
        },
        { key: 'id' as keyof Bill, title: '账单ID', width: 120 },
        {
            key: 'deal_log_id' as keyof Bill,
            title: '缴费ID',
            width: 120,
            render: (val: any) => <span className="text-secondary text-sm">{val ?? '-'}</span>,
        },
        { key: 'community_name' as keyof Bill, title: '园区', width: 150 },
        { key: 'asset_name' as keyof Bill, title: '资产名称' },
        { key: 'customer_name' as keyof Bill, title: '客户名称' },

        { key: 'charge_item_name' as keyof Bill, title: '收费项目' },
        { key: 'in_month' as keyof Bill, title: '所属月份' },
        {
            key: 'amount' as keyof Bill,
            title: '收款金额',
            render: (val: any) => <span className="font-medium">¥{Number(val).toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>,
        },
        {
            key: 'pay_status_str' as keyof Bill,
            title: '收费状态',
            render: (val: any) => (
                <span className={`badge ${val === '已缴' ? 'success' : 'warning'}`}>{val}</span>
            ),
        },
        {
            key: 'receive_date' as keyof Bill,
            title: '支付日期',
            render: (val: any) => <span className="text-secondary text-sm">{val || '-'}</span>,
        },
        {
            key: 'created_at' as keyof Bill,
            title: '创建时间',
            render: (val: any) => <span className="text-secondary text-sm">{val ? new Date(val).toLocaleDateString() : '-'}</span>,
        },
    ];

    const filteredProjectsList = projects.filter(p =>
        p.proj_name.toLowerCase().includes(projectSearch.toLowerCase()) ||
        String(p.proj_id).includes(projectSearch)
    );

    const toggleProject = (id: string) => {
        setSelectedProjectIds(prev =>
            prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]
        );
    };

    const totalPages = Math.ceil(totalRecords / pageSize);

    const currentPageAmount = bills.reduce((sum, bill) => sum + Number(bill.amount || 0), 0);
    const selectedTotalAmount = Array.from(selectedBillAmounts.values()).reduce((a, b) => a + b, 0);

    return (
        <div className="page-container fade-in">
            {/* Filter Section - Collapsible */}
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">数据筛选与同步</h4>
                    </div>
                    <button className="collapse-toggle" onClick={() => setIsFilterCollapsed(!isFilterCollapsed)}>
                        {isFilterCollapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                    </button>
                </div>

                {!isFilterCollapsed && (
                    <div className="filter-content-wrapper fade-in">
                        {/* 顶部主工作流：同步与刷新 */}
                        <div className="selection-row">
                            <div className="flex items-center gap-3 flex-1 flex-wrap">
                                <div className="selection-group" ref={dropdownRef}>
                                    <div className={`custom-select-trigger ${isDropdownOpen ? 'active' : ''}`} onClick={() => setIsDropdownOpen(!isDropdownOpen)}>
                                        <div className="trigger-content">
                                            <Filter size={14} />
                                            <span className={selectedProjectIds.length === 0 ? 'placeholder' : ''}>
                                                {selectedProjectIds.length === 0 ? '选择同步园区...' : `已选 ${selectedProjectIds.length} 个园区`}
                                            </span>
                                        </div>
                                        <ChevronDown size={14} className={`arrow ${isDropdownOpen ? 'rotate' : ''}`} />
                                    </div>
                                    {isDropdownOpen && (
                                        <div className="custom-dropdown card-shadow slide-up" style={{ zIndex: 100 }}>
                                            <div className="p-2 border-b border-gray-100 flex items-center gap-2">
                                                <Search size={14} className="text-tertiary" />
                                                <input autoFocus type="text" placeholder="搜索园区..." className="dropdown-search" value={projectSearch} onChange={(e) => setProjectSearch(e.target.value)} onClick={(e) => e.stopPropagation()} />
                                            </div>
                                            <div className="p-1 flex justify-between bg-gray-50/50">
                                                <button className="btn-text text-xs" onClick={() => setSelectedProjectIds(projects.map(p => p.proj_id))}>全选</button>
                                                <button className="btn-text text-xs" onClick={() => setSelectedProjectIds([])}>清空</button>
                                            </div>
                                            <div className="dropdown-list custom-scrollbar">
                                                {filteredProjectsList.map(p => (
                                                    <div key={p.proj_id} className={`dropdown-item ${selectedProjectIds.includes(p.proj_id) ? 'selected' : ''}`} onClick={(e) => { e.stopPropagation(); toggleProject(p.proj_id); }}>
                                                        <div className="checkbox">{selectedProjectIds.includes(p.proj_id) && <div className="check-dot"></div>}</div>
                                                        <div className="item-info"><span className="name">{p.proj_name}</span></div>
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </div>
                                <div className="chips-container">
                                    {selectedProjectIds.slice(0, 3).map(id => (
                                        <div key={id} className="selected-chip">
                                            <span>{projects.find(p => p.proj_id === id)?.proj_name || id}</span>
                                            <button onClick={() => toggleProject(id)}><X size={10} /></button>
                                        </div>
                                    ))}
                                    {selectedProjectIds.length > 3 && <span className="text-xs text-secondary">+{selectedProjectIds.length - 3}</span>}
                                </div>
                            </div>

                            <div className="flex gap-2">
                                <button className={`btn-primary ${selectedProjectIds.length === 0 ? 'disabled' : ''}`} onClick={handleSync} disabled={selectedProjectIds.length === 0}>
                                    <RefreshCw size={14} className={isSyncing && !["completed", "failed"].includes(syncState.status) ? 'animate-spin' : ''} />
                                    {isSyncing && !["completed", "failed"].includes(syncState.status) ? '同步中...' : '开始增量同步'}
                                </button>
                                <button className="btn-outline btn-refresh-list" onClick={fetchBillsList}>
                                    <RefreshCw size={14} /> 刷新列表
                                </button>
                            </div>
                        </div>

                        {/* 次级工作流：条件过滤 */}
                        <div className="flex items-center justify-between" style={{ marginBottom: isConditionCollapsed ? '0' : '0.5rem', padding: '0 0.5rem' }}>
                            <span className="text-xs text-secondary font-medium">筛选条件</span>
                            <button className="btn-text text-xs text-primary flex items-center gap-1" onClick={() => setIsConditionCollapsed(!isConditionCollapsed)}>
                                {isConditionCollapsed ? '展开筛选' : '收起筛选'} {isConditionCollapsed ? <ChevronDown size={14} /> : <ChevronUp size={14} />}
                            </button>
                        </div>
                        {!isConditionCollapsed && (
                        <div className="action-row flex-wrap">
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

                                <div className="search-group" style={{ display: 'none' }}>
                                    <input
                                        type="text"
                                        placeholder="缴费ID..."
                                        className="filter-input"
                                        value={keywordFilter}
                                        onChange={(e) => setKeywordFilter(e.target.value)}
                                    />
                                </div>

                                <div className="search-group" style={{ display: 'none' }}>
                                    <input
                                        type="text"
                                        placeholder="收据ID..."
                                        className="filter-input"
                                        value={keywordFilter}
                                        onChange={(e) => setKeywordFilter(e.target.value)}
                                    />
                                </div>

                                <div className="search-group" style={{ display: 'none' }}>
                                    <input
                                        type="text"
                                        placeholder="房号..."
                                        className="filter-input"
                                        value={keywordFilter}
                                        onChange={(e) => setKeywordFilter(e.target.value)}
                                    />
                                </div>

                                <div className="search-group" style={{ display: 'none' }}>
                                    <input
                                        type="text"
                                        placeholder="客户姓名..."
                                        className="filter-input"
                                        value={keywordFilter}
                                        onChange={(e) => setKeywordFilter(e.target.value)}
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

                                <div className="flex items-center gap-1">
                                    <span className="text-secondary text-xs" style={{ whiteSpace: 'nowrap' }}>所属月份:</span>
                                    <select
                                        className="enhanced-select text-xs"
                                        style={{ width: '90px' }}
                                        value={quickInMonth}
                                        onChange={(e) => handleQuickDate(e.target.value, setInMonthStart, setInMonthEnd, 'month', setQuickInMonth)}
                                    >
                                        <option value="">全部</option>
                                        <option value="this_month">本月</option>
                                        <option value="last_month">上月</option>
                                        <option value="this_quarter">本季度</option>
                                        <option value="this_year">本年</option>
                                        <option value="custom">范围</option>
                                    </select>
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
                                    <select
                                        className="enhanced-select text-xs"
                                        style={{ width: '90px' }}
                                        value={quickPayTime}
                                        onChange={(e) => handleQuickDate(e.target.value, setPayTimeStart, setPayTimeEnd, 'date', setQuickPayTime)}
                                    >
                                        <option value="">全部</option>
                                        <option value="today">今日</option>
                                        <option value="this_month">本月</option>
                                        <option value="last_month">上月</option>
                                        <option value="this_quarter">本季度</option>
                                        <option value="this_year">本年</option>
                                        <option value="custom">范围</option>
                                    </select>
                                    {quickPayTime === 'custom' && (
                                        <div className="flex items-center gap-1">
                                            <input type="date" className="enhanced-select text-xs" style={{ width: '120px' }} value={payTimeStart} onChange={(e) => setPayTimeStart(e.target.value)} />
                                            <span className="text-secondary">-</span>
                                            <input type="date" className="enhanced-select text-xs" style={{ width: '120px' }} value={payTimeEnd} onChange={(e) => setPayTimeEnd(e.target.value)} />
                                        </div>
                                    )}
                                </div>
                            </div>

                            <button className="btn-outline" style={{ color: '#ef4444' }} onClick={() => {
                                setCommunityFilter([]);
                                setStatusFilter('全部状态');
                                setChargeItemFilter([]);
                                setKeywordFilter('');
                                setQuickInMonth('');
                                setInMonthStart('');
                                setInMonthEnd('');
                                setQuickPayTime('');
                                setPayTimeStart('');
                                setPayTimeEnd('');
                            }}>
                                <X size={14} /> 重置筛选
                            </button>
                        </div>
                        )}

                    </div>
                )}
            </div>

            {/* Table Area with Pagination */}
            <div className="table-area-wrapper">
                <DataTable columns={columns} data={filteredBills} loading={isLoading} />

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

                        <select className="page-select" value={pageSize} onChange={(e) => { setPageSize(Number(e.target.value)); setPage(1); }}>
                            <option value={10}>10 条/页</option>
                            <option value={25}>25 条/页</option>
                            <option value={50}>50 条/页</option>
                            <option value={100}>100 条/页</option>
                        </select>

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

            {/* Sync Progress Modal */}
            <SyncProgressModal
                isOpen={isSyncing}
                onClose={() => setIsSyncing(false)}
                total={syncState.total}
                current={syncState.current}
                logs={syncState.logs}
                status={syncState.status}
            />

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

export default Bills;


