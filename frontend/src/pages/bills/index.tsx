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
    ChevronUp,
    FileText,
    Link2Off,
    ShieldCheck
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import type { Bill, Project, BillVoucherPushStatus } from '../../types';
import './Bills.css';
import { syncBills, getProjects, getSyncStatus, getBills, previewVoucherForBill, previewBatchVoucherForBills, pushVoucherToKingdee, getBillChargeItems, resetBillVoucherBinding, queryVoucherById } from '../../services/api';
import VoucherPreviewModal from '../../components/common/VoucherPreviewModal';
import ConfirmModal from '../../components/common/ConfirmModal';
import { useToast, ToastContainer } from '../../components/Toast';

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
    const { toasts, showToast, removeToast } = useToast();
    const [bills, setBills] = useState<Bill[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [totalRecords, setTotalRecords] = useState(0);
    const [totalAmount, setTotalAmount] = useState(0);

    const [selectedBillIds, setSelectedBillIds] = useState<Set<string>>(new Set());
    const [selectedBillAmounts, setSelectedBillAmounts] = useState<Map<string, number>>(new Map());
    const [selectedBillRefs, setSelectedBillRefs] = useState<Map<string, { bill_id: number; community_id: number }>>(new Map());

    const [isLoading, setIsLoading] = useState(true);
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
    const [statusFilter, setStatusFilter] = useState('全部状态');
    const [communityFilter, setCommunityFilter] = useState<string[]>([]);
    const [chargeItemFilter, setChargeItemFilter] = useState<string[]>([]);
    const [availableChargeItems, setAvailableChargeItems] = useState<{ value: string, label: string }[]>([]);
    const [customerNameFilter, setCustomerNameFilter] = useState('');
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

    // Confirm Modal (replace browser alert/confirm for critical actions)
    const confirmActionRef = useRef<null | (() => Promise<void> | void)>(null);
    const [confirmModalState, setConfirmModalState] = useState<{
        isOpen: boolean;
        title: string;
        message: string;
        confirmText: string;
        cancelText: string;
        variant: 'primary' | 'danger';
        showAlsoResetToggle: boolean;
    }>({
        isOpen: false,
        title: '',
        message: '',
        confirmText: '确定',
        cancelText: '取消',
        variant: 'primary',
        showAlsoResetToggle: false,
    });
    const [confirmModalLoading, setConfirmModalLoading] = useState(false);

    const [batchVerifyAlsoReset, setBatchVerifyAlsoReset] = useState(false);
    const batchVerifyAlsoResetRef = useRef(false);
    useEffect(() => {
        batchVerifyAlsoResetRef.current = batchVerifyAlsoReset;
    }, [batchVerifyAlsoReset]);

    const openConfirmModal = useCallback((opts: {
        title: string;
        message: string;
        confirmText?: string;
        cancelText?: string;
        variant?: 'primary' | 'danger';
        showAlsoResetToggle?: boolean;
        onConfirm: () => Promise<void> | void;
    }) => {
        confirmActionRef.current = opts.onConfirm;
        setConfirmModalState({
            isOpen: true,
            title: opts.title,
            message: opts.message,
            confirmText: opts.confirmText || '确定',
            cancelText: opts.cancelText || '取消',
            variant: opts.variant || 'primary',
            showAlsoResetToggle: !!opts.showAlsoResetToggle,
        });
    }, []);

    const closeConfirmModal = useCallback(() => {
        if (confirmModalLoading) return;
        confirmActionRef.current = null;
        setConfirmModalState(prev => ({ ...prev, isOpen: false, showAlsoResetToggle: false }));
    }, [confirmModalLoading]);

    const handleConfirmModalConfirm = useCallback(async () => {
        const action = confirmActionRef.current;
        if (!action) {
            closeConfirmModal();
            return;
        }
        setConfirmModalLoading(true);
        try {
            await action();
        } catch (err: any) {
            const msg = err?.response?.data?.detail || err?.response?.data?.message || err?.message || '操作失败';
            showToast('error', '操作失败', typeof msg === 'string' ? msg : JSON.stringify(msg));
        } finally {
            setConfirmModalLoading(false);
            confirmActionRef.current = null;
            setConfirmModalState(prev => ({ ...prev, isOpen: false, showAlsoResetToggle: false }));
        }
    }, [closeConfirmModal, showToast]);

    // 凭证预览状态
    const [voucherPreview, setVoucherPreview] = useState<{ isOpen: boolean; data: any; isLoading: boolean; error: string | null }>({
        isOpen: false, data: null, isLoading: false, error: null
    });

    const buildBillSelectionKey = (billId: string | number, communityId?: number) => `${communityId ?? ''}|${billId}`;
    const summarizePushStatuses = (items: BillVoucherPushStatus[]) => items.reduce((acc, item) => {
        acc.total += 1;
        const statusKey = item.push_status || 'not_pushed';
        if (statusKey in acc) {
            acc[statusKey as keyof typeof acc] += 1;
        }
        return acc;
    }, {
        total: 0,
        not_pushed: 0,
        pushing: 0,
        success: 0,
        failed: 0,
    });

    const handlePreviewVoucher = async (billId: number, communityId?: number) => {
        setVoucherPreview({ isOpen: true, data: null, isLoading: true, error: null });
        try {
            const result = await previewVoucherForBill(billId, communityId);
            setVoucherPreview({ isOpen: true, data: result, isLoading: false, error: null });
        } catch (err: any) {
            setVoucherPreview({
                isOpen: true, data: null, isLoading: false,
                error: err.response?.data?.detail || err.message || '预览失败'
            });
        }
    };

    const handleResetVoucherBinding = async (billId: number, communityId?: number) => {
        if (!Number.isFinite(billId) || !Number.isFinite(Number(communityId))) {
            showToast('error', '解除失败', '缺少账单ID或园区ID');
            return;
        }
        let voucherExists = false;
        try {
            const voucherId = bills.find(b => Number(b.id) === Number(billId) && Number(b.community_id) === Number(communityId))?.voucher_id;
            if (voucherId) {
                const queryResult = await queryVoucherById(String(voucherId));
                voucherExists = !!queryResult?.exists;
            }
        } catch (err) {
            showToast('info', '提示', '凭证存在性校验失败，将继续执行解除。');
        }
        if (voucherExists) {
            showToast('error', '无法解除', '金蝶凭证仍存在，请先在金蝶删除后再解除。');
            return;
        }
        openConfirmModal({
            title: '确认解除',
            message: '确定解除该账单的凭证关联状态并允许重新推送吗？\n注意：此操作不会删除金蝶系统中的凭证，仅重置本系统状态。',
            confirmText: '确定解除',
            cancelText: '取消',
            variant: 'danger',
            onConfirm: async () => {
                const result = await resetBillVoucherBinding([{
                    bill_id: Number(billId),
                    community_id: Number(communityId),
                }], 'manual_reset');
                if (result?.success) {
                    showToast('success', '已解除', '已重置推送状态，可重新推送凭证');
                    await fetchBillsList();
                } else {
                    showToast('error', '解除失败', '服务端返回失败');
                }
            }
        });
    };

    const verifyBillsForReset = async (successBills: Bill[]) => {
        const resetTargets: Array<{ bill_id: number; community_id: number }> = [];
        let existsCount = 0;
        let queryFailed = 0;

        for (const bill of successBills) {
            if (!bill.voucher_id) {
                resetTargets.push({ bill_id: Number(bill.id), community_id: Number(bill.community_id) });
                continue;
            }
            try {
                const queryResult = await queryVoucherById(String(bill.voucher_id));
                if (queryResult?.exists) {
                    existsCount += 1;
                } else {
                    resetTargets.push({ bill_id: Number(bill.id), community_id: Number(bill.community_id) });
                }
            } catch {
                queryFailed += 1;
                resetTargets.push({ bill_id: Number(bill.id), community_id: Number(bill.community_id) });
            }
        }

        return { resetTargets, existsCount, queryFailed };
    };

    const handleBatchVerify = async () => {
        const refs = Array.from(selectedBillRefs.values());
        if (refs.length === 0) {
            showToast('info', '提示', '请先选择账单');
            return;
        }
        const selectedBills = bills.filter(b => selectedBillIds.has(buildBillSelectionKey(b.id, b.community_id)));
        const successBills = selectedBills.filter(b => (b.push_status || '') === 'success');
        if (successBills.length === 0) {
            showToast('info', '提示', '所选账单中没有已推送成功的记录');
            return;
        }

        setBatchVerifyAlsoReset(false);
        batchVerifyAlsoResetRef.current = false;
        openConfirmModal({
            title: '批量校验',
            message: `将校验金蝶凭证是否存在（共 ${successBills.length} 条）。`,
            confirmText: '开始校验',
            cancelText: '取消',
            variant: 'primary',
            showAlsoResetToggle: true,
            onConfirm: async () => {
                const { resetTargets, existsCount, queryFailed } = await verifyBillsForReset(successBills);

                if (!batchVerifyAlsoResetRef.current) {
                    showToast(
                        'success',
                        '校验完成',
                        `可解除 ${resetTargets.length} 条，金蝶仍存在 ${existsCount} 条，校验失败 ${queryFailed} 条`
                    );
                    return;
                }

                if (resetTargets.length === 0) {
                    showToast('info', '提示', '金蝶凭证仍存在，无法解除');
                    return;
                }

                const result = await resetBillVoucherBinding(resetTargets, 'batch_reset');
                if (result?.success) {
                    showToast(
                        'success',
                        '校验并解除完成',
                        `解除 ${resetTargets.length} 条，金蝶仍存在 ${existsCount} 条，校验失败 ${queryFailed} 条`
                    );
                    await fetchBillsList();
                } else {
                    showToast('error', '批量解除失败', '服务端返回失败');
                }
            }
        });
    };

    const handleBatchReset = async () => {
        const refs = Array.from(selectedBillRefs.values());
        if (refs.length === 0) {
            showToast('info', '提示', '请先选择账单');
            return;
        }
        const selectedBills = bills.filter(b => selectedBillIds.has(buildBillSelectionKey(b.id, b.community_id)));
        const successBills = selectedBills.filter(b => (b.push_status || '') === 'success');
        if (successBills.length === 0) {
            showToast('info', '提示', '所选账单中没有已推送成功的记录');
            return;
        }

        openConfirmModal({
            title: '批量解除',
            message: `将先校验金蝶凭证是否存在，仅解除可解除的记录（共 ${successBills.length} 条）。继续吗？`,
            confirmText: '确定解除',
            cancelText: '取消',
            variant: 'danger',
            onConfirm: async () => {
                const { resetTargets, existsCount, queryFailed } = await verifyBillsForReset(successBills);
                if (resetTargets.length === 0) {
                    showToast('info', '提示', '金蝶凭证仍存在，无法解除');
                    return;
                }

                const result = await resetBillVoucherBinding(resetTargets, 'batch_reset');
                if (result?.success) {
                    showToast(
                        'success',
                        '批量解除完成',
                        `解除 ${resetTargets.length} 条，金蝶仍存在 ${existsCount} 条，校验失败 ${queryFailed} 条`
                    );
                    await fetchBillsList();
                } else {
                    showToast('error', '批量解除失败', '服务端返回失败');
                }
            }
        });
    };

    const handlePreviewBatchVoucher = async () => {
        const refs = Array.from(selectedBillRefs.values());
        if (refs.length === 0) {
            showToast('info', '提示', '请先选择账单');
            return;
        }

        setVoucherPreview({ isOpen: true, data: null, isLoading: true, error: null });
        try {
            const result = await previewBatchVoucherForBills(refs);
            if (Array.isArray(result?.skipped_bills) && result.skipped_bills.length > 0) {
                const preview = result.skipped_bills
                    .slice(0, 5)
                    .map((b: any) => `${b.community_id}:${b.bill_id}`)
                    .join(', ');
                showToast('info', '部分账单未匹配模板', `已跳过：${preview}`);
            }
            setVoucherPreview({ isOpen: true, data: result, isLoading: false, error: null });
        } catch (err: any) {
            setVoucherPreview({
                isOpen: true, data: null, isLoading: false,
                error: err.response?.data?.detail || err.message || 'Batch voucher preview failed'
            });
        }
    };

    const handlePushVoucher = async (kingdeeJson: any) => {
        try {
            const sourceBills = Array.isArray(voucherPreview.data?.source_bills)
                ? voucherPreview.data.source_bills
                    .map((item: BillVoucherPushStatus) => ({
                        bill_id: Number(item.bill_id),
                        community_id: Number(item.community_id),
                    }))
                    .filter((item: { bill_id: number; community_id: number }) => Number.isFinite(item.bill_id) && Number.isFinite(item.community_id))
                : [];

            const result = await pushVoucherToKingdee(kingdeeJson, sourceBills);
            if (result?.success) {
                const successText = result?.voucher_number
                    ? `${result?.message || '凭证已推送到金蝶系统'}（凭证号：${result.voucher_number}）`
                    : (result?.message || '凭证已推送到金蝶系统');
                showToast('success', '推送成功', successText);

                if (Array.isArray(result?.tracked_bills) && result.tracked_bills.length > 0) {
                    const trackedBills = result.tracked_bills as BillVoucherPushStatus[];
                    const trackedSummary = summarizePushStatuses(trackedBills);
                    setVoucherPreview(prev => {
                        if (!prev.data) return prev;
                        return {
                            ...prev,
                            data: {
                                ...prev.data,
                                selected_bills: trackedBills,
                                source_bills: trackedBills,
                                selected_bill_push_summary: trackedSummary,
                                source_bill_push_summary: trackedSummary,
                                push_blocked: true,
                                push_block_reason: `当前凭证已推送完成${result?.voucher_number ? `，凭证号：${result.voucher_number}` : ''}`,
                            }
                        };
                    });
                }

                await fetchBillsList();
            } else {
                showToast('error', '推送失败', result?.message || '金蝶接口返回失败');
            }
            return result;
        } catch (err: any) {
            const msg = err.response?.data?.detail || err.response?.data?.message || err.message || '推送失败';
            showToast('error', '推送失败', typeof msg === 'string' ? msg : JSON.stringify(msg));
            throw err;
        }
    };

    const fetchBillsList = useCallback(async () => {
        setIsLoading(true);
        try {
            const params: any = {
                skip: (page - 1) * pageSize,
                limit: pageSize,
                status: statusFilter !== '全部状态' ? statusFilter : undefined,
                community_ids: communityFilter.length > 0 ? communityFilter.join(',') : undefined,
                charge_items: chargeItemFilter.length > 0 ? chargeItemFilter.join(',') : undefined,
                customer_name: customerNameFilter || undefined,
                in_month_start: inMonthStart || undefined,
                in_month_end: inMonthEnd || undefined,
                pay_time_start: payTimeStart || undefined,
                pay_time_end: payTimeEnd || undefined
            };
            if (debouncedSearchQuery) {
                params.search = debouncedSearchQuery;
            }
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
    }, [page, pageSize, debouncedSearchQuery, statusFilter, communityFilter, chargeItemFilter, customerNameFilter, inMonthStart, inMonthEnd, payTimeStart, payTimeEnd]);

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchQuery(searchQuery);
            setPage(1);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchQuery]);

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
            const data = await getProjects();
            setProjects(data.items || data);
        } catch (e) {
            console.error('Failed to load projects:', e);
        }
    };

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
            key: 'push_status_label' as keyof Bill,
            title: '凭证状态',
            width: 110,
            render: (_val: any, row: Bill) => {
                const status = row.push_status || 'not_pushed';
                const statusLabel = row.push_status_label || '未推送';
                const colorMap: Record<string, { bg: string; color: string; border: string }> = {
                    not_pushed: { bg: '#f8fafc', color: '#64748b', border: '#e2e8f0' },
                    pushing: { bg: '#eff6ff', color: '#2563eb', border: '#bfdbfe' },
                    success: { bg: '#ecfdf5', color: '#059669', border: '#a7f3d0' },
                    failed: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca' },
                };
                const style = colorMap[status] || colorMap.not_pushed;
                return (
                    <span style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        padding: '0.2rem 0.5rem',
                        borderRadius: '999px',
                        fontSize: '0.75rem',
                        fontWeight: 600,
                        background: style.bg,
                        color: style.color,
                        border: `1px solid ${style.border}`,
                    }}>
                        {statusLabel}
                    </span>
                );
            },
        },
        {
            key: 'voucher_number' as keyof Bill,
            title: '金蝶凭证号',
            width: 140,
            render: (val: any, row: Bill) => (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.125rem' }}>
                    <span style={{ fontWeight: 600, color: row.voucher_number ? '#0f172a' : '#94a3b8' }}>
                        {val || '-'}
                    </span>
                    {row.pushed_at && (
                        <span className="text-secondary text-xs">
                            {new Date(row.pushed_at).toLocaleString()}
                        </span>
                    )}
                </div>
            ),
        },
        {
            key: 'pay_time' as keyof Bill,
            title: '支付时间',
            render: (val: any) => <span className="text-secondary text-sm">{val ? new Date(val * 1000).toLocaleString() : '-'}</span>,
        },
        {
            key: 'created_at' as keyof Bill,
            title: '创建时间',
            render: (val: any) => <span className="text-secondary text-sm">{val ? new Date(val).toLocaleDateString() : '-'}</span>,
        },
        {
            key: 'actions' as keyof Bill,
            title: '操作',
            width: 130,
            render: (_: any, row: Bill) => (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                <button
                    onClick={() => handlePreviewVoucher(Number(row.id), row.community_id)}
                    style={{
                        display: 'flex', alignItems: 'center', gap: '0.25rem',
                        padding: '0.25rem 0.625rem', borderRadius: '0.375rem',
                        border: '1px solid #e2e8f0', background: '#fafafa',
                        cursor: 'pointer', fontSize: '0.7rem', fontWeight: 500,
                        color: '#3b82f6', transition: 'all 0.15s',
                    }}
                    onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = '#eff6ff'; }}
                    onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = '#fafafa'; }}
                    title="预览凭证"
                >
                    <FileText size={12} /> 凭证
                </button>
                {(row.push_status || '') === 'success' && (
                    <button
                        onClick={() => handleResetVoucherBinding(Number(row.id), row.community_id)}
                        style={{
                            display: 'flex', alignItems: 'center', gap: '0.25rem',
                            padding: '0.25rem 0.625rem', borderRadius: '0.375rem',
                            border: '1px solid #fee2e2', background: '#fff7ed',
                            cursor: 'pointer', fontSize: '0.7rem', fontWeight: 500,
                            color: '#ea580c', transition: 'all 0.15s',
                        }}
                        onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = '#ffedd5'; }}
                        onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = '#fff7ed'; }}
                        title="解除凭证关联状态（用于金蝶已删除后重新推送）"
                    >
                        <Link2Off size={12} /> 解除
                    </button>
                )}
                </div>
            )
        }
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
    const selectedBills = bills.filter(b => selectedBillIds.has(buildBillSelectionKey(b.id, b.community_id)));
    const selectedSuccessCount = selectedBills.filter(b => (b.push_status || '') === 'success').length;
    const canBatchVerify = selectedBillRefs.size > 0 && selectedSuccessCount > 0;
    const canBatchReset = selectedBillRefs.size > 0 && selectedSuccessCount > 0;

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
                                <button
                                    className={`btn-outline btn-batch-voucher ${selectedBillRefs.size === 0 ? 'disabled' : ''}`}
                                    onClick={handlePreviewBatchVoucher}
                                    disabled={selectedBillRefs.size === 0}
                                >
                                    <FileText size={14} /> 批量凭证预览
                                </button>
                                <button
                                    className={`btn-outline btn-batch-check ${!canBatchVerify ? 'disabled' : ''}`}
                                    onClick={handleBatchVerify}
                                    disabled={!canBatchVerify}
                                    title={
                                        selectedBillRefs.size === 0
                                            ? '请先选择账单'
                                            : selectedSuccessCount === 0
                                                ? '仅支持已推送成功的账单进行校验'
                                                : '批量校验金蝶凭证是否存在'
                                    }
                                >
                                    <ShieldCheck size={14} /> 批量校验
                                </button>
                                <button
                                    className={`btn-outline btn-batch-reset ${!canBatchReset ? 'disabled' : ''}`}
                                    onClick={handleBatchReset}
                                    disabled={!canBatchReset}
                                    title={
                                        selectedBillRefs.size === 0
                                            ? '请先选择账单'
                                            : selectedSuccessCount === 0
                                                ? '仅支持已推送成功的账单进行解除'
                                                : '校验金蝶凭证是否存在，仅解除可解除的记录'
                                    }
                                >
                                    <Link2Off size={14} /> 批量解除
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
                                <div className="search-group" style={{ maxWidth: '180px' }}>
                                    <Search size={14} className="search-icon" />
                                    <input type="text" placeholder="搜索ID、房号..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} />
                                </div>

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

                                <div className="search-group" style={{ maxWidth: '120px' }}>
                                    <input
                                        type="text"
                                        placeholder="客户姓名..."
                                        className="enhanced-input"
                                        style={{ paddingLeft: '0.75rem' }}
                                        value={customerNameFilter}
                                        onChange={(e) => setCustomerNameFilter(e.target.value)}
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
                                    <span className="text-secondary text-xs" style={{ whiteSpace: 'nowrap' }}>支付时间:</span>
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
                                setSearchQuery('');
                                setCommunityFilter([]);
                                setStatusFilter('全部状态');
                                setChargeItemFilter([]);
                                setCustomerNameFilter('');
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

            <ConfirmModal
                isOpen={confirmModalState.isOpen}
                title={confirmModalState.title}
                message={confirmModalState.message}
                confirmText={confirmModalState.confirmText}
                cancelText={confirmModalState.cancelText}
                variant={confirmModalState.variant}
                loading={confirmModalLoading}
                onCancel={closeConfirmModal}
                onConfirm={handleConfirmModalConfirm}
            >
                {confirmModalState.showAlsoResetToggle ? (
                    <label style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', color: '#334155' }}>
                        <input
                            type="checkbox"
                            checked={batchVerifyAlsoReset}
                            onChange={(e) => {
                                batchVerifyAlsoResetRef.current = e.target.checked;
                                setBatchVerifyAlsoReset(e.target.checked);
                            }}
                            disabled={confirmModalLoading}
                        />
                        <span style={{ fontSize: '0.85rem' }}>校验后同时批量解除（仅解除可解除项）</span>
                    </label>
                ) : null}
            </ConfirmModal>

            {/* 凭证预览弹窗 */}
            <VoucherPreviewModal
                isOpen={voucherPreview.isOpen}
                onClose={() => setVoucherPreview(prev => ({ ...prev, isOpen: false }))}
                data={voucherPreview.data}
                isLoading={voucherPreview.isLoading}
                error={voucherPreview.error}
                onPushVoucher={handlePushVoucher}
            />
            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

export default Bills;


