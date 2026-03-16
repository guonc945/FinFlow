import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
    RefreshCw,
    Search,
    ChevronDown,
    ChevronUp,
    X,
    Info,
    ChevronLeft,
    ChevronRight,
    ChevronsLeft,
    ChevronsRight,
    FileText,
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import type { Bill, BillVoucherPushStatus, Project, ReceiptBill } from '../../types';
import '../bills/Bills.css'; // reuse bills styling
import { getBills, getProjects, getReceiptBills, getReceiptBillSyncStatus, previewBatchVoucherForBills, pushVoucherToKingdee, syncReceiptBills } from '../../services/api';
import { useToast, ToastContainer } from '../../components/Toast';
import VoucherPreviewModal from '../../components/common/VoucherPreviewModal';

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
    const percentage = Math.round((current / total) * 100) || 0;

    return (
        <div className="sync-overlay">
            <div className="sync-modal">
                <div className="sync-header">
                    <div className="sync-title">
                        {isCompleted ? (
                            <div className={`status-icon-${isFailed ? 'error' : 'success'} ${isFailed ? 'text-error' : 'text-success'}`}>
                                {isFailed ? <X size={24} /> : <RefreshCw size={24} />}
                            </div>
                        ) : (
                            <div className="status-icon-rotating text-primary"><RefreshCw size={24} /></div>
                        )}
                        <div>
                            <h3>{isCompleted ? (isFailed ? '同步失败' : '同步完成') : '正在同步收款账单数据'}</h3>
                            <p className="text-secondary text-sm">
                                {isCompleted
                                    ? (isFailed ? '同步过程中发生错误' : `已完成 ${total} 个园区的同步`)
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

const RelatedBillsModal = ({
    isOpen,
    onClose,
    receiptBill,
    loading,
    bills,
    total,
    totalAmount,
    page,
    pageSize,
    onPageChange,
    onPageSizeChange,
}: {
    isOpen: boolean;
    onClose: () => void;
    receiptBill: ReceiptBill | null;
    loading: boolean;
    bills: Bill[];
    total: number;
    totalAmount: number;
    page: number;
    pageSize: number;
    onPageChange: (nextPage: number) => void;
    onPageSizeChange: (nextSize: number) => void;
}) => {
    if (!isOpen) return null;

    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    const billColumns = [
        { key: 'id', title: '账单ID' },
        { key: 'charge_item_name', title: '收费项目' },
        { key: 'full_house_name', title: '房号' },
        { key: 'in_month', title: '所属月份' },
        {
            key: 'amount',
            title: '金额',
            render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        { key: 'pay_status_str', title: '缴费状态' },
        {
            key: 'pay_time',
            title: '缴费时间',
            render: (v: any) => {
                const ts = Number(v || 0);
                if (!ts) return '-';
                return new Date(ts * 1000).toLocaleString();
            },
        },
    ];

    return (
        <div className="sync-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
            <div className="sync-modal" style={{ width: '960px', maxWidth: '96vw' }}>
                <div className="sync-header">
                    <div className="sync-title" style={{ justifyContent: 'space-between', width: '100%' }}>
                        <div>
                            <h3>关联运营账单</h3>
                            <p className="text-secondary text-sm">
                                收款明细ID: <span className="font-mono">{receiptBill?.id || '-'}</span> | 园区: {receiptBill?.community_name || '-'}
                            </p>
                        </div>
                        <button className="collapse-toggle" onClick={onClose} aria-label="Close">
                            <X size={16} />
                        </button>
                    </div>
                </div>

                <div className="sync-content" style={{ padding: '1rem' }}>
                    <div className="flex items-center justify-between" style={{ marginBottom: '0.75rem' }}>
                        <div className="text-sm text-secondary">
                            共 {total} 条 | 金额合计: <strong style={{ color: '#2563eb' }}>¥{Number(totalAmount || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}</strong>
                        </div>
                        <div className="flex items-center gap-2">
                            <select className="page-select" value={pageSize} onChange={(e) => onPageSizeChange(Number(e.target.value))}>
                                <option value={10}>10 条/页</option>
                                <option value={25}>25 条/页</option>
                                <option value={50}>50 条/页</option>
                                <option value={100}>100 条/页</option>
                            </select>
                            <div className="flex gap-1">
                                <button className="page-btn" disabled={page === 1} onClick={() => onPageChange(1)}><ChevronsLeft size={16} /></button>
                                <button className="page-btn" disabled={page === 1} onClick={() => onPageChange(Math.max(1, page - 1))}><ChevronLeft size={16} /></button>
                                <button className="page-btn active">{page}</button>
                                {page < totalPages && <button className="page-btn" onClick={() => onPageChange(page + 1)}>{page + 1}</button>}
                                <button className="page-btn" disabled={page === totalPages} onClick={() => onPageChange(Math.min(totalPages, page + 1))}><ChevronRight size={16} /></button>
                                <button className="page-btn" disabled={page === totalPages} onClick={() => onPageChange(totalPages)}><ChevronsRight size={16} /></button>
                            </div>
                        </div>
                    </div>

                    <div style={{ maxHeight: '60vh', overflow: 'auto', border: '1px solid #e2e8f0', borderRadius: '0.75rem' }}>
                        <DataTable columns={billColumns as any} data={bills} loading={loading} />
                    </div>
                </div>

                <div className="sync-footer">
                    <button className="btn btn-primary w-full" onClick={onClose}>
                        关闭
                    </button>
                </div>
            </div>
        </div>
    );
};

const ReceiptBills = () => {
    const { toasts, showToast, removeToast } = useToast();

    const [items, setItems] = useState<ReceiptBill[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [totalRecords, setTotalRecords] = useState(0);
    const [totalIncomeAmount, setTotalIncomeAmount] = useState(0);

    const [isLoading, setIsLoading] = useState(true);

    const [searchQuery, setSearchQuery] = useState('');
    const [communityFilter, setCommunityFilter] = useState<string[]>([]);
    const [payChannelStrFilter, setPayChannelStrFilter] = useState('');
    const [payeeFilter, setPayeeFilter] = useState('');
    const [dealDateStart, setDealDateStart] = useState('');
    const [dealDateEnd, setDealDateEnd] = useState('');

    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [isCommunityDropdownOpen, setIsCommunityDropdownOpen] = useState(false);
    const communityDropdownRef = useRef<HTMLDivElement>(null);

    // Pagination
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);

    // Sync progress
    const [isSyncing, setIsSyncing] = useState(false);
    const [syncState, setSyncState] = useState({
        taskId: '',
        total: 0,
        current: 0,
        logs: [] as { message: string; type: 'success' | 'error' | 'info'; time: string }[],
        status: 'idle',
    });
    const pollingTimer = useRef<any>(null);

    // Related bills modal state
    const [relatedBillsOpen, setRelatedBillsOpen] = useState(false);
    const [relatedBillsLoading, setRelatedBillsLoading] = useState(false);
    const [relatedBills, setRelatedBills] = useState<Bill[]>([]);
    const [relatedBillsTotal, setRelatedBillsTotal] = useState(0);
    const [relatedBillsTotalAmount, setRelatedBillsTotalAmount] = useState(0);
    const [relatedBillsPage, setRelatedBillsPage] = useState(1);
    const [relatedBillsPageSize, setRelatedBillsPageSize] = useState(25);
    const [relatedReceiptBill, setRelatedReceiptBill] = useState<ReceiptBill | null>(null);

    // Voucher preview state (moved from bills page)
    const [voucherPreview, setVoucherPreview] = useState<{ isOpen: boolean; data: any; isLoading: boolean; error: string | null }>({
        isOpen: false, data: null, isLoading: false, error: null
    });

    // Batch selection (receipt bills)
    const [selectedReceiptKeys, setSelectedReceiptKeys] = useState<Set<string>>(new Set());
    const [selectedReceiptRefs, setSelectedReceiptRefs] = useState<Map<string, { receipt_bill_id: number; community_id: number }>>(new Map());
    const [selectedReceiptAmounts, setSelectedReceiptAmounts] = useState<Map<string, number>>(new Map());

    const totalPages = useMemo(() => Math.max(1, Math.ceil(totalRecords / pageSize)), [totalRecords, pageSize]);
    const selectedTotalAmount = useMemo(() => {
        let total = 0;
        for (const value of selectedReceiptAmounts.values()) {
            total += Number(value || 0);
        }
        return total;
    }, [selectedReceiptAmounts]);

    const clearReceiptSelection = useCallback(() => {
        setSelectedReceiptKeys(new Set());
        setSelectedReceiptRefs(new Map());
        setSelectedReceiptAmounts(new Map());
    }, []);

    const buildReceiptSelectionKey = (receiptBillId: string | number, communityId?: number) => `${communityId ?? ''}|${receiptBillId}`;
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

    const fetchProjects = useCallback(async () => {
        const resp = await getProjects({ skip: 0, limit: 2000 });
        setProjects(resp?.items || resp || []);
    }, []);

    const fetchReceiptBills = useCallback(async () => {
        setIsLoading(true);
        try {
            const skip = (page - 1) * pageSize;
            const params = {
                search: searchQuery || undefined,
                community_ids: communityFilter.length ? communityFilter.join(',') : undefined,
                pay_channel_str: payChannelStrFilter || undefined,
                payee: payeeFilter || undefined,
                deal_date_start: dealDateStart || undefined,
                deal_date_end: dealDateEnd || undefined,
                skip,
                limit: pageSize,
            };
            const resp = await getReceiptBills(params);
            setItems(resp.items || []);
            setTotalRecords(resp.total || 0);
            setTotalIncomeAmount(resp.total_income_amount || 0);
        } catch (err: any) {
            const msg = err?.response?.data?.detail || err?.message || '加载失败';
            showToast('error', '收款账单加载失败', typeof msg === 'string' ? msg : JSON.stringify(msg));
        } finally {
            setIsLoading(false);
        }
    }, [communityFilter, dealDateEnd, dealDateStart, page, pageSize, payChannelStrFilter, payeeFilter, searchQuery, showToast]);

    useEffect(() => {
        void fetchProjects();
    }, [fetchProjects]);

    useEffect(() => {
        void fetchReceiptBills();
    }, [fetchReceiptBills]);

    useEffect(() => {
        clearReceiptSelection();
    }, [clearReceiptSelection, searchQuery, communityFilter, payChannelStrFilter, payeeFilter, dealDateStart, dealDateEnd, page, pageSize]);

    // Close dropdown on outside click
    useEffect(() => {
        const onClick = (e: MouseEvent) => {
            if (!communityDropdownRef.current) return;
            if (communityDropdownRef.current.contains(e.target as Node)) return;
            setIsCommunityDropdownOpen(false);
        };
        document.addEventListener('mousedown', onClick);
        return () => document.removeEventListener('mousedown', onClick);
    }, []);

    const stopPolling = useCallback(() => {
        if (pollingTimer.current) {
            clearInterval(pollingTimer.current);
            pollingTimer.current = null;
        }
    }, []);

    const startPolling = useCallback((taskId: string) => {
        stopPolling();
        pollingTimer.current = setInterval(async () => {
            try {
                const status = await getReceiptBillSyncStatus(taskId);
                setSyncState(prev => ({
                    ...prev,
                    total: status.total_communities || 0,
                    current: status.current_community_index || 0,
                    logs: status.logs || [],
                    status: status.status || 'running',
                }));
                if (['completed', 'failed', 'partially_completed'].includes(status.status)) {
                    stopPolling();
                    await fetchReceiptBills();
                }
            } catch {
                // ignore polling errors
            }
        }, 1000);
    }, [fetchReceiptBills, stopPolling]);

    const handleSync = useCallback(async () => {
        try {
            setIsSyncing(true);
            setSyncState({ taskId: '', total: 0, current: 0, logs: [], status: 'pending' });
            const ids = communityFilter.length ? communityFilter.map(v => Number(v)).filter(Number.isFinite) : undefined;
            const resp = await syncReceiptBills(ids);
            const taskId = resp?.task_id as string;
            if (!taskId) {
                showToast('error', '同步失败', '未返回 task_id');
                setIsSyncing(false);
                return;
            }
            setSyncState(prev => ({ ...prev, taskId, status: 'running' }));
            startPolling(taskId);
        } catch (err: any) {
            const msg = err?.response?.data?.detail || err?.message || '同步失败';
            showToast('error', '同步失败', typeof msg === 'string' ? msg : JSON.stringify(msg));
            setIsSyncing(false);
        }
    }, [communityFilter, showToast, startPolling]);

    const loadRelatedBills = useCallback(async (receiptBill: ReceiptBill, pageNo: number, size: number) => {
        const dealLogId = Number(receiptBill.id);
        if (!Number.isFinite(dealLogId)) {
            showToast('error', '无法查看账单', '收款明细ID不是有效数字');
            return;
        }
        setRelatedBillsLoading(true);
        try {
            const skip = (pageNo - 1) * size;
            const resp = await getBills({
                community_ids: String(receiptBill.community_id),
                deal_log_id: dealLogId,
                skip,
                limit: size,
            });
            setRelatedBills(resp?.items || []);
            setRelatedBillsTotal(resp?.total || 0);
            setRelatedBillsTotalAmount(resp?.total_amount || 0);
        } catch (err: any) {
            const msg = err?.response?.data?.detail || err?.message || '加载失败';
            showToast('error', '关联账单加载失败', typeof msg === 'string' ? msg : JSON.stringify(msg));
        } finally {
            setRelatedBillsLoading(false);
        }
    }, [showToast]);

    const openRelatedBills = useCallback(async (receiptBill: ReceiptBill) => {
        setRelatedReceiptBill(receiptBill);
        setRelatedBills([]);
        setRelatedBillsTotal(0);
        setRelatedBillsTotalAmount(0);
        setRelatedBillsPage(1);
        setRelatedBillsPageSize(25);
        setRelatedBillsOpen(true);
        await loadRelatedBills(receiptBill, 1, 25);
    }, [loadRelatedBills]);

    const fetchRelatedBillRefsForReceipt = useCallback(async (receiptBillIdRaw: string | number, communityIdRaw: number) => {
        const dealLogId = Number(receiptBillIdRaw);
        const communityId = Number(communityIdRaw);
        if (!Number.isFinite(dealLogId) || !Number.isFinite(communityId)) return [];

        const limit = 200;
        const maxLoops = 50; // safety cap
        const refs: Array<{ bill_id: number; community_id: number }> = [];
        const seen = new Set<string>();

        let skip = 0;
        let expectedTotal = Infinity;
        for (let i = 0; i < maxLoops; i += 1) {
            const resp = await getBills({
                community_ids: String(communityId),
                deal_log_id: dealLogId,
                skip,
                limit,
            });

            const rows = Array.isArray(resp?.items) ? (resp.items as Bill[]) : [];
            expectedTotal = Number(resp?.total || expectedTotal);

            for (const bill of rows) {
                const billId = Number(bill.id);
                const billCommunityId = Number(bill.community_id ?? communityId);
                if (!Number.isFinite(billId) || !Number.isFinite(billCommunityId)) continue;
                const key = `${billCommunityId}|${billId}`;
                if (seen.has(key)) continue;
                seen.add(key);
                refs.push({ bill_id: billId, community_id: billCommunityId });
            }

            skip += rows.length;
            if (rows.length === 0) break;
            if (Number.isFinite(expectedTotal) && refs.length >= expectedTotal) break;
            if (skip >= expectedTotal) break;
        }

        return refs;
    }, []);

    const notifySkippedBills = useCallback((skippedBills: any[]) => {
        if (!Array.isArray(skippedBills) || skippedBills.length === 0) return;

        const preview = skippedBills
            .slice(0, 5)
            .map((b: any) => `${b.community_id}:${b.bill_id}`)
            .join(', ');
        const reasons = Array.from(
            new Set(
                skippedBills
                    .map((b: any) => String(b?.reason || '').trim())
                    .filter(Boolean)
            )
        );
        const title = reasons.includes('template not matched') && reasons.length === 1
            ? '部分账单未匹配模板'
            : '部分账单已跳过';
        const detail = reasons.length > 0
            ? `原因：${reasons.slice(0, 2).join('；')}`
            : '';

        showToast('info', title, detail ? `已跳过：${preview}；${detail}` : `已跳过：${preview}`);
    }, [showToast]);

    const handlePreviewVoucherForReceipt = useCallback(async (receipt: { id: string | number; community_id: number }) => {
        setVoucherPreview({ isOpen: true, data: null, isLoading: true, error: null });
        try {
            const refs = await fetchRelatedBillRefsForReceipt(receipt.id, receipt.community_id);
            if (!refs.length) {
                setVoucherPreview({ isOpen: false, data: null, isLoading: false, error: null });
                showToast('info', '提示', '当前收款单下没有关联的运营账单，无法预览凭证');
                return;
            }

            const result = await previewBatchVoucherForBills(refs);
            notifySkippedBills(result?.skipped_bills || []);
            setVoucherPreview({ isOpen: true, data: result, isLoading: false, error: null });
        } catch (err: any) {
            setVoucherPreview({
                isOpen: true, data: null, isLoading: false,
                error: err?.response?.data?.detail || err?.message || '预览失败'
            });
        }
    }, [fetchRelatedBillRefsForReceipt, notifySkippedBills, showToast]);

    const handlePreviewBatchVoucher = useCallback(async () => {
        const receipts = Array.from(selectedReceiptRefs.values());
        if (receipts.length === 0) {
            showToast('info', '提示', '请先选择收款账单');
            return;
        }

        setVoucherPreview({ isOpen: true, data: null, isLoading: true, error: null });
        try {
            const allRefs: Array<{ bill_id: number; community_id: number }> = [];
            const seen = new Set<string>();
            let missingCount = 0;

            for (const receipt of receipts) {
                const refs = await fetchRelatedBillRefsForReceipt(receipt.receipt_bill_id, receipt.community_id);
                if (!refs.length) {
                    missingCount += 1;
                    continue;
                }
                for (const r of refs) {
                    const key = `${r.community_id}|${r.bill_id}`;
                    if (seen.has(key)) continue;
                    seen.add(key);
                    allRefs.push(r);
                }
            }

            if (!allRefs.length) {
                setVoucherPreview({ isOpen: false, data: null, isLoading: false, error: null });
                showToast('info', '提示', '所选收款单下均没有关联的运营账单，无法预览凭证');
                return;
            }

            const result = await previewBatchVoucherForBills(allRefs);
            notifySkippedBills(result?.skipped_bills || []);
            if (missingCount > 0) {
                showToast('info', '提示', `有 ${missingCount} 条收款单没有关联账单，已自动跳过`);
            }
            setVoucherPreview({ isOpen: true, data: result, isLoading: false, error: null });
        } catch (err: any) {
            setVoucherPreview({
                isOpen: true, data: null, isLoading: false,
                error: err?.response?.data?.detail || err?.message || '预览失败'
            });
        }
    }, [fetchRelatedBillRefsForReceipt, notifySkippedBills, selectedReceiptRefs, showToast]);

    const handlePushVoucher = useCallback(async (kingdeeJson: any) => {
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

                await fetchReceiptBills();
            } else {
                showToast('error', '推送失败', result?.message || '金蝶接口返回失败');
            }
            return result;
        } catch (err: any) {
            const msg = err?.response?.data?.detail || err?.response?.data?.message || err?.message || '推送失败';
            showToast('error', '推送失败', typeof msg === 'string' ? msg : JSON.stringify(msg));
            throw err;
        }
    }, [fetchReceiptBills, showToast, summarizePushStatuses, voucherPreview.data]);

    const columns = [
        {
            key: '_selection',
            width: 40,
            title: (
                <input
                    type="checkbox"
                    checked={items.length > 0 && items.every(r => selectedReceiptKeys.has(buildReceiptSelectionKey(r.id, r.community_id)))}
                    onChange={(e) => {
                        const checked = e.target.checked;
                        const newKeys = new Set(selectedReceiptKeys);
                        const newRefs = new Map(selectedReceiptRefs);
                        const newAmounts = new Map(selectedReceiptAmounts);
                        items.forEach((row) => {
                            const key = buildReceiptSelectionKey(row.id, row.community_id);
                            const receiptBillId = Number(row.id);
                            const communityId = Number(row.community_id);
                            if (checked) {
                                newKeys.add(key);
                                newAmounts.set(key, Number(row.income_amount || 0));
                                if (Number.isFinite(receiptBillId) && Number.isFinite(communityId)) {
                                    newRefs.set(key, { receipt_bill_id: receiptBillId, community_id: communityId });
                                }
                            } else {
                                newKeys.delete(key);
                                newRefs.delete(key);
                                newAmounts.delete(key);
                            }
                        });
                        setSelectedReceiptKeys(newKeys);
                        setSelectedReceiptRefs(newRefs);
                        setSelectedReceiptAmounts(newAmounts);
                    }}
                />
            ),
            render: (_: any, row: ReceiptBill) => (
                <input
                    type="checkbox"
                    checked={selectedReceiptKeys.has(buildReceiptSelectionKey(row.id, row.community_id))}
                    onChange={(e) => {
                        const checked = e.target.checked;
                        const newKeys = new Set(selectedReceiptKeys);
                        const newRefs = new Map(selectedReceiptRefs);
                        const newAmounts = new Map(selectedReceiptAmounts);
                        const key = buildReceiptSelectionKey(row.id, row.community_id);
                        const receiptBillId = Number(row.id);
                        const communityId = Number(row.community_id);
                        if (checked) {
                            newKeys.add(key);
                            newAmounts.set(key, Number(row.income_amount || 0));
                            if (Number.isFinite(receiptBillId) && Number.isFinite(communityId)) {
                                newRefs.set(key, { receipt_bill_id: receiptBillId, community_id: communityId });
                            }
                        } else {
                            newKeys.delete(key);
                            newRefs.delete(key);
                            newAmounts.delete(key);
                        }
                        setSelectedReceiptKeys(newKeys);
                        setSelectedReceiptRefs(newRefs);
                        setSelectedReceiptAmounts(newAmounts);
                    }}
                />
            )
        },
        { key: 'community_name', title: '园区' },
        { key: 'receipt_id', title: '收据号' },
        { key: 'id', title: '明细ID' },
        { key: 'asset_name', title: '资产/房号' },
        { key: 'payee', title: '收款人' },
        { key: 'payer_name', title: '付款人' },
        {
            key: 'income_amount',
            title: '入账金额',
            render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        {
            key: 'amount',
            title: '实收金额',
            render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        {
            key: 'bill_amount',
            title: '账单金额',
            render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        { key: 'pay_channel_str', title: '收款渠道' },
        {
            key: 'deal_time',
            title: '收款时间',
            render: (v: any, record: ReceiptBill) => {
                const ts = Number(v || 0);
                if (!ts) return record.deal_date || '-';
                const d = new Date(ts * 1000);
                return d.toLocaleString();
            },
        },
        {
            key: 'actions',
            title: '操作',
            fixed: 'right' as const,
            className: 'dt-sticky-right',
            render: (_: any, record: ReceiptBill) => (
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <button
                        className="btn-outline"
                        style={{ height: '30px', padding: '0 0.75rem' }}
                        onClick={(e) => {
                            e.stopPropagation();
                            void handlePreviewVoucherForReceipt({ id: record.id, community_id: record.community_id });
                        }}
                        title="预览凭证（按当前收款单关联账单生成）"
                    >
                        <FileText size={14} /> 凭证
                    </button>
                    <button
                        className="btn-outline"
                        style={{ height: '30px', padding: '0 0.75rem' }}
                        onClick={(e) => {
                            e.stopPropagation();
                            void openRelatedBills(record);
                        }}
                        title="查看关联账单"
                    >
                        账单
                    </button>
                </div>
            )
        },
    ];

    return (
        <div className="page-container">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <h3 className="font-bold text-slate-800">收款账单</h3>
                        <span className="text-xs text-secondary">共 {totalRecords} 条</span>
                    </div>
                    <button className="collapse-toggle" onClick={() => setIsFilterCollapsed(v => !v)}>
                        {isFilterCollapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                    </button>
                </div>

                {!isFilterCollapsed && (
                    <div className="filter-content-wrapper">
                        <div className="action-row flex-wrap">
                            <div className="flex items-center gap-2 flex-1 flex-wrap">
                                <div className="search-group" style={{ maxWidth: '240px' }}>
                                    <Search size={14} className="search-icon" />
                                    <input
                                        type="text"
                                        placeholder="搜索收据号/房号/付款人..."
                                        value={searchQuery}
                                        onChange={(e) => { setSearchQuery(e.target.value); setPage(1); }}
                                    />
                                </div>

                                <div className="selection-group" ref={communityDropdownRef} style={{ maxWidth: '200px', minWidth: '180px' }}>
                                    <div className={`custom-select-trigger ${isCommunityDropdownOpen ? 'active' : ''}`} onClick={() => setIsCommunityDropdownOpen(v => !v)}>
                                        <div className="trigger-content">
                                            <span className={communityFilter.length === 0 ? 'placeholder' : 'text-xs truncate'} style={{ maxWidth: '130px' }}>
                                                {communityFilter.length === 0 ? '选择园区...' : `已选 ${communityFilter.length} 个`}
                                            </span>
                                        </div>
                                        <ChevronDown size={14} className={`arrow ${isCommunityDropdownOpen ? 'rotate' : ''}`} />
                                    </div>
                                    {isCommunityDropdownOpen && (
                                        <div className="custom-dropdown card-shadow slide-up" style={{ zIndex: 100, width: '240px' }}>
                                            <div className="dropdown-list custom-scrollbar" style={{ maxHeight: '220px' }}>
                                                {projects.map(p => (
                                                    <div
                                                        key={p.proj_id}
                                                        className={`dropdown-item ${communityFilter.includes(String(p.proj_id)) ? 'selected' : ''}`}
                                                        onClick={(e) => {
                                                            e.stopPropagation();
                                                            setCommunityFilter(prev => prev.includes(String(p.proj_id)) ? prev.filter(x => x !== String(p.proj_id)) : [...prev, String(p.proj_id)]);
                                                            setPage(1);
                                                        }}
                                                    >
                                                        <div className="checkbox">{communityFilter.includes(String(p.proj_id)) && <div className="check-dot"></div>}</div>
                                                        <div className="item-info"><span className="name">{p.proj_name}</span></div>
                                                    </div>
                                                ))}
                                            </div>
                                            <div className="p-1 flex justify-between bg-gray-50/50 border-t border-gray-100">
                                                <button className="btn-text text-xs" onClick={() => { setCommunityFilter(projects.map(p => String(p.proj_id))); setPage(1); }}>全选</button>
                                                <button className="btn-text text-xs" onClick={() => { setCommunityFilter([]); setPage(1); }}>清空</button>
                                            </div>
                                        </div>
                                    )}
                                </div>

                                <input
                                    type="text"
                                    className="enhanced-select"
                                    style={{ width: '160px' }}
                                    placeholder="收款渠道(模糊)"
                                    value={payChannelStrFilter}
                                    onChange={(e) => { setPayChannelStrFilter(e.target.value); setPage(1); }}
                                />

                                <input
                                    type="text"
                                    className="enhanced-select"
                                    style={{ width: '140px' }}
                                    placeholder="收款人"
                                    value={payeeFilter}
                                    onChange={(e) => { setPayeeFilter(e.target.value); setPage(1); }}
                                />

                                <div className="flex items-center gap-1">
                                    <span className="text-secondary text-xs" style={{ whiteSpace: 'nowrap' }}>收款日期:</span>
                                    <input type="date" className="enhanced-select text-xs" style={{ width: '140px' }} value={dealDateStart} onChange={(e) => { setDealDateStart(e.target.value); setPage(1); }} />
                                    <span className="text-secondary">-</span>
                                    <input type="date" className="enhanced-select text-xs" style={{ width: '140px' }} value={dealDateEnd} onChange={(e) => { setDealDateEnd(e.target.value); setPage(1); }} />
                                </div>
                            </div>

                            <button className="btn-primary btn-refresh-list" onClick={handleSync}>
                                <RefreshCw size={14} /> 同步收款账单
                            </button>

                            <button
                                className={`btn-outline btn-batch-voucher ${selectedReceiptRefs.size === 0 ? 'disabled' : ''}`}
                                onClick={handlePreviewBatchVoucher}
                                disabled={selectedReceiptRefs.size === 0}
                                title={selectedReceiptRefs.size === 0 ? '请先勾选收款账单' : '批量预览凭证（按收款单关联账单生成）'}
                            >
                                <FileText size={14} /> 批量凭证预览
                            </button>

                            <button className="btn-outline" style={{ color: '#ef4444' }} onClick={() => {
                                setSearchQuery('');
                                setCommunityFilter([]);
                                setPayChannelStrFilter('');
                                setPayeeFilter('');
                                setDealDateStart('');
                                setDealDateEnd('');
                                setPage(1);
                            }}>
                                <X size={14} /> 重置筛选
                            </button>
                        </div>
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <DataTable columns={columns} data={items} loading={isLoading} />

                <div className="pagination-footer">
                    <div className="pagination-info" style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
                        <span>
                            显示 {(page - 1) * pageSize + 1} 到 {Math.min(page * pageSize, totalRecords)} 条，共 {totalRecords} 条
                        </span>
                        <span className="text-secondary">|</span>
                        <span>入账总额: <strong style={{ color: '#2563eb' }}>¥{totalIncomeAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}</strong></span>
                        {selectedReceiptKeys.size > 0 && (
                            <>
                                <span className="text-secondary">|</span>
                                <span>
                                    已选 {selectedReceiptKeys.size} 条 / 入账合计:
                                    <strong style={{ color: '#16a34a', marginLeft: '0.25rem' }}>
                                        ¥{selectedTotalAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                                    </strong>
                                </span>
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

            <SyncProgressModal
                isOpen={isSyncing}
                onClose={() => { setIsSyncing(false); stopPolling(); }}
                total={syncState.total}
                current={syncState.current}
                logs={syncState.logs}
                status={syncState.status}
            />

            <RelatedBillsModal
                isOpen={relatedBillsOpen}
                onClose={() => setRelatedBillsOpen(false)}
                receiptBill={relatedReceiptBill}
                loading={relatedBillsLoading}
                bills={relatedBills}
                total={relatedBillsTotal}
                totalAmount={relatedBillsTotalAmount}
                page={relatedBillsPage}
                pageSize={relatedBillsPageSize}
                onPageChange={(nextPage) => {
                    if (!relatedReceiptBill) return;
                    setRelatedBillsPage(nextPage);
                    void loadRelatedBills(relatedReceiptBill, nextPage, relatedBillsPageSize);
                }}
                onPageSizeChange={(nextSize) => {
                    if (!relatedReceiptBill) return;
                    setRelatedBillsPageSize(nextSize);
                    setRelatedBillsPage(1);
                    void loadRelatedBills(relatedReceiptBill, 1, nextSize);
                }}
            />

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

export default ReceiptBills;

