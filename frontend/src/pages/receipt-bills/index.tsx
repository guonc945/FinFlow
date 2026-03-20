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
    Link2Off,
    ShieldCheck,
    MoreHorizontal,
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import type { Bill, BillVoucherPushStatus, Project, PushStatusSummary, ReceiptBill } from '../../types';
import '../bills/Bills.css'; // reuse bills styling
import {
    getBills,
    getProjects,
    getReceiptBill,
    getReceiptBills,
    getReceiptBillSyncStatus,
    previewBatchVoucherForReceipts,
    previewVoucherForReceipt,
    pushVoucherToKingdee,
    queryVoucherById,
    resetBillVoucherBinding,
    syncReceiptBills,
} from '../../services/api';
import ConfirmModal from '../../components/common/ConfirmModal';
import { useToast, ToastContainer } from '../../components/Toast';
import VoucherPreviewModal from '../../components/common/VoucherPreviewModal';
import type {
    DepositRecord,
    PrepaymentRecord,
    ReceiptBillDepositRefundLinkSummary,
    ReceiptBillDetail,
    ReceiptBillDrilldownSection,
} from '../../types';

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

const RECEIPT_BILL_DEAL_TYPE_LABELS: Record<number, string> = {
    1: '预存款充值',
    2: '预存款退款',
    3: '账单实收',
    4: '账单退款',
    5: '收取押金',
    6: '退还押金',
};

const RECEIPT_PUSH_STATUS_LABELS: Record<string, string> = {
    unbound: '未关联账单',
    not_pushed: '未推送',
    pushing: '推送中',
    success: '已推送',
    failed: '推送失败',
    partial: '部分推送',
};

const RECEIPT_VOUCHER_STATUS_STYLES: Record<string, { bg: string; color: string; border: string }> = {
    untracked: { bg: '#f8fafc', color: '#64748b', border: '#e2e8f0' },
    unbound: { bg: '#f8fafc', color: '#64748b', border: '#e2e8f0' },
    not_pushed: { bg: '#f8fafc', color: '#64748b', border: '#e2e8f0' },
    pushing: { bg: '#eff6ff', color: '#2563eb', border: '#bfdbfe' },
    success: { bg: '#ecfdf5', color: '#059669', border: '#a7f3d0' },
    failed: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca' },
    partial: { bg: '#fff7ed', color: '#c2410c', border: '#fdba74' },
};

const RECEIPT_DEPOSIT_REFUND_LINK_TYPE_LABELS: Record<string, string> = {
    actual_refund: '实际退款',
    transfer_to_prepayment: '转入预存',
    mixed: '混合处理',
    unmatched: '未匹配',
};

const getReceiptBillDealTypeLabel = (dealType?: number | null, dealTypeLabel?: string | null) => {
    if (dealTypeLabel) return dealTypeLabel;
    if (dealType == null) return '-';
    return RECEIPT_BILL_DEAL_TYPE_LABELS[dealType] || `未知类型(${dealType})`;
};

const getDepositRefundLinkTypeLabel = (summary?: ReceiptBillDepositRefundLinkSummary | null) => {
    const linkType = String(summary?.link_type || '').trim();
    if (linkType && RECEIPT_DEPOSIT_REFUND_LINK_TYPE_LABELS[linkType]) {
        return RECEIPT_DEPOSIT_REFUND_LINK_TYPE_LABELS[linkType];
    }
    return summary?.link_type_label || '未识别';
};

const getDrilldownSectionSourceLabel = (sourceType?: string | null) => {
    if (sourceType === 'bills') return '运营账单';
    if (sourceType === 'deposit_records') return '押金变动';
    if (sourceType === 'prepayment_records') return '预存款';
    return '关联数据';
};

const getReceiptDrilldownBadges = (receipt: ReceiptBill) => {
    const sections = Array.isArray(receipt.drilldown_sections) ? receipt.drilldown_sections : [];
    const deduped = new Map<string, { key: string; label: string; tone: 'blue' | 'amber' | 'emerald' | 'slate' }>();

    sections.forEach((section) => {
        let badge: { key: string; label: string; tone: 'blue' | 'amber' | 'emerald' | 'slate' };
        if (section.relation_key === 'receipt_to_prepayment_transfer') {
            badge = { key: 'transfer_to_prepayment', label: '转预存', tone: 'emerald' };
        } else if (section.source_type === 'bills') {
            badge = { key: 'bills', label: '运营账单', tone: 'blue' };
        } else if (section.source_type === 'deposit_records') {
            badge = { key: 'deposit', label: '押金', tone: 'amber' };
        } else if (section.source_type === 'prepayment_records') {
            badge = { key: 'prepayment', label: '预存', tone: 'emerald' };
        } else {
            badge = { key: section.relation_key, label: getDrilldownSectionSourceLabel(section.source_type), tone: 'slate' };
        }
        deduped.set(badge.key, badge);
    });

    return Array.from(deduped.values());
};

const DRILLDOWN_BADGE_STYLES: Record<'blue' | 'amber' | 'emerald' | 'slate', { bg: string; color: string; border: string }> = {
    blue: { bg: '#eff6ff', color: '#1d4ed8', border: '#bfdbfe' },
    amber: { bg: '#fff7ed', color: '#c2410c', border: '#fdba74' },
    emerald: { bg: '#ecfdf5', color: '#059669', border: '#a7f3d0' },
    slate: { bg: '#f8fafc', color: '#475569', border: '#cbd5e1' },
};

const formatUnixTimestamp = (value?: number | null) => {
    const ts = Number(value || 0);
    if (!ts) return '-';
    return new Date(ts * 1000).toLocaleString();
};

const getDrilldownColumns = (section: ReceiptBillDrilldownSection) => {
    if (section.source_type === 'bills') {
        return [
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
                render: (v: any) => formatUnixTimestamp(v),
            },
        ];
    }

    if (section.source_type === 'deposit_records') {
        return [
            { key: 'id', title: '押金记录ID' },
            { key: 'house_name', title: '房号' },
            { key: 'cash_pledge_name', title: '押金类型' },
            {
                key: 'amount',
                title: '金额',
                render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
            },
            { key: 'operate_type_label', title: '变动类型' },
            { key: 'pay_channel_str', title: '渠道' },
            {
                key: 'operate_time',
                title: '操作时间',
                render: (_: any, record: DepositRecord) => formatUnixTimestamp(record.operate_time || record.pay_time),
            },
            {
                key: 'remark',
                title: '备注',
                render: (v: any) => v || '-',
            },
        ];
    }

    return [
        { key: 'id', title: '预存记录ID' },
        { key: 'house_name', title: '房号' },
        { key: 'category_name', title: '预存类别' },
        {
            key: 'amount',
            title: '金额',
            render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        {
            key: 'balance_after_change',
            title: '变动后余额',
            render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        { key: 'operate_type_label', title: '变动类型' },
        { key: 'pay_channel_str', title: '渠道' },
        {
            key: 'operate_time',
            title: '操作时间',
            render: (_: any, record: PrepaymentRecord) => formatUnixTimestamp(record.operate_time || record.pay_time),
        },
        {
            key: 'remark',
            title: '备注',
            render: (v: any) => v || '-',
        },
    ];
};

const ReceiptDrilldownModal = ({
    isOpen,
    onClose,
    receiptBill,
    loading,
    summary,
    sections,
    depositRefundLinkSummary,
}: {
    isOpen: boolean;
    onClose: () => void;
    receiptBill: ReceiptBill | null;
    loading: boolean;
    summary: string;
    sections: ReceiptBillDrilldownSection[];
    depositRefundLinkSummary?: ReceiptBillDepositRefundLinkSummary | null;
}) => {
    if (!isOpen) return null;

    return (
        <div className="sync-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
            <div className="sync-modal" style={{ width: '1120px', maxWidth: '96vw' }}>
                <div className="sync-header">
                    <div className="sync-title" style={{ justifyContent: 'space-between', width: '100%' }}>
                        <div>
                            <h3>关联数据钻取</h3>
                            <p className="text-secondary text-sm">
                                收款明细ID: <span className="font-mono">{receiptBill?.id || '-'}</span> | 园区: {receiptBill?.community_name || '-'}
                            </p>
                            <p className="text-secondary text-sm">
                                类型: {getReceiptBillDealTypeLabel(receiptBill?.deal_type, receiptBill?.deal_type_label)} | {summary || '暂无关联数据'}
                            </p>
                            {receiptBill?.deal_type === 6 && depositRefundLinkSummary && (
                                <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginTop: '0.5rem' }}>
                                    <span style={{
                                        display: 'inline-flex',
                                        alignItems: 'center',
                                        padding: '0.18rem 0.55rem',
                                        borderRadius: '999px',
                                        fontSize: '0.75rem',
                                        fontWeight: 700,
                                        background: '#eff6ff',
                                        color: '#1d4ed8',
                                        border: '1px solid #bfdbfe',
                                    }}>
                                        退还路径：{getDepositRefundLinkTypeLabel(depositRefundLinkSummary)}
                                    </span>
                                    <span className="text-secondary text-xs" style={{ display: 'inline-flex', alignItems: 'center' }}>
                                        绑定 {depositRefundLinkSummary.link_count || 0} 条
                                        {depositRefundLinkSummary.match_confidence != null ? ` · 置信度 ${Number(depositRefundLinkSummary.match_confidence).toFixed(2)}` : ''}
                                    </span>
                                </div>
                            )}
                        </div>
                        <button className="collapse-toggle" onClick={onClose} aria-label="Close">
                            <X size={16} />
                        </button>
                    </div>
                </div>

                <div className="sync-content" style={{ padding: '1rem' }}>
                    {loading ? (
                        <div style={{ minHeight: '240px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                            加载中...
                        </div>
                    ) : sections.length === 0 ? (
                        <div style={{ minHeight: '200px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#64748b' }}>
                            当前收款单暂无可钻取的关联数据
                        </div>
                    ) : (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem', maxHeight: '68vh', overflow: 'auto' }}>
                            {sections.map((section) => (
                                <div key={section.relation_key} style={{ border: '1px solid #e2e8f0', borderRadius: '0.75rem', overflow: 'hidden' }}>
                                    <div style={{
                                        padding: '0.75rem 1rem',
                                        borderBottom: '1px solid #e2e8f0',
                                        background: '#f8fafc',
                                        display: 'flex',
                                        justifyContent: 'space-between',
                                        alignItems: 'center',
                                    }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                            <div style={{ fontWeight: 700, color: '#0f172a' }}>{section.label}</div>
                                            <span style={{
                                                display: 'inline-flex',
                                                alignItems: 'center',
                                                padding: '0.12rem 0.45rem',
                                                borderRadius: '999px',
                                                fontSize: '0.72rem',
                                                fontWeight: 600,
                                                background: '#ffffff',
                                                color: '#475569',
                                                border: '1px solid #cbd5e1',
                                            }}>
                                                {getDrilldownSectionSourceLabel(section.source_type)}
                                            </span>
                                        </div>
                                        <div className="text-secondary text-sm">共 {section.count} 条</div>
                                    </div>
                                    <DataTable
                                        columns={getDrilldownColumns(section) as any}
                                        data={(section.items || []) as any[]}
                                        loading={false}
                                        tableId={`receipt-drilldown-${section.relation_key}`}
                                    />
                                </div>
                            ))}
                        </div>
                    )}
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
    const [dealDateStart, setDealDateStart] = useState('');
    const [dealDateEnd, setDealDateEnd] = useState('');
    const [dealTypeFilter, setDealTypeFilter] = useState('');

    const [isOperationsCollapsed, setIsOperationsCollapsed] = useState(false);
    const [isConditionCollapsed, setIsConditionCollapsed] = useState(false);
    const [isCommunityDropdownOpen, setIsCommunityDropdownOpen] = useState(false);
    const communityDropdownRef = useRef<HTMLDivElement>(null);
    const [openActionMenuKey, setOpenActionMenuKey] = useState<string | null>(null);
    const [isBatchActionsOpen, setIsBatchActionsOpen] = useState(false);

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

    const [drilldownOpen, setDrilldownOpen] = useState(false);
    const [drilldownLoading, setDrilldownLoading] = useState(false);
    const [drilldownReceiptBill, setDrilldownReceiptBill] = useState<ReceiptBill | null>(null);
    const [drilldownSummary, setDrilldownSummary] = useState('');
    const [drilldownSections, setDrilldownSections] = useState<ReceiptBillDrilldownSection[]>([]);
    const [drilldownDepositRefundSummary, setDrilldownDepositRefundSummary] = useState<ReceiptBillDepositRefundLinkSummary | null>(null);

    // Voucher preview state (moved from bills page)
    const [voucherPreview, setVoucherPreview] = useState<{ isOpen: boolean; data: any; isLoading: boolean; error: string | null }>({
        isOpen: false, data: null, isLoading: false, error: null
    });

    // Batch selection (receipt bills)
    const [selectedReceiptKeys, setSelectedReceiptKeys] = useState<Set<string>>(new Set());
    const [selectedReceiptRefs, setSelectedReceiptRefs] = useState<Map<string, { receipt_bill_id: number; community_id: number }>>(new Map());
    const [selectedReceiptAmounts, setSelectedReceiptAmounts] = useState<Map<string, number>>(new Map());
    const [confirmModalState, setConfirmModalState] = useState<{
        isOpen: boolean;
        title: string;
        message: string;
        confirmText: string;
        cancelText: string;
        variant: 'primary' | 'danger';
    }>({
        isOpen: false,
        title: '',
        message: '',
        confirmText: '确认',
        cancelText: '取消',
        variant: 'primary',
    });
    const [confirmModalLoading, setConfirmModalLoading] = useState(false);
    const confirmActionRef = useRef<(() => Promise<void>) | null>(null);

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
    const summarizePushStatuses = (items: Array<Pick<BillVoucherPushStatus, 'push_status'>>): PushStatusSummary => items.reduce((acc, item) => {
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
    const openConfirmModal = useCallback((opts: {
        title: string;
        message: string;
        confirmText?: string;
        cancelText?: string;
        variant?: 'primary' | 'danger';
        onConfirm: () => Promise<void>;
    }) => {
        confirmActionRef.current = opts.onConfirm;
        setConfirmModalState({
            isOpen: true,
            title: opts.title,
            message: opts.message,
            confirmText: opts.confirmText || '确认',
            cancelText: opts.cancelText || '取消',
            variant: opts.variant || 'primary',
        });
    }, []);
    const closeConfirmModal = useCallback(() => {
        if (confirmModalLoading) return;
        confirmActionRef.current = null;
        setConfirmModalState(prev => ({ ...prev, isOpen: false }));
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
            setConfirmModalState(prev => ({ ...prev, isOpen: false }));
        }
    }, [closeConfirmModal, showToast]);

    const fetchProjects = useCallback(async () => {
        const resp = await getProjects({ skip: 0, limit: 2000, current_account_book_only: true });
        setProjects(resp?.items || resp || []);
    }, []);

    const fetchReceiptBills = useCallback(async () => {
        setIsLoading(true);
        try {
            const skip = (page - 1) * pageSize;
            const params = {
                search: searchQuery || undefined,
                community_ids: communityFilter.length ? communityFilter.join(',') : undefined,
                deal_date_start: dealDateStart || undefined,
                deal_date_end: dealDateEnd || undefined,
                deal_type: dealTypeFilter ? Number(dealTypeFilter) : undefined,
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
    }, [communityFilter, dealDateEnd, dealDateStart, dealTypeFilter, page, pageSize, searchQuery, showToast]);

    useEffect(() => {
        void fetchProjects();
    }, [fetchProjects]);

    useEffect(() => {
        void fetchReceiptBills();
    }, [fetchReceiptBills]);

    useEffect(() => {
        clearReceiptSelection();
    }, [clearReceiptSelection, searchQuery, communityFilter, dealDateStart, dealDateEnd, dealTypeFilter, page, pageSize]);

    useEffect(() => {
        const validProjectIds = new Set(projects.map(project => String(project.proj_id)));
        setCommunityFilter(prev => {
            const next = prev.filter(id => validProjectIds.has(String(id)));
            return next.length === prev.length ? prev : next;
        });
    }, [projects]);

    // Close dropdown on outside click
    useEffect(() => {
        const onClick = (e: MouseEvent) => {
            const target = e.target as HTMLElement | null;
            if (communityDropdownRef.current?.contains(e.target as Node)) return;
            if (target?.closest('.receipt-row-action')) return;
            if (target?.closest('.receipt-batch-action')) return;
            setIsCommunityDropdownOpen(false);
            setOpenActionMenuKey(null);
            setIsBatchActionsOpen(false);
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

    const openReceiptDrilldown = useCallback(async (receiptBill: ReceiptBill) => {
        if (!receiptBill.drilldown_enabled) {
            showToast('info', '提示', '当前收款单暂无可钻取的关联数据');
            return;
        }

        setDrilldownReceiptBill(receiptBill);
        setDrilldownSummary(receiptBill.drilldown_summary || '');
        setDrilldownSections([]);
        setDrilldownDepositRefundSummary(null);
        setDrilldownOpen(true);
        setDrilldownLoading(true);
        try {
            const detail: ReceiptBillDetail = await getReceiptBill(Number(receiptBill.id), Number(receiptBill.community_id));
            setDrilldownSummary(detail.drilldown_summary || receiptBill.drilldown_summary || '');
            setDrilldownSections(detail.drilldown_sections || []);
            setDrilldownDepositRefundSummary(detail.deposit_refund_link_summary || null);
        } catch (err: any) {
            const msg = err?.response?.data?.detail || err?.message || '加载失败';
            showToast('error', '关联数据加载失败', typeof msg === 'string' ? msg : JSON.stringify(msg));
            setDrilldownOpen(false);
        } finally {
            setDrilldownLoading(false);
        }
    }, [showToast]);

    const fetchRelatedBillsForReceipt = useCallback(async (receiptBillIdRaw: string | number, communityIdRaw: number) => {
        const dealLogId = Number(receiptBillIdRaw);
        const communityId = Number(communityIdRaw);
        if (!Number.isFinite(dealLogId) || !Number.isFinite(communityId)) return [];

        const limit = 200;
        const maxLoops = 50; // safety cap
        const relatedBills: Bill[] = [];
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
                relatedBills.push(bill);
            }

            skip += rows.length;
            if (rows.length === 0) break;
            if (Number.isFinite(expectedTotal) && relatedBills.length >= expectedTotal) break;
            if (skip >= expectedTotal) break;
        }

        return relatedBills;
    }, []);
    const collectRelatedBillsForReceipts = useCallback(async (receipts: Array<{ receipt_bill_id: number; community_id: number }>) => {
        const allBills: Bill[] = [];
        const seen = new Set<string>();
        let missingCount = 0;

        for (const receipt of receipts) {
            const billsForReceipt = await fetchRelatedBillsForReceipt(receipt.receipt_bill_id, receipt.community_id);
            if (!billsForReceipt.length) {
                missingCount += 1;
                continue;
            }
            for (const bill of billsForReceipt) {
                const billId = Number(bill.id);
                const communityId = Number(bill.community_id);
                if (!Number.isFinite(billId) || !Number.isFinite(communityId)) continue;
                const key = `${communityId}|${billId}`;
                if (seen.has(key)) continue;
                seen.add(key);
                allBills.push(bill);
            }
        }

        return { bills: allBills, missingCount };
    }, [fetchRelatedBillsForReceipt]);
    const verifyBillsForReset = useCallback(async (successBills: Bill[]) => {
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

    const handlePreviewVoucherForReceipt = useCallback(async (receipt: ReceiptBill) => {
        setVoucherPreview({ isOpen: true, data: null, isLoading: true, error: null });
        try {
            const result = await previewVoucherForReceipt(Number(receipt.id), Number(receipt.community_id));
            notifySkippedBills(result?.skipped_bills || []);
            setVoucherPreview({ isOpen: true, data: result, isLoading: false, error: null });
        } catch (err: any) {
            setVoucherPreview({
                isOpen: true, data: null, isLoading: false,
                error: err?.response?.data?.detail || err?.message || '预览失败'
            });
        }
    }, [notifySkippedBills]);

    const handlePreviewBatchVoucher = useCallback(async () => {
        const selectedRows = items.filter(r => selectedReceiptKeys.has(buildReceiptSelectionKey(r.id, r.community_id)));
        if (selectedRows.length === 0) {
            showToast('info', '提示', '请先选择收款账单');
            return;
        }
        const receipts = selectedRows
            .map(r => ({ receipt_bill_id: Number(r.id), community_id: Number(r.community_id) }))
            .filter(r => Number.isFinite(r.receipt_bill_id) && Number.isFinite(r.community_id));

        setVoucherPreview({ isOpen: true, data: null, isLoading: true, error: null });
        try {
            const result = await previewBatchVoucherForReceipts(receipts);
            notifySkippedBills(result?.skipped_bills || []);
            setVoucherPreview({ isOpen: true, data: result, isLoading: false, error: null });
        } catch (err: any) {
            setVoucherPreview({
                isOpen: true, data: null, isLoading: false,
                error: err?.response?.data?.detail || err?.message || '预览失败'
            });
        }
    }, [items, notifySkippedBills, selectedReceiptKeys, showToast]);

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
    const refreshReceiptViews = useCallback(async () => {
        await fetchReceiptBills();
    }, [fetchReceiptBills]);
    const getReceiptPushSummary = useCallback((receipt: ReceiptBill): PushStatusSummary => (
        receipt.related_bill_push_summary || summarizePushStatuses([])
    ), [summarizePushStatuses]);
    const handleVerifyReceipt = useCallback(async (receipt: ReceiptBill) => {
        const summary = getReceiptPushSummary(receipt);
        if ((summary.success || 0) === 0) {
            showToast('info', '提示', '当前收款单下没有已推送成功的运营账单');
            return;
        }

        openConfirmModal({
            title: '推送校验',
            message: `将校验该收款单关联的 ${summary.success} 条已推送运营账单，确认继续吗？`,
            confirmText: '开始校验',
            variant: 'primary',
            onConfirm: async () => {
                const relatedBillsForReceipt = await fetchRelatedBillsForReceipt(receipt.id, receipt.community_id);
                const successBills = relatedBillsForReceipt.filter(b => (b.push_status || '') === 'success');
                if (!successBills.length) {
                    showToast('info', '提示', '当前收款单下没有已推送成功的运营账单');
                    return;
                }
                const { resetTargets, existsCount, queryFailed } = await verifyBillsForReset(successBills);
                showToast(
                    'success',
                    '校验完成',
                    `可解除 ${resetTargets.length} 条，金蝶仍存在 ${existsCount} 条，校验失败 ${queryFailed} 条`,
                );
            },
        });
    }, [fetchRelatedBillsForReceipt, getReceiptPushSummary, openConfirmModal, showToast, verifyBillsForReset]);
    const handleResetReceiptBinding = useCallback(async (receipt: ReceiptBill) => {
        const summary = getReceiptPushSummary(receipt);
        if ((summary.success || 0) === 0) {
            showToast('info', '提示', '当前收款单下没有可解除绑定的已推送记录');
            return;
        }

        openConfirmModal({
            title: '确认解除',
            message: `将先校验金蝶凭证是否仍然存在，仅解除可解除的记录（共 ${summary.success} 条已推送运营账单）。继续吗？`,
            confirmText: '确认解除',
            variant: 'danger',
            onConfirm: async () => {
                const relatedBillsForReceipt = await fetchRelatedBillsForReceipt(receipt.id, receipt.community_id);
                const successBills = relatedBillsForReceipt.filter(b => (b.push_status || '') === 'success');
                if (!successBills.length) {
                    showToast('info', '提示', '当前收款单下没有可解除绑定的已推送记录');
                    return;
                }
                const { resetTargets, existsCount, queryFailed } = await verifyBillsForReset(successBills);
                if (resetTargets.length === 0) {
                    showToast('info', '提示', '金蝶凭证仍存在，暂无可解除记录');
                    return;
                }
                const result = await resetBillVoucherBinding(resetTargets, 'receipt_bill_reset');
                if (!result?.success) {
                    showToast('error', '解除失败', '服务端未返回成功结果');
                    return;
                }
                showToast(
                    'success',
                    '解除完成',
                    `已解除 ${resetTargets.length} 条，金蝶仍存在 ${existsCount} 条，校验失败 ${queryFailed} 条`,
                );
                await refreshReceiptViews();
            },
        });
    }, [fetchRelatedBillsForReceipt, getReceiptPushSummary, openConfirmModal, refreshReceiptViews, showToast, verifyBillsForReset]);
    const handleBatchVerify = useCallback(async () => {
        const receipts = Array.from(selectedReceiptRefs.values());
        if (receipts.length === 0) {
            showToast('info', '提示', '请先选择收款账单');
            return;
        }

        openConfirmModal({
            title: '批量推送校验',
            message: `将校验所选 ${receipts.length} 条收款单关联的已推送运营账单，确认继续吗？`,
            confirmText: '开始校验',
            variant: 'primary',
            onConfirm: async () => {
                const { bills: relatedBillsForSelection, missingCount } = await collectRelatedBillsForReceipts(receipts);
                const successBills = relatedBillsForSelection.filter(b => (b.push_status || '') === 'success');
                if (!successBills.length) {
                    showToast('info', '提示', '所选收款单下没有已推送成功的运营账单');
                    return;
                }
                const { resetTargets, existsCount, queryFailed } = await verifyBillsForReset(successBills);
                const missingNotice = missingCount > 0 ? `，另有 ${missingCount} 条收款单未关联运营账单` : '';
                showToast(
                    'success',
                    '校验完成',
                    `可解除 ${resetTargets.length} 条，金蝶仍存在 ${existsCount} 条，校验失败 ${queryFailed} 条${missingNotice}`,
                );
            },
        });
    }, [collectRelatedBillsForReceipts, openConfirmModal, selectedReceiptRefs, showToast, verifyBillsForReset]);
    const handleBatchReset = useCallback(async () => {
        const receipts = Array.from(selectedReceiptRefs.values());
        if (receipts.length === 0) {
            showToast('info', '提示', '请先选择收款账单');
            return;
        }

        openConfirmModal({
            title: '批量解除绑定',
            message: `将先校验金蝶凭证是否存在，仅解除可解除记录（共 ${receipts.length} 条收款单）。继续吗？`,
            confirmText: '确认解除',
            variant: 'danger',
            onConfirm: async () => {
                const { bills: relatedBillsForSelection, missingCount } = await collectRelatedBillsForReceipts(receipts);
                const successBills = relatedBillsForSelection.filter(b => (b.push_status || '') === 'success');
                if (!successBills.length) {
                    showToast('info', '提示', '所选收款单下没有可解除绑定的已推送记录');
                    return;
                }
                const { resetTargets, existsCount, queryFailed } = await verifyBillsForReset(successBills);
                if (resetTargets.length === 0) {
                    showToast('info', '提示', '金蝶凭证仍存在，暂无可解除记录');
                    return;
                }
                const result = await resetBillVoucherBinding(resetTargets, 'receipt_bill_batch_reset');
                if (!result?.success) {
                    showToast('error', '批量解除失败', '服务端未返回成功结果');
                    return;
                }
                const missingNotice = missingCount > 0 ? `，另有 ${missingCount} 条收款单未关联运营账单` : '';
                showToast(
                    'success',
                    '批量解除完成',
                    `已解除 ${resetTargets.length} 条，金蝶仍存在 ${existsCount} 条，校验失败 ${queryFailed} 条${missingNotice}`,
                );
                await refreshReceiptViews();
            },
        });
    }, [collectRelatedBillsForReceipts, openConfirmModal, refreshReceiptViews, selectedReceiptRefs, showToast, verifyBillsForReset]);

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
        { key: 'id', title: '缴费ID' },
        { key: 'asset_name', title: '资产/房号' },
        { key: 'payee', title: '收款人' },
        { key: 'payer_name', title: '付款人' },
        {
            key: 'deal_type_label',
            title: '收入类型',
            width: 120,
            render: (_: any, record: ReceiptBill) => getReceiptBillDealTypeLabel(record.deal_type, record.deal_type_label),
        },
        {
            key: 'income_amount',
            title: '入账金额',
            render: (v: any) => `¥${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
        },
        {
            key: 'drilldown_summary',
            title: '关联数据',
            width: 220,
            render: (_: any, record: ReceiptBill) => {
                const badges = getReceiptDrilldownBadges(record);
                return (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                        {badges.length > 0 && (
                            <div style={{ display: 'flex', gap: '0.35rem', flexWrap: 'wrap' }}>
                                {badges.map((badge) => {
                                    const style = DRILLDOWN_BADGE_STYLES[badge.tone];
                                    return (
                                        <span
                                            key={badge.key}
                                            style={{
                                                display: 'inline-flex',
                                                alignItems: 'center',
                                                padding: '0.12rem 0.45rem',
                                                borderRadius: '999px',
                                                fontSize: '0.72rem',
                                                fontWeight: 700,
                                                background: style.bg,
                                                color: style.color,
                                                border: `1px solid ${style.border}`,
                                            }}
                                        >
                                            {badge.label}
                                        </span>
                                    );
                                })}
                            </div>
                        )}
                        <span style={{ fontWeight: 600, color: record.drilldown_enabled ? '#0f172a' : '#94a3b8' }}>
                            {record.drilldown_summary || '暂无关联数据'}
                        </span>
                    </div>
                );
            },
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
        {
            key: 'push_status_label',
            title: '凭证状态',
            width: 180,
            render: (_: any, record: ReceiptBill) => {
                const statusKey = String(record.push_status || 'not_pushed').trim() || 'not_pushed';
                const statusLabel = String(
                    record.push_status_label || RECEIPT_PUSH_STATUS_LABELS[statusKey] || '未推送'
                ).trim();

                const style = RECEIPT_VOUCHER_STATUS_STYLES[statusKey] || RECEIPT_VOUCHER_STATUS_STYLES.untracked;
                return (
                    <span style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        width: 'fit-content',
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
            key: 'voucher_number',
            title: '金蝶凭证',
            width: 170,
            render: (_: any, record: ReceiptBill) => {
                const primaryText = String(record.voucher_number || '').trim();
                if (!primaryText) return '';

                return (
                    <span style={{ fontWeight: 600, color: record.voucher_number ? '#0f172a' : '#94a3b8' }}>
                        {primaryText}
                    </span>
                );
            },
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
            width: 180,
            render: (_: any, record: ReceiptBill) => {
                const actionMenuKey = buildReceiptSelectionKey(record.id, record.community_id);
                const isActionMenuOpen = openActionMenuKey === actionMenuKey;
                const successCount = Number(getReceiptPushSummary(record).success || 0);
                const canManageVoucher = successCount > 0 && Boolean(record.supports_bill_push_ops);

                return (
                    <div
                        className={`receipt-row-action ${isActionMenuOpen ? 'menu-open' : ''}`}
                        style={{ position: 'relative', display: 'flex', justifyContent: 'flex-end' }}
                    >
                        <div className="receipt-row-action-group">
                            <button
                                className="receipt-row-btn receipt-row-btn-primary"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setOpenActionMenuKey(null);
                                    void handlePreviewVoucherForReceipt(record);
                                }}
                                title={record.supports_bill_push_ops ? '预览凭证（按当前收款单关联账单生成）' : '预览凭证（按收款单模板生成）'}
                            >
                                <FileText size={14} /> 凭证
                            </button>
                            <button
                                className={`receipt-row-btn receipt-row-btn-secondary ${isActionMenuOpen ? 'open' : ''}`}
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setOpenActionMenuKey(current => current === actionMenuKey ? null : actionMenuKey);
                                }}
                                title="更多操作"
                            >
                                <MoreHorizontal size={14} /> 更多
                            </button>
                        </div>

                        {isActionMenuOpen && (
                            <div
                                className="custom-dropdown card-shadow slide-up receipt-row-action-menu"
                                style={{
                                    position: 'absolute',
                                    top: 'calc(100% + 0.35rem)',
                                    right: 0,
                                    width: '164px',
                                    zIndex: 140,
                                    padding: '0.35rem 0',
                                }}
                                onClick={(e) => e.stopPropagation()}
                            >
                                <button
                                    type="button"
                                    className={`dropdown-item receipt-row-menu-item ${!canManageVoucher ? 'is-disabled' : ''}`}
                                    disabled={!canManageVoucher}
                                    onClick={() => {
                                        if (!canManageVoucher) return;
                                        setOpenActionMenuKey(null);
                                        void handleVerifyReceipt(record);
                                    }}
                                    title={canManageVoucher ? '校验金蝶凭证是否仍然存在' : '当前收款单下没有已推送记录'}
                                >
                                    <ShieldCheck size={14} /> 校验凭证
                                </button>
                                <button
                                    type="button"
                                    className={`dropdown-item receipt-row-menu-item is-danger ${!canManageVoucher ? 'is-disabled' : ''}`}
                                    disabled={!canManageVoucher}
                                    onClick={() => {
                                        if (!canManageVoucher) return;
                                        setOpenActionMenuKey(null);
                                        void handleResetReceiptBinding(record);
                                    }}
                                    title={canManageVoucher ? '校验后解除本地绑定状态' : '当前收款单下没有可解除记录'}
                                >
                                    <Link2Off size={14} /> 解除绑定
                                </button>
                                <button
                                    type="button"
                                    className="dropdown-item receipt-row-menu-item"
                                    onClick={() => {
                                        setOpenActionMenuKey(null);
                                        void openReceiptDrilldown(record);
                                    }}
                                    title="按收款单类型查看关联数据"
                                >
                                    <FileText size={14} /> 关联数据
                                </button>
                            </div>
                        )}
                    </div>
                );
            },
        },
    ];
    const selectedReceipts = items.filter(r => selectedReceiptKeys.has(buildReceiptSelectionKey(r.id, r.community_id)));
    const selectedPreviewCount = selectedReceipts.length;
    const selectedSuccessCount = selectedReceipts.reduce((acc, receipt) => acc + Number(getReceiptPushSummary(receipt).success || 0), 0);
    const canBatchPreview = selectedPreviewCount > 0;
    const canBatchVerify = selectedReceiptRefs.size > 0 && selectedSuccessCount > 0;
    const canBatchReset = selectedReceiptRefs.size > 0 && selectedSuccessCount > 0;

    return (
        <div className="page-container">
            <div className={`bills-filter-section ${isOperationsCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <FileText size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">同步与操作</h4>
                    </div>
                    <button className="collapse-toggle" onClick={() => setIsOperationsCollapsed(v => !v)}>
                        {isOperationsCollapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                    </button>
                </div>

                {!isOperationsCollapsed && (
                    <div className="filter-content-wrapper fade-in">
                        <div className="selection-row" style={{ gap: '1rem', alignItems: 'flex-start' }}>
                            <div className="flex items-center gap-3 flex-1 flex-wrap" style={{ minWidth: 0 }}>
                                <div className="flex items-center gap-2" style={{ paddingTop: '0.2rem' }}>
                                    <h3 className="font-bold text-slate-800">收款账单</h3>
                                    <span
                                        className="text-xs text-secondary"
                                        style={{
                                            padding: '0.15rem 0.5rem',
                                            borderRadius: '999px',
                                            background: '#f8fafc',
                                            border: '1px solid #e2e8f0',
                                            whiteSpace: 'nowrap',
                                        }}
                                    >
                                        共 {totalRecords} 条
                                    </span>
                                </div>
                                <div
                                    style={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: '0.5rem',
                                        padding: '0.35rem 0.5rem',
                                        borderRadius: '0.75rem',
                                        background: '#f8fafc',
                                        border: '1px solid #e2e8f0',
                                    }}
                                >
                                    <span className="text-xs text-secondary" style={{ whiteSpace: 'nowrap' }}>园区范围</span>
                                <div className="selection-group" ref={communityDropdownRef} style={{ maxWidth: '220px', minWidth: '180px' }}>
                                    <div className={`custom-select-trigger ${isCommunityDropdownOpen ? 'active' : ''}`} onClick={() => setIsCommunityDropdownOpen(v => !v)}>
                                        <div className="trigger-content">
                                            <span className={communityFilter.length === 0 ? 'placeholder' : 'text-xs truncate'} style={{ maxWidth: '140px' }}>
                                                {communityFilter.length === 0 ? '选择园区...' : `已选 ${communityFilter.length} 个园区`}
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
                                </div>
                                <div className="chips-container" style={{ minHeight: '32px', gap: '0.5rem' }}>
                                    <div className="selected-chip">
                                        <span>已选 {selectedReceiptRefs.size} 条</span>
                                    </div>
                                    {selectedReceiptRefs.size > 0 && (
                                        <div className="selected-chip">
                                            <span>入账合计 ¥{selectedTotalAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>
                                        </div>
                                    )}
                                </div>
                            </div>

                            <div className="flex gap-2 flex-wrap" style={{ justifyContent: 'flex-end' }}>
                                <button className="btn-primary btn-refresh-list" onClick={handleSync}>
                                    <RefreshCw size={14} className={isSyncing && !['completed', 'failed', 'partially_completed'].includes(syncState.status) ? 'animate-spin' : ''} />
                                    {isSyncing && !['completed', 'failed', 'partially_completed'].includes(syncState.status) ? '同步中...' : '同步收款账单'}
                                </button>
                                <button className="btn-outline" onClick={() => void refreshReceiptViews()}>
                                    <RefreshCw size={14} /> 刷新列表
                                </button>

                                <div className="receipt-batch-action" style={{ position: 'relative' }}>
                                    <button
                                        className={`btn-outline btn-batch-actions ${isBatchActionsOpen ? 'open' : ''}`}
                                        onClick={() => {
                                            setIsBatchActionsOpen(v => !v);
                                        }}
                                        title="展开批量操作"
                                        style={{ height: '36px', padding: '0 0.85rem' }}
                                    >
                                        <FileText size={14} />
                                        <span>批量操作</span>
                                        <span className="batch-actions-badge">{selectedReceiptRefs.size}</span>
                                        <ChevronDown size={14} className={`batch-actions-arrow ${isBatchActionsOpen ? 'rotate' : ''}`} />
                                    </button>

                                    {isBatchActionsOpen && (
                                        <div
                                            className="custom-dropdown card-shadow slide-up batch-actions-menu"
                                            style={{
                                                position: 'absolute',
                                                top: 'calc(100% + 0.45rem)',
                                                right: 0,
                                                width: '188px',
                                                zIndex: 130,
                                                padding: '0.35rem 0',
                                            }}
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
                                                    color: canBatchPreview ? '#334155' : '#94a3b8',
                                                    opacity: canBatchPreview ? 1 : 0.5,
                                                    cursor: canBatchPreview ? 'pointer' : 'not-allowed',
                                                }}
                                                onClick={() => {
                                                    if (!canBatchPreview) return;
                                                    setIsBatchActionsOpen(false);
                                                    void handlePreviewBatchVoucher();
                                                }}
                                                title={
                                                    selectedReceiptRefs.size === 0
                                                        ? '请先勾选收款账单'
                                                        : '批量预览凭证（按收款单模板/关联账单生成）'
                                                }
                                            >
                                                <FileText size={14} /> 批量凭证预览
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
                                                    color: canBatchVerify ? '#334155' : '#94a3b8',
                                                    opacity: canBatchVerify ? 1 : 0.5,
                                                    cursor: canBatchVerify ? 'pointer' : 'not-allowed',
                                                }}
                                                disabled={!canBatchVerify}
                                                onClick={() => {
                                                    if (!canBatchVerify) return;
                                                    setIsBatchActionsOpen(false);
                                                    void handleBatchVerify();
                                                }}
                                                title={
                                                    selectedSuccessCount === 0
                                                        ? '所选收款单下没有已推送记录'
                                                        : '批量校验金蝶凭证是否仍然存在'
                                                }
                                            >
                                                <ShieldCheck size={14} /> 批量校验
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
                                                    color: canBatchReset ? '#c2410c' : '#94a3b8',
                                                    opacity: canBatchReset ? 1 : 0.5,
                                                    cursor: canBatchReset ? 'pointer' : 'not-allowed',
                                                }}
                                                disabled={!canBatchReset}
                                                onClick={() => {
                                                    if (!canBatchReset) return;
                                                    setIsBatchActionsOpen(false);
                                                    void handleBatchReset();
                                                }}
                                                title={
                                                    selectedSuccessCount === 0
                                                        ? '所选收款单下没有可解除记录'
                                                        : '校验后批量解除本地绑定状态'
                                                }
                                            >
                                                <Link2Off size={14} /> 批量解除
                                            </button>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>

                        <div
                            style={{
                                border: '1px solid #eef2f7',
                                borderRadius: '0.75rem',
                                background: '#fcfdff',
                                padding: '0.75rem 0.85rem 0.2rem',
                            }}
                        >
                            <div className="flex items-center justify-between" style={{ marginBottom: isConditionCollapsed ? '0' : '0.65rem' }}>
                                <span className="text-xs text-secondary font-medium">筛选条件</span>
                                <button className="btn-text text-xs text-primary flex items-center gap-1" onClick={() => setIsConditionCollapsed(v => !v)}>
                                    {isConditionCollapsed ? '展开筛选' : '收起筛选'} {isConditionCollapsed ? <ChevronDown size={14} /> : <ChevronUp size={14} />}
                                </button>
                            </div>

                            {!isConditionCollapsed && (
                                <div className="action-row flex-wrap" style={{ paddingTop: 0, borderTop: 'none', gap: '0.65rem' }}>
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

                                        <div className="flex items-center gap-1">
                                            <select
                                                className="enhanced-select text-xs"
                                                style={{ width: '150px' }}
                                                value={dealTypeFilter}
                                                onChange={(e) => { setDealTypeFilter(e.target.value); setPage(1); }}
                                            >
                                                <option value="">收入类型</option>
                                                {Object.entries(RECEIPT_BILL_DEAL_TYPE_LABELS).map(([value, label]) => (
                                                    <option key={value} value={value}>{label}</option>
                                                ))}
                                            </select>
                                        </div>

                                        <div className="flex items-center gap-1">
                                            <span className="text-secondary text-xs" style={{ whiteSpace: 'nowrap' }}>收款日期:</span>
                                            <input type="date" className="enhanced-select text-xs" style={{ width: '140px' }} value={dealDateStart} onChange={(e) => { setDealDateStart(e.target.value); setPage(1); }} />
                                            <span className="text-secondary">-</span>
                                            <input type="date" className="enhanced-select text-xs" style={{ width: '140px' }} value={dealDateEnd} onChange={(e) => { setDealDateEnd(e.target.value); setPage(1); }} />
                                        </div>
                                    </div>

                                    <button className="btn-outline" style={{ color: '#ef4444' }} onClick={() => {
                                        setSearchQuery('');
                                        setCommunityFilter([]);
                                        setDealDateStart('');
                                        setDealDateEnd('');
                                        setDealTypeFilter('');
                                        setPage(1);
                                    }}>
                                        <X size={14} /> 重置筛选
                                    </button>
                                </div>
                            )}
                        </div>
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns}
                    data={items}
                    loading={isLoading}
                    serialStart={(page - 1) * pageSize + 1}
                    tableId="receipt-bills-list"
                />

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
                                <span className="text-secondary">|</span>
                                <span>已推送运营账单 {selectedSuccessCount} 条</span>
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

            <ReceiptDrilldownModal
                isOpen={drilldownOpen}
                onClose={() => setDrilldownOpen(false)}
                receiptBill={drilldownReceiptBill}
                loading={drilldownLoading}
                summary={drilldownSummary}
                sections={drilldownSections}
                depositRefundLinkSummary={drilldownDepositRefundSummary}
            />

            <VoucherPreviewModal
                isOpen={voucherPreview.isOpen}
                onClose={() => setVoucherPreview(prev => ({ ...prev, isOpen: false }))}
                data={voucherPreview.data}
                isLoading={voucherPreview.isLoading}
                error={voucherPreview.error}
                onPushVoucher={handlePushVoucher}
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
            />

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

export default ReceiptBills;

