import { useEffect, useMemo, useState } from 'react';
import type { ChangeEvent, FormEvent } from 'react';
import {
    CalendarClock,
    CheckCircle2,
    Landmark,
    Loader2,
    Pause,
    Pencil,
    Play,
    Plus,
    RefreshCw,
    Settings2,
    Trash2,
    XCircle,
} from 'lucide-react';
import ConfirmModal from '../../../components/common/ConfirmModal';
import { ToastContainer, useToast } from '../../../components/Toast';
import { getAccountBooks } from '../../../api/accountBook';
import {
    createSyncSchedule,
    deleteSyncSchedule,
    getLatestSyncScheduleExecutions,
    getProjects,
    getSyncScheduleExecutions,
    getSyncScheduleMeta,
    getSyncSchedules,
    runSyncScheduleNow,
    toggleSyncSchedule,
    updateSyncSchedule,
} from '../../../services/api';
import type {
    Project,
    SyncSchedule,
    SyncScheduleExecution,
    SyncScheduleExecutionTargetResult,
    SyncScheduleMeta,
    SyncScheduleTargetMeta,
} from '../../../types';
import type { AccountBook } from '../../../types/accountBook';
import './SyncSchedules.css';

type ScheduleModuleType = 'data-sync' | 'voucher-push';
type VoucherPushFormStep = 'basic' | 'target' | 'schedule';

type ScheduleFormState = {
    name: string;
    description: string;
    target_codes: string[];
    community_ids: number[];
    account_book_number: string;
    account_book_name: string;
    schedule_type: 'interval' | 'daily' | 'weekly';
    interval_minutes: string;
    daily_time: string;
    weekly_days: string[];
    timezone: string;
    enabled: boolean;
};

type ScheduleModuleCopy = {
    eyebrow: string;
    title: string;
    subtitle: string;
    summaryCaption: string;
    loadingText: string;
    emptyText: string;
    createTitle: string;
    editTitle: string;
    modalDescription: string;
    namePlaceholder: string;
    descriptionPlaceholder: string;
    targetPanelTitle: string;
    targetPanelDescription: string;
    markEmptyText: string;
    kingdeeEmptyText: string;
    executionSubtitle: string;
    autoResolvedTitle: string;
    enabledCaption: string;
    runningCaption: string;
    failedCaption: string;
    scheduleListDescription: string;
    accountBookLabel: string;
    accountBookMetaLabel: string;
    communityMetaLabel: string;
    communityMetaEmptyText: string;
    accountBookRequiredMessage: string;
    accountBookHelperText: string;
    autoResolvedDescription: string;
    createSuccessTitle: string;
    updateSuccessTitle: string;
    deleteSuccessTitle: string;
    toggleSuccessTitle: string;
    runSuccessTitle: string;
    loadErrorText: string;
    targetRequiredMessage: string;
};

const VOUCHER_PUSH_TARGET_CODE = 'receipt_voucher_auto_push';
const WEEKDAY_ORDER = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'];
const VOUCHER_FORM_STEP_ORDER: VoucherPushFormStep[] = ['basic', 'target', 'schedule'];

const MODULE_COPY: Record<ScheduleModuleType, ScheduleModuleCopy> = {
    'data-sync': {
        eyebrow: 'Data Sync Control',
        title: '数据同步计划',
        subtitle: '独立管理马克业务与金蝶基础档案同步计划，支持配置目标、园区、账簿、频率与手动执行。',
        summaryCaption: '当前已配置的数据同步计划数量',
        loadingText: '数据同步计划加载中...',
        emptyText: '还没有数据同步计划，先创建一个吧。',
        createTitle: '新建数据同步计划',
        editTitle: '编辑数据同步计划',
        modalDescription: '复用现有同步能力，配置独立的定时规则和数据范围。',
        namePlaceholder: '例如：凌晨基础资料同步',
        descriptionPlaceholder: '说明这个计划负责同步哪些数据、适用于什么场景',
        targetPanelTitle: '同步目标',
        targetPanelDescription: '可跨马克与金蝶组合配置，同一计划会分发到多个独立目标执行。',
        markEmptyText: '暂无可用的马克同步目标',
        kingdeeEmptyText: '暂无可用的金蝶同步目标',
        executionSubtitle: '查看计划级明细与最近执行情况',
        autoResolvedTitle: '自动解析说明',
        enabledCaption: '会被调度服务自动扫描执行',
        runningCaption: '后台正在处理中的计划数量',
        failedCaption: '最近一次执行失败或部分成功的计划数量',
        scheduleListDescription: '每个计划可组合多个同步目标，并独立配置执行频率。',
        accountBookLabel: '账簿上下文',
        accountBookMetaLabel: '账簿上下文',
        communityMetaLabel: '园区范围',
        communityMetaEmptyText: '不适用',
        accountBookRequiredMessage: '当前所选目标要求必须选择账簿。',
        accountBookHelperText: '如目标需要账簿，执行时会自动传入当前所选账簿。',
        autoResolvedDescription: '已选择 {count} 个会按账簿自动解析园区的目标。',
        createSuccessTitle: '创建成功',
        updateSuccessTitle: '更新成功',
        deleteSuccessTitle: '删除成功',
        toggleSuccessTitle: '状态已更新',
        runSuccessTitle: '已开始执行',
        loadErrorText: '无法获取数据同步计划配置，请稍后重试',
        targetRequiredMessage: '请至少选择一个同步目标',
    },
    'voucher-push': {
        eyebrow: 'Voucher Push Control',
        title: '凭证推送计划',
        subtitle: '独立管理收款单凭证自动推送计划，按账簿、频率严格执行。',
        summaryCaption: '当前已配置的凭证推送计划数量',
        loadingText: '凭证推送计划加载中...',
        emptyText: '还没有凭证推送计划，先创建一个吧。',
        createTitle: '新建凭证推送计划',
        editTitle: '编辑凭证推送计划',
        modalDescription: '配置自动遍历账簿关联园区并推送当天收款单凭证的独立计划。',
        namePlaceholder: '例如：每日收款单凭证推送',
        descriptionPlaceholder: '说明这个计划对应哪个账簿、执行时间和推送规则',
        targetPanelTitle: '推送目标',
        targetPanelDescription: '当前模块仅支持运管收款单目标，请在保存前完成明确勾选。',
        markEmptyText: '凭证推送模块不提供马克侧目标',
        kingdeeEmptyText: '暂无可用的凭证推送目标',
        executionSubtitle: '查看凭证推送计划的执行明细与最近执行情况',
        autoResolvedTitle: '自动推送说明',
        enabledCaption: '到达计划时间后会自动扫描当天待推送收款单',
        runningCaption: '后台正在校验和推送的计划数量',
        failedCaption: '最近一次执行失败或部分成功的计划数量',
        scheduleListDescription: '每个计划对应一个凭证自动推送场景，需显式选择推送目标并配置执行账簿。',
        accountBookLabel: '执行账簿',
        accountBookMetaLabel: '执行账簿',
        communityMetaLabel: '自动解析园区',
        communityMetaEmptyText: '按账簿解析',
        accountBookRequiredMessage: '凭证推送计划必须选择账簿，系统会按账簿自动解析关联园区。',
        accountBookHelperText: '系统会使用所选账簿自动解析园区，并处理执行当天的收款单。',
        autoResolvedDescription: '已选择 {count} 个会按账簿自动解析园区的目标。',
        createSuccessTitle: '创建成功',
        updateSuccessTitle: '更新成功',
        deleteSuccessTitle: '删除成功',
        toggleSuccessTitle: '状态已更新',
        runSuccessTitle: '已开始执行',
        loadErrorText: '无法获取凭证推送计划配置，请稍后重试',
        targetRequiredMessage: '请先选择推送目标后再保存',
    },
};

const createEmptyForm = (timezone: string): ScheduleFormState => ({
    name: '',
    description: '',
    target_codes: [],
    community_ids: [],
    account_book_number: '',
    account_book_name: '',
    schedule_type: 'daily',
    interval_minutes: '60',
    daily_time: '02:00',
    weekly_days: ['MON'],
    timezone,
    enabled: true,
});

const formatDateTime = (value?: string | null) => {
    if (!value) return '未执行';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
    });
};

const formatExecutionStatus = (status?: string | null) => {
    switch (status) {
        case 'success':
            return '成功';
        case 'failed':
            return '失败';
        case 'partial':
            return '部分成功';
        case 'running':
            return '执行中';
        default:
            return status || '未执行';
    }
};

const describeSchedule = (schedule: SyncSchedule) => {
    if (schedule.schedule_type === 'interval') {
        return `每 ${schedule.interval_minutes || 0} 分钟执行`;
    }
    if (schedule.schedule_type === 'weekly') {
        return `每周 ${schedule.weekly_days.join(' / ') || 'MON'} ${schedule.daily_time || '00:00'}`;
    }
    return `每日 ${schedule.daily_time || '00:00'}`;
};

const isVoucherPushTargetCode = (code?: string | null) => String(code || '').trim() === VOUCHER_PUSH_TARGET_CODE;

const isTargetVisibleForModule = (code: string, moduleType: ScheduleModuleType) =>
    moduleType === 'voucher-push' ? isVoucherPushTargetCode(code) : !isVoucherPushTargetCode(code);

const applyForcedTargetCodes = (targetCodes: string[], targetMap: Map<string, SyncScheduleTargetMeta>) => {
    const seen = new Set<string>();
    const normalized: string[] = [];

    for (const code of targetCodes) {
        if (!code || seen.has(code)) continue;
        seen.add(code);
        normalized.push(code);

        const forcedTargets = targetMap.get(code)?.forced_with || [];
        for (const forcedCode of forcedTargets) {
            if (!forcedCode || seen.has(forcedCode)) continue;
            seen.add(forcedCode);
            normalized.push(forcedCode);
        }
    }

    return normalized;
};

const hasExecutionTargetMetrics = (target: SyncScheduleExecutionTargetResult) =>
    [target.scanned_receipts, target.pushed_receipts, target.skipped_receipts, target.failed_receipts].some(
        (value) => typeof value === 'number'
    );

const renderExecutionTargetMetrics = (target: SyncScheduleExecutionTargetResult) => {
    const metrics = [
        { label: '扫描', value: target.scanned_receipts },
        { label: '成功', value: target.pushed_receipts },
        { label: '跳过', value: target.skipped_receipts },
        { label: '失败', value: target.failed_receipts },
    ].filter((item) => typeof item.value === 'number');

    if (metrics.length === 0) return null;

    return (
        <div className="execution-target-metrics">
            {metrics.map((item) => (
                <span key={item.label}>
                    <strong>{item.value}</strong>
                    {item.label}
                </span>
            ))}
        </div>
    );
};

export const SyncSchedulesPage = ({ moduleType = 'data-sync' }: { moduleType?: ScheduleModuleType }) => {
    const { toasts, showToast, removeToast } = useToast();
    const copy = MODULE_COPY[moduleType];
    const isVoucherPushModule = moduleType === 'voucher-push';

    const [meta, setMeta] = useState<SyncScheduleMeta | null>(null);
    const [schedules, setSchedules] = useState<SyncSchedule[]>([]);
    const [selectedScheduleId, setSelectedScheduleId] = useState<number | null>(null);
    const [selectedExecutions, setSelectedExecutions] = useState<SyncScheduleExecution[]>([]);
    const [latestExecutions, setLatestExecutions] = useState<SyncScheduleExecution[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [accountBooks, setAccountBooks] = useState<AccountBook[]>([]);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [formOpen, setFormOpen] = useState(false);
    const [editingSchedule, setEditingSchedule] = useState<SyncSchedule | null>(null);
    const [activeTargetTab, setActiveTargetTab] = useState<'mark' | 'kingdee'>('mark');
    const [voucherFormStep, setVoucherFormStep] = useState<VoucherPushFormStep>('basic');
    const [formState, setFormState] = useState<ScheduleFormState>(createEmptyForm('Asia/Shanghai'));
    const [confirmState, setConfirmState] = useState<{
        open: boolean;
        title: string;
        message: string;
        confirmText: string;
        variant?: 'primary' | 'danger';
        onConfirm: null | (() => Promise<void>);
    }>({
        open: false,
        title: '',
        message: '',
        confirmText: '确定',
        variant: 'primary',
        onConfirm: null,
    });
    const [confirmLoading, setConfirmLoading] = useState(false);

    const selectedSchedule = useMemo(
        () => schedules.find((item) => item.id === selectedScheduleId) || null,
        [schedules, selectedScheduleId]
    );

    const voucherFormSteps = useMemo(
        () => [
            {
                id: 'basic' as const,
                title: '基础信息',
                description: '先定义名称、说明和时区',
            },
            {
                id: 'target' as const,
                title: '推送目标',
                description: '明确推送对象和执行账簿',
            },
            {
                id: 'schedule' as const,
                title: '调度规则',
                description: '配置执行频率并确认启停状态',
            },
        ],
        []
    );

    const targetMap = useMemo(() => {
        const map = new Map<string, SyncScheduleTargetMeta>();
        (meta?.targets || []).forEach((target) => map.set(target.code, target));
        return map;
    }, [meta]);

    const groupedTargets = useMemo(() => {
        const targets = meta?.targets || [];
        return {
            mark: targets.filter((item) => item.system === 'mark'),
            kingdee: targets.filter((item) => item.system === 'kingdee'),
        };
    }, [meta]);

    const defaultTargetTab = useMemo<'mark' | 'kingdee'>(
        () => (moduleType === 'voucher-push' || groupedTargets.mark.length === 0 ? 'kingdee' : 'mark'),
        [groupedTargets.mark.length, moduleType]
    );

    const visibleTargetTab = moduleType === 'voucher-push' ? 'kingdee' : activeTargetTab;

    const requiresCommunitySelection = useMemo(
        () => formState.target_codes.some((code) => targetMap.get(code)?.requires_community_ids),
        [formState.target_codes, targetMap]
    );

    const requiresAccountBookSelection = useMemo(
        () => formState.target_codes.some((code) => targetMap.get(code)?.requires_account_book),
        [formState.target_codes, targetMap]
    );

    const autoResolvedCommunityTargets = useMemo(
        () => formState.target_codes.filter((code) => targetMap.get(code)?.auto_resolve_communities),
        [formState.target_codes, targetMap]
    );

    const summary = useMemo(() => {
        const total = schedules.length;
        const enabled = schedules.filter((item) => item.enabled).length;
        const running = schedules.filter((item) => item.is_running).length;
        const failed = schedules.filter((item) => item.last_status === 'failed' || item.last_status === 'partial').length;
        return { total, enabled, running, failed };
    }, [schedules]);

    const selectedScheduleTargetLabels = useMemo(
        () => (selectedSchedule?.target_codes || []).map((code) => targetMap.get(code)?.label || code),
        [selectedSchedule, targetMap]
    );

    const selectedScheduleSummaryItems = useMemo(
        () => [
            {
                label: copy.accountBookMetaLabel,
                value: selectedSchedule?.account_book_name || selectedSchedule?.account_book_number || '未限定',
            },
            {
                label: copy.communityMetaLabel,
                value: selectedSchedule
                    ? selectedSchedule.community_ids.length > 0
                        ? `${selectedSchedule.community_ids.length} 个园区`
                        : copy.communityMetaEmptyText
                    : '请先选择计划',
            },
            {
                label: '执行频率',
                value: selectedSchedule ? describeSchedule(selectedSchedule) : '请先选择计划',
            },
            {
                label: '下次执行',
                value: selectedSchedule ? formatDateTime(selectedSchedule.next_run_at) : '请先选择计划',
            },
        ],
        [copy.accountBookMetaLabel, copy.communityMetaEmptyText, copy.communityMetaLabel, selectedSchedule]
    );

    const loadBaseData = async () => {
        const [metaRes, scheduleRes, latestRunRes, projectRes, accountBookRes] = await Promise.all([
            getSyncScheduleMeta(),
            getSyncSchedules(),
            getLatestSyncScheduleExecutions(20),
            getProjects({ skip: 0, limit: 500 }),
            getAccountBooks(0, 500),
        ]);

        const filteredTargets = (metaRes.targets || []).filter((target) =>
            isTargetVisibleForModule(target.code, moduleType)
        );
        const filteredSchedules = scheduleRes.filter((schedule) =>
            schedule.target_codes.some((code) => isTargetVisibleForModule(code, moduleType))
        );
        const visibleScheduleIds = new Set(filteredSchedules.map((item) => item.id));

        setMeta({
            ...metaRes,
            targets: filteredTargets,
        });
        setSchedules(filteredSchedules);
        setLatestExecutions(latestRunRes.filter((item) => visibleScheduleIds.has(item.schedule_id)));
        setProjects(Array.isArray(projectRes) ? projectRes : (projectRes?.items || []));
        setAccountBooks(accountBookRes?.items || []);
        setSelectedScheduleId((prev) => {
            if (prev && filteredSchedules.some((item) => item.id === prev)) {
                return prev;
            }
            return filteredSchedules[0]?.id ?? null;
        });
        setFormState((prev) => ({
            ...prev,
            timezone: prev.timezone || metaRes.default_timezone || 'Asia/Shanghai',
        }));
    };

    const loadSelectedExecutions = async (scheduleId: number) => {
        const data = await getSyncScheduleExecutions(scheduleId, 20);
        setSelectedExecutions(data);
    };

    const refreshAll = async (scheduleId?: number | null) => {
        await loadBaseData();
        if (scheduleId) {
            await loadSelectedExecutions(scheduleId);
        }
    };

    useEffect(() => {
        let mounted = true;
        const boot = async () => {
            setLoading(true);
            try {
                await loadBaseData();
            } catch (error) {
                console.error(error);
                if (mounted) {
                    showToast('error', '加载失败', copy.loadErrorText);
                }
            } finally {
                if (mounted) {
                    setLoading(false);
                }
            }
        };
        void boot();
        return () => {
            mounted = false;
        };
    }, [copy.loadErrorText, moduleType, showToast]);

    useEffect(() => {
        if (groupedTargets.mark.length === 0 && activeTargetTab === 'mark') {
            setActiveTargetTab('kingdee');
        }
    }, [activeTargetTab, groupedTargets.mark.length]);

    useEffect(() => {
        setVoucherFormStep('basic');
    }, [moduleType]);

    useEffect(() => {
        if (!selectedScheduleId) {
            setSelectedExecutions([]);
            return;
        }
        void loadSelectedExecutions(selectedScheduleId).catch((error) => {
            console.error(error);
        });
    }, [selectedScheduleId]);

    useEffect(() => {
        const timer = window.setInterval(() => {
            void loadBaseData().catch((error) => console.error(error));
            if (selectedScheduleId) {
                void loadSelectedExecutions(selectedScheduleId).catch((error) => console.error(error));
            }
        }, 30000);
        return () => window.clearInterval(timer);
    }, [selectedScheduleId, moduleType]);

    const openCreateModal = () => {
        setEditingSchedule(null);
        setActiveTargetTab(defaultTargetTab);
        setFormState(createEmptyForm(meta?.default_timezone || 'Asia/Shanghai'));
        setVoucherFormStep('basic');
        setFormOpen(true);
    };

    const openEditModal = (schedule: SyncSchedule) => {
        setEditingSchedule(schedule);
        const editableTargetCodes = schedule.target_codes.filter((code) => targetMap.has(code));
        const hasMarkTarget = editableTargetCodes.some((code) => targetMap.get(code)?.system === 'mark');
        setActiveTargetTab(moduleType === 'voucher-push' ? 'kingdee' : hasMarkTarget ? 'mark' : 'kingdee');
        setFormState({
            name: schedule.name,
            description: schedule.description || '',
            target_codes: editableTargetCodes,
            community_ids: [...schedule.community_ids],
            account_book_number: schedule.account_book_number || '',
            account_book_name: schedule.account_book_name || '',
            schedule_type: schedule.schedule_type,
            interval_minutes: schedule.interval_minutes ? String(schedule.interval_minutes) : '60',
            daily_time: schedule.daily_time || '02:00',
            weekly_days: schedule.weekly_days.length > 0 ? [...schedule.weekly_days] : ['MON'],
            timezone: schedule.timezone || meta?.default_timezone || 'Asia/Shanghai',
            enabled: schedule.enabled,
        });
        setVoucherFormStep('basic');
        setFormOpen(true);
    };

    const closeFormModal = () => {
        if (saving) return;
        setVoucherFormStep('basic');
        setFormOpen(false);
    };

    const toggleTarget = (code: string) => {
        setFormState((prev) => {
            const hasCode = prev.target_codes.includes(code);
            const requestedTargets = hasCode
                ? prev.target_codes.filter((item) => item !== code)
                : [...prev.target_codes, code];
            const nextTargets = applyForcedTargetCodes(
                requestedTargets.filter((item) => targetMap.has(item)),
                targetMap
            );
            const stillRequiresCommunity = nextTargets.some((item) => targetMap.get(item)?.requires_community_ids);
            return {
                ...prev,
                target_codes: nextTargets,
                community_ids: stillRequiresCommunity ? prev.community_ids : [],
            };
        });
    };

    const toggleWeekday = (code: string) => {
        setFormState((prev) => {
            const next = prev.weekly_days.includes(code)
                ? prev.weekly_days.filter((item) => item !== code)
                : [...prev.weekly_days, code];
            return {
                ...prev,
                weekly_days: WEEKDAY_ORDER.filter((item) => next.includes(item)),
            };
        });
    };

    const toggleCommunity = (communityId: number) => {
        if (!requiresCommunitySelection) return;
        setFormState((prev) => {
            const checked = prev.community_ids.includes(communityId);
            return {
                ...prev,
                community_ids: checked
                    ? prev.community_ids.filter((id) => id !== communityId)
                    : [...prev.community_ids, communityId],
            };
        });
    };

    const handleSelectAllCommunities = () => {
        if (!requiresCommunitySelection) return;
        const allCommunityIds = projects
            .map((project) => Number(project.proj_id))
            .filter((value) => !Number.isNaN(value));
        setFormState((prev) => ({
            ...prev,
            community_ids: Array.from(new Set(allCommunityIds)),
        }));
    };

    const handleClearCommunities = () => {
        setFormState((prev) => ({
            ...prev,
            community_ids: [],
        }));
    };

    const handleAccountBookChange = (event: ChangeEvent<HTMLSelectElement>) => {
        const book = accountBooks.find((item) => item.number === event.target.value);
        setFormState((prev) => ({
            ...prev,
            account_book_number: book?.number || '',
            account_book_name: book?.name || '',
        }));
    };

    const validateVoucherFormStep = (step: VoucherPushFormStep) => {
        if (step === 'basic' && !formState.name.trim()) {
            showToast('error', '缺少计划名称', '请先填写计划名称后再继续');
            return false;
        }

        if (step === 'target') {
            const targetCodes = applyForcedTargetCodes(
                formState.target_codes.filter((code) => targetMap.has(code)),
                targetMap
            );
            if (targetCodes.length === 0) {
                showToast('error', '缺少目标', copy.targetRequiredMessage);
                return false;
            }
            if (requiresAccountBookSelection && !formState.account_book_number) {
                showToast('error', '缺少账簿', copy.accountBookRequiredMessage);
                return false;
            }
        }

        if (step === 'schedule' && formState.schedule_type === 'weekly' && formState.weekly_days.length === 0) {
            showToast('error', '缺少执行日', '按周执行时请至少选择一个执行日');
            return false;
        }

        return true;
    };

    const moveVoucherFormStep = (nextStep: VoucherPushFormStep) => {
        const currentIndex = VOUCHER_FORM_STEP_ORDER.indexOf(voucherFormStep);
        const nextIndex = VOUCHER_FORM_STEP_ORDER.indexOf(nextStep);
        if (nextIndex > currentIndex) {
            for (let index = currentIndex; index < nextIndex; index += 1) {
                if (!validateVoucherFormStep(VOUCHER_FORM_STEP_ORDER[index])) {
                    return;
                }
            }
        }
        setVoucherFormStep(nextStep);
    };

    const handleVoucherFormNext = () => {
        const currentIndex = VOUCHER_FORM_STEP_ORDER.indexOf(voucherFormStep);
        if (!validateVoucherFormStep(VOUCHER_FORM_STEP_ORDER[currentIndex])) return;
        const nextStep = VOUCHER_FORM_STEP_ORDER[currentIndex + 1];
        if (nextStep) {
            setVoucherFormStep(nextStep);
        }
    };

    const handleVoucherFormPrev = () => {
        const currentIndex = VOUCHER_FORM_STEP_ORDER.indexOf(voucherFormStep);
        const previousStep = VOUCHER_FORM_STEP_ORDER[currentIndex - 1];
        if (previousStep) {
            setVoucherFormStep(previousStep);
        }
    };

    const buildPayload = () => ({
        name: formState.name.trim(),
        description: formState.description.trim() || null,
        target_codes: applyForcedTargetCodes(
            formState.target_codes.filter((code) => targetMap.has(code)),
            targetMap
        ),
        community_ids: formState.community_ids,
        account_book_number: formState.account_book_number || null,
        account_book_name: formState.account_book_name || null,
        schedule_type: formState.schedule_type,
        interval_minutes: formState.schedule_type === 'interval' ? Number(formState.interval_minutes || 0) : null,
        daily_time: formState.schedule_type === 'interval' ? null : formState.daily_time || null,
        weekly_days: formState.schedule_type === 'weekly' ? formState.weekly_days : [],
        timezone: formState.timezone.trim() || meta?.default_timezone || 'Asia/Shanghai',
        enabled: formState.enabled,
    });

    const handleSubmit = async (event: FormEvent) => {
        event.preventDefault();
        if (isVoucherPushModule && voucherFormStep !== 'schedule') {
            handleVoucherFormNext();
            return;
        }
        const payload = buildPayload();

        if (payload.target_codes.length === 0) {
            showToast('error', '缺少目标', copy.targetRequiredMessage);
            return;
        }

        if (requiresAccountBookSelection && !payload.account_book_number) {
            showToast('error', '缺少账簿', copy.accountBookRequiredMessage);
            return;
        }

        if (payload.schedule_type === 'weekly' && payload.weekly_days.length === 0) {
            showToast('error', '缺少执行日', '按周执行时请至少选择一个执行日');
            return;
        }

        setSaving(true);
        try {
            const saved = editingSchedule
                ? await updateSyncSchedule(editingSchedule.id, payload)
                : await createSyncSchedule(payload);

            showToast(
                'success',
                editingSchedule ? copy.updateSuccessTitle : copy.createSuccessTitle,
                editingSchedule ? `${copy.title}已更新` : `新的${copy.title}已创建`
            );
            setFormOpen(false);
            setSelectedScheduleId(saved.id);
            await refreshAll(saved.id);
        } catch (error: any) {
            console.error(error);
            showToast('error', '保存失败', error.response?.data?.detail || error.message || `${copy.title}保存失败`);
        } finally {
            setSaving(false);
        }
    };

    const openConfirm = (options: {
        title: string;
        message: string;
        confirmText: string;
        variant?: 'primary' | 'danger';
        onConfirm: () => Promise<void>;
    }) => {
        setConfirmState({
            open: true,
            title: options.title,
            message: options.message,
            confirmText: options.confirmText,
            variant: options.variant || 'primary',
            onConfirm: options.onConfirm,
        });
    };

    const closeConfirm = () => {
        if (confirmLoading) return;
        setConfirmState((prev) => ({ ...prev, open: false, onConfirm: null }));
    };

    const handleConfirm = async () => {
        if (!confirmState.onConfirm) return;
        setConfirmLoading(true);
        try {
            await confirmState.onConfirm();
            closeConfirm();
        } finally {
            setConfirmLoading(false);
        }
    };

    const handleRunNow = (schedule: SyncSchedule) => {
        openConfirm({
            title: `立即执行${copy.title}`,
            message:
                moduleType === 'voucher-push'
                    ? `确定立即执行「${schedule.name}」吗？系统会先完成凭证预览校验，再推送金蝶。`
                    : `确定立即执行「${schedule.name}」吗？系统会按当前配置执行同步目标。`,
            confirmText: '立即执行',
            onConfirm: async () => {
                await runSyncScheduleNow(schedule.id);
                showToast('success', copy.runSuccessTitle, `${copy.title}已经进入执行队列`);
                await refreshAll(schedule.id);
            },
        });
    };

    const handleToggle = async (schedule: SyncSchedule) => {
        try {
            await toggleSyncSchedule(schedule.id, !schedule.enabled);
            showToast('success', copy.toggleSuccessTitle, schedule.enabled ? `${copy.title}已停用` : `${copy.title}已启用`);
            await refreshAll(schedule.id);
        } catch (error: any) {
            console.error(error);
            showToast('error', '操作失败', error.response?.data?.detail || error.message || '状态切换失败');
        }
    };

    const handleDelete = (schedule: SyncSchedule) => {
        openConfirm({
            title: `删除${copy.title}`,
            message: `删除后将无法恢复「${schedule.name}」的计划配置，历史执行记录也会被一并移除。`,
            confirmText: '删除计划',
            variant: 'danger',
            onConfirm: async () => {
                await deleteSyncSchedule(schedule.id);
                showToast('success', copy.deleteSuccessTitle, `${copy.title}已删除`);
                await refreshAll();
            },
        });
    };

    const renderSchedulePills = (schedule: SyncSchedule) => (
        <div className="schedule-pill-group">
            {schedule.target_codes.map((code) => (
                <span key={code} className="schedule-pill">
                    {targetMap.get(code)?.label || code}
                </span>
            ))}
        </div>
    );

    const autoResolvedDescription = copy.autoResolvedDescription.replace(
        '{count}',
        String(autoResolvedCommunityTargets.length)
    );

    const currentVoucherFormStepIndex = VOUCHER_FORM_STEP_ORDER.indexOf(voucherFormStep);

    const scheduleListContent = loading ? (
        <div className="schedule-empty">
            <Loader2 size={22} className="spin" />
            <span>{copy.loadingText}</span>
        </div>
    ) : schedules.length === 0 ? (
        <div className="schedule-empty">
            <CalendarClock size={22} />
            <span>{copy.emptyText}</span>
        </div>
    ) : (
        <div className="schedule-card-list">
            {schedules.map((schedule) => (
                <article
                    key={schedule.id}
                    className={`schedule-card ${selectedScheduleId === schedule.id ? 'selected' : ''}`}
                    onClick={() => setSelectedScheduleId(schedule.id)}
                >
                    <div className="schedule-card-top">
                        <div>
                            <div className="schedule-card-title-row">
                                <h3>{schedule.name}</h3>
                                <span className={`status-badge ${schedule.enabled ? 'active' : 'paused'}`}>
                                    {schedule.enabled ? '启用' : '停用'}
                                </span>
                                {schedule.is_running && (
                                    <span className="status-badge running">
                                        <Loader2 size={12} className="spin" />
                                        执行中
                                    </span>
                                )}
                            </div>
                            <p>{schedule.description || '未填写说明'}</p>
                        </div>
                        <div className="schedule-card-actions">
                            <button
                                type="button"
                                className="icon-button"
                                title="立即执行"
                                onClick={(event) => {
                                    event.stopPropagation();
                                    handleRunNow(schedule);
                                }}
                            >
                                <Play size={15} />
                            </button>
                            <button
                                type="button"
                                className="icon-button"
                                title="编辑计划"
                                onClick={(event) => {
                                    event.stopPropagation();
                                    openEditModal(schedule);
                                }}
                            >
                                <Pencil size={15} />
                            </button>
                            <button
                                type="button"
                                className="icon-button"
                                title={schedule.enabled ? '停用计划' : '启用计划'}
                                onClick={(event) => {
                                    event.stopPropagation();
                                    void handleToggle(schedule);
                                }}
                            >
                                {schedule.enabled ? <Pause size={15} /> : <Play size={15} />}
                            </button>
                            <button
                                type="button"
                                className="icon-button danger"
                                title="删除计划"
                                onClick={(event) => {
                                    event.stopPropagation();
                                    handleDelete(schedule);
                                }}
                            >
                                <Trash2 size={15} />
                            </button>
                        </div>
                    </div>

                    {renderSchedulePills(schedule)}

                    <div className="schedule-meta-grid">
                        <div>
                            <span>执行频率</span>
                            <strong>{describeSchedule(schedule)}</strong>
                        </div>
                        <div>
                            <span>{copy.accountBookMetaLabel}</span>
                            <strong>{schedule.account_book_name || schedule.account_book_number || '未限定'}</strong>
                        </div>
                        <div>
                            <span>{copy.communityMetaLabel}</span>
                            <strong>
                                {schedule.community_ids.length > 0
                                    ? `${schedule.community_ids.length} 个园区`
                                    : copy.communityMetaEmptyText}
                            </strong>
                        </div>
                        <div>
                            <span>下次执行</span>
                            <strong>{formatDateTime(schedule.next_run_at)}</strong>
                        </div>
                    </div>

                    <div className="schedule-footer">
                        <div>
                            <span>最近结果</span>
                            <strong>{formatExecutionStatus(schedule.last_status)}</strong>
                        </div>
                        <p>{schedule.last_message || '暂无执行结果'}</p>
                    </div>
                </article>
            ))}
        </div>
    );

    const selectedExecutionSection = (
        <div className="execution-section">
            <div className="execution-section-title">当前计划</div>
            {!selectedSchedule ? (
                <div className="execution-empty">先从左侧选择一个计划。</div>
            ) : selectedExecutions.length === 0 ? (
                <div className="execution-empty">该计划还没有执行记录。</div>
            ) : (
                <div className="execution-list">
                    {selectedExecutions.map((execution) => (
                        <div key={execution.id} className={`execution-card ${execution.status}`}>
                            <div className="execution-card-head">
                                <div>
                                    <strong>{execution.trigger_type === 'manual' ? '手动执行' : '自动执行'}</strong>
                                    <span>{formatDateTime(execution.started_at)}</span>
                                </div>
                                <span className="execution-state">{formatExecutionStatus(execution.status)}</span>
                            </div>
                            <p>{execution.summary || execution.error_message || '暂无摘要'}</p>
                            <div className="execution-counts">
                                <span>
                                    <CheckCircle2 size={14} />
                                    成功 {execution.success_targets}
                                </span>
                                <span>
                                    <XCircle size={14} />
                                    失败 {execution.failed_targets}
                                </span>
                            </div>

                            {execution.result_payload.length > 0 && (
                                <>
                                    <div className="execution-target-list">
                                        {execution.result_payload.map((target) => (
                                            <span
                                                key={`${execution.id}-${target.code}`}
                                                className={`execution-target-pill ${target.status}`}
                                            >
                                                {(targetMap.get(target.code)?.label || target.code) +
                                                    ' · ' +
                                                    formatExecutionStatus(target.status)}
                                            </span>
                                        ))}
                                    </div>
                                    <div className="execution-target-detail-list">
                                        {execution.result_payload.map((target) => (
                                            <div
                                                key={`${execution.id}-${target.code}-detail`}
                                                className={`execution-target-detail ${target.status}`}
                                            >
                                                <div className="execution-target-detail-head">
                                                    <strong>{targetMap.get(target.code)?.label || target.code}</strong>
                                                    <span>{formatExecutionStatus(target.status)}</span>
                                                </div>
                                                <p>{target.message || '暂无执行摘要'}</p>
                                                {(target.account_book_number || target.run_date) && (
                                                    <div className="execution-target-meta">
                                                        {target.account_book_number && <span>账簿：{target.account_book_number}</span>}
                                                        {target.run_date && <span>业务日期：{target.run_date}</span>}
                                                    </div>
                                                )}
                                                {hasExecutionTargetMetrics(target) && renderExecutionTargetMetrics(target)}
                                            </div>
                                        ))}
                                    </div>
                                </>
                            )}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );

    const latestExecutionSection = (
        <div className="execution-section">
            <div className="execution-section-title">全局最近执行</div>
            {latestExecutions.length === 0 ? (
                <div className="execution-empty">暂无全局执行记录。</div>
            ) : (
                <div className="timeline-list">
                    {latestExecutions.slice(0, 8).map((execution) => (
                        <div key={execution.id} className="timeline-item">
                            <div className={`timeline-dot ${execution.status}`} />
                            <div className="timeline-content">
                                <strong>{execution.schedule_name || `计划 #${execution.schedule_id}`}</strong>
                                <span>{formatDateTime(execution.started_at)}</span>
                                <p>{execution.summary || execution.error_message || '暂无摘要'}</p>
                            </div>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );

    const voucherFormContent = (
        <>
            <div className="voucher-form-stepper">
                {voucherFormSteps.map((step, index) => {
                    const isActive = voucherFormStep === step.id;
                    const isDone = index < currentVoucherFormStepIndex;
                    return (
                        <button
                            key={step.id}
                            type="button"
                            className={`voucher-form-step ${isActive ? 'active' : ''} ${isDone ? 'done' : ''}`}
                            onClick={() => moveVoucherFormStep(step.id)}
                        >
                            <span className="voucher-form-step-index">{index + 1}</span>
                            <span className="voucher-form-step-copy">
                                <strong>{step.title}</strong>
                                <small>{step.description}</small>
                            </span>
                        </button>
                    );
                })}
            </div>

            {voucherFormStep === 'basic' && (
                <div className="voucher-form-panel">
                    <div className="voucher-form-panel-header">
                        <h3>步骤 1 · 基础信息</h3>
                        <p>先定义这个凭证推送计划的名称、用途说明和时区。</p>
                    </div>
                    <div className="form-grid two-columns">
                        <label className="field">
                            <span>计划名称</span>
                            <input
                                value={formState.name}
                                onChange={(event) => setFormState((prev) => ({ ...prev, name: event.target.value }))}
                                placeholder={copy.namePlaceholder}
                                required
                            />
                        </label>
                        <label className="field">
                            <span>时区</span>
                            <input
                                value={formState.timezone}
                                onChange={(event) => setFormState((prev) => ({ ...prev, timezone: event.target.value }))}
                                placeholder="Asia/Shanghai"
                            />
                        </label>
                    </div>

                    <label className="field">
                        <span>计划说明</span>
                        <textarea
                            value={formState.description}
                            onChange={(event) => setFormState((prev) => ({ ...prev, description: event.target.value }))}
                            rows={4}
                            placeholder={copy.descriptionPlaceholder}
                        />
                    </label>
                </div>
            )}

            {voucherFormStep === 'target' && (
                <div className="voucher-form-panel">
                    <div className="voucher-form-panel-header">
                        <h3>步骤 2 · 推送目标</h3>
                        <p>明确推送到哪个目标，并绑定执行账簿，账簿关联园区会在执行时自动解析。</p>
                    </div>

                    <div className="target-group-panel">
                        <div className="target-group-header">
                            <h3>{copy.targetPanelTitle}</h3>
                            <p>{copy.targetPanelDescription}</p>
                        </div>
                        <div className="target-tab-content">
                            <div className="target-column">
                                <div className="target-column-title">
                                    <Landmark size={16} />
                                    金蝶财务
                                </div>
                                {groupedTargets.kingdee.length === 0 ? (
                                    <p className="target-empty">{copy.kingdeeEmptyText}</p>
                                ) : (
                                    groupedTargets.kingdee.map((target) => (
                                        <label key={target.code} className="target-checkbox">
                                            <input
                                                type="checkbox"
                                                checked={formState.target_codes.includes(target.code)}
                                                onChange={() => toggleTarget(target.code)}
                                            />
                                            <span>{target.label}</span>
                                        </label>
                                    ))
                                )}
                            </div>
                        </div>
                    </div>

                    <div className="form-grid two-columns">
                        <label className="field">
                            <span>{copy.accountBookLabel}</span>
                            <select value={formState.account_book_number} onChange={handleAccountBookChange}>
                                <option value="">{requiresAccountBookSelection ? '请选择账簿' : '不限定账簿'}</option>
                                {accountBooks.map((book) => (
                                    <option key={book.id} value={book.number || ''}>
                                        {(book.number || '未编码') + ' · ' + book.name}
                                    </option>
                                ))}
                            </select>
                            <small>
                                {requiresAccountBookSelection ? copy.accountBookRequiredMessage : copy.accountBookHelperText}
                            </small>
                        </label>
                        <div className="field">
                            <span>执行闭环</span>
                            <div className="schedule-inline-note">
                                保存前必须明确推送目标和账簿。执行时将按“账簿 / 关联园区 / 当天收款单 / 预览校验 / 推送金蝶”的完整链路处理。
                            </div>
                        </div>
                    </div>

                    {autoResolvedCommunityTargets.length > 0 && (
                        <div className="field">
                            <span>{copy.autoResolvedTitle}</span>
                            <div className="schedule-inline-note">{autoResolvedDescription}</div>
                        </div>
                    )}
                </div>
            )}

            {voucherFormStep === 'schedule' && (
                <div className="voucher-form-panel">
                    <div className="voucher-form-panel-header">
                        <h3>步骤 3 · 调度规则</h3>
                        <p>最后配置执行频率与状态，确认系统何时自动扫描并推送当天收款单凭证。</p>
                    </div>

                    <div className="form-grid two-columns">
                        <label className="field">
                            <span>执行方式</span>
                            <select
                                value={formState.schedule_type}
                                onChange={(event) =>
                                    setFormState((prev) => ({
                                        ...prev,
                                        schedule_type: event.target.value as ScheduleFormState['schedule_type'],
                                    }))
                                }
                            >
                                <option value="interval">按间隔执行</option>
                                <option value="daily">按天执行</option>
                                <option value="weekly">按周执行</option>
                            </select>
                        </label>

                        {formState.schedule_type === 'interval' ? (
                            <label className="field">
                                <span>间隔分钟</span>
                                <input
                                    type="number"
                                    min={5}
                                    value={formState.interval_minutes}
                                    onChange={(event) =>
                                        setFormState((prev) => ({ ...prev, interval_minutes: event.target.value }))
                                    }
                                />
                            </label>
                        ) : (
                            <label className="field">
                                <span>执行时间</span>
                                <input
                                    type="time"
                                    value={formState.daily_time}
                                    onChange={(event) =>
                                        setFormState((prev) => ({ ...prev, daily_time: event.target.value }))
                                    }
                                />
                            </label>
                        )}
                    </div>

                    {formState.schedule_type === 'weekly' && (
                        <div className="field">
                            <span>每周执行日</span>
                            <div className="weekday-grid">
                                {(meta?.weekdays || []).map((item) => (
                                    <label key={item.value} className="weekday-chip">
                                        <input
                                            type="checkbox"
                                            checked={formState.weekly_days.includes(item.value)}
                                            onChange={() => toggleWeekday(item.value)}
                                        />
                                        <span>{item.label}</span>
                                    </label>
                                ))}
                            </div>
                        </div>
                    )}

                    <div className="form-grid two-columns">
                        <label className="field toggle-field">
                            <span>计划状态</span>
                            <button
                                type="button"
                                className={`toggle-switch ${formState.enabled ? 'on' : 'off'}`}
                                onClick={() => setFormState((prev) => ({ ...prev, enabled: !prev.enabled }))}
                            >
                                <span>{formState.enabled ? '启用' : '停用'}</span>
                            </button>
                        </label>
                        <div className="field">
                            <span>执行说明</span>
                            <div className="schedule-inline-note">
                                启用后会按你设定的账簿与频率自动执行；如果任意收款单存在模板或校验不匹配，本次推送会被整体阻拦。
                            </div>
                        </div>
                    </div>
                </div>
            )}

            <div className="schedule-form-footer voucher-form-footer">
                <button type="button" className="sync-action ghost" onClick={closeFormModal}>
                    取消
                </button>
                <div className="voucher-form-footer-actions">
                    {currentVoucherFormStepIndex > 0 && (
                        <button type="button" className="sync-action ghost" onClick={handleVoucherFormPrev}>
                            上一步
                        </button>
                    )}
                    {currentVoucherFormStepIndex < VOUCHER_FORM_STEP_ORDER.length - 1 ? (
                        <button type="button" className="sync-action primary" onClick={handleVoucherFormNext}>
                            下一步
                        </button>
                    ) : (
                        <button type="submit" className="sync-action primary" disabled={saving}>
                            {saving ? <Loader2 size={16} className="spin" /> : <Plus size={16} />}
                            {saving ? '保存中...' : editingSchedule ? '保存修改' : '创建计划'}
                        </button>
                    )}
                </div>
            </div>
        </>
    );

    return (
        <div className={`sync-schedules-page ${isVoucherPushModule ? 'voucher-push-page' : 'data-sync-page'}`}>
            <section className="sync-schedules-hero">
                <div>
                    <p className="sync-schedules-eyebrow">{copy.eyebrow}</p>
                    <h1>{copy.title}</h1>
                    <p className="sync-schedules-subtitle">{copy.subtitle}</p>
                </div>
                <div className="sync-schedules-actions">
                    <button className="sync-action ghost" onClick={() => void refreshAll(selectedScheduleId)}>
                        <RefreshCw size={16} />
                        刷新数据
                    </button>
                    <button className="sync-action primary" onClick={openCreateModal}>
                        <Plus size={16} />
                        新建计划
                    </button>
                </div>
            </section>

            <section className="sync-summary-grid">
                <div className="sync-summary-card">
                    <span>计划总数</span>
                    <strong>{summary.total}</strong>
                    <small>{copy.summaryCaption}</small>
                </div>
                <div className="sync-summary-card">
                    <span>启用中</span>
                    <strong>{summary.enabled}</strong>
                    <small>{copy.enabledCaption}</small>
                </div>
                <div className="sync-summary-card">
                    <span>执行中</span>
                    <strong>{summary.running}</strong>
                    <small>{copy.runningCaption}</small>
                </div>
                <div className="sync-summary-card warning">
                    <span>需关注</span>
                    <strong>{summary.failed}</strong>
                    <small>{copy.failedCaption}</small>
                </div>
            </section>

            {isVoucherPushModule ? (
                <section className="voucher-workbench">
                    {/*
                    {voucherPageStep === 'plans' && (
                        <div className="voucher-stage-layout">
                            <div className="schedule-board">
                                <div className="panel-header">
                                    <div>
                                        <h2>步骤 1 · 计划选择</h2>
                                        <p>{copy.scheduleListDescription}</p>
                                    </div>
                                    <div className="panel-header-badge">
                                        <Settings2 size={14} />
                                        先选计划再进入下一步
                                    </div>
                                </div>
                                {scheduleListContent}
                            </div>

                            <aside className="voucher-stage-sidebar">
                                <div className="voucher-stage-card voucher-stage-focus">
                                    <span className="voucher-stage-kicker">当前选中</span>
                                    <h3>{selectedSchedule?.name || '还没有选择计划'}</h3>
                                    <p>
                                        {selectedSchedule?.description ||
                                            '先从左侧卡片中选中一个凭证推送计划，再查看配置和执行记录。'}
                                    </p>
                                    <div className="voucher-focus-grid">
                                        {selectedScheduleSummaryItems.slice(0, 2).map((item) => (
                                            <div key={item.label}>
                                                <span>{item.label}</span>
                                                <strong>{item.value}</strong>
                                            </div>
                                        ))}
                                    </div>
                                    <div className="voucher-stage-actions">
                                        <button
                                            type="button"
                                            className="sync-action ghost"
                                            onClick={() => setVoucherPageStep('config')}
                                            disabled={!selectedSchedule}
                                        >
                                            查看配置
                                        </button>
                                        <button
                                            type="button"
                                            className="sync-action primary"
                                            onClick={() => setVoucherPageStep('history')}
                                            disabled={!selectedSchedule && latestExecutions.length === 0}
                                        >
                                            查看记录
                                        </button>
                                    </div>
                                </div>

                                <div className="voucher-stage-card">
                                    <span className="voucher-stage-kicker">推送路径</span>
                                    <div className="voucher-flow-list">
                                        <div className="voucher-flow-item">
                                            <strong>1. 锁定执行账簿</strong>
                                            <p>按计划绑定的账簿作为起点，园区范围由账簿自动解析。</p>
                                        </div>
                                        <div className="voucher-flow-item">
                                            <strong>2. 扫描当天收款单</strong>
                                            <p>系统遍历收款日期为当天的数据，逐单准备后续处理。</p>
                                        </div>
                                        <div className="voucher-flow-item">
                                            <strong>3. 凭证预览与校验</strong>
                                            <p>先完成模板匹配、金额闭环和完整性校验，再决定能否继续。</p>
                                        </div>
                                        <div className="voucher-flow-item">
                                            <strong>4. 推送金蝶</strong>
                                            <p>只有全部匹配成功且校验通过，才会执行整单推送。</p>
                                        </div>
                                    </div>
                                </div>
                            </aside>
                        </div>
                    )}

                    {voucherPageStep === 'config' && (
                        <div className="voucher-stage-layout single">
                            <div className="voucher-config-shell">
                                <div className="voucher-config-hero-card">
                                    <div>
                                        <span className="voucher-stage-kicker">步骤 2 · 配置视图</span>
                                        <h2>{selectedSchedule ? selectedSchedule.name : '请选择一个计划'}</h2>
                                        <p>
                                            {selectedSchedule?.description ||
                                                '这里会聚焦展示计划定义、推送目标、账簿和调度规则，不再把执行记录混在同一个视图。'}
                                        </p>
                                    </div>
                                    <div className="voucher-stage-actions">
                                        <button type="button" className="sync-action ghost" onClick={() => setVoucherPageStep('plans')}>
                                            返回选计划
                                        </button>
                                        <button
                                            type="button"
                                            className="sync-action ghost"
                                            onClick={() => selectedSchedule && handleRunNow(selectedSchedule)}
                                            disabled={!selectedSchedule}
                                        >
                                            立即执行
                                        </button>
                                        <button
                                            type="button"
                                            className="sync-action primary"
                                            onClick={() => (selectedSchedule ? openEditModal(selectedSchedule) : openCreateModal())}
                                        >
                                            {selectedSchedule ? '编辑当前计划' : '新建计划'}
                                        </button>
                                    </div>
                                </div>

                                {!selectedSchedule ? (
                                    <div className="schedule-empty">先从“计划选择”步骤里选中一个计划。</div>
                                ) : (
                                    <div className="voucher-config-grid">
                                        <div className="voucher-config-card">
                                            <span className="voucher-stage-kicker">计划定义</span>
                                            <div className="voucher-config-meta">
                                                <div>
                                                    <span>计划状态</span>
                                                    <strong>{selectedSchedule.enabled ? '启用' : '停用'}</strong>
                                                </div>
                                                <div>
                                                    <span>时区</span>
                                                    <strong>{selectedSchedule.timezone || 'Asia/Shanghai'}</strong>
                                                </div>
                                                <div>
                                                    <span>最近结果</span>
                                                    <strong>{formatExecutionStatus(selectedSchedule.last_status)}</strong>
                                                </div>
                                                <div>
                                                    <span>下次执行</span>
                                                    <strong>{formatDateTime(selectedSchedule.next_run_at)}</strong>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="voucher-config-card">
                                            <span className="voucher-stage-kicker">推送范围</span>
                                            <div className="voucher-config-meta">
                                                <div>
                                                    <span>推送目标</span>
                                                    <strong>{selectedScheduleTargetLabels.join('、') || '未配置'}</strong>
                                                </div>
                                                <div>
                                                    <span>{copy.accountBookMetaLabel}</span>
                                                    <strong>
                                                        {selectedSchedule.account_book_name ||
                                                            selectedSchedule.account_book_number ||
                                                            '未限定'}
                                                    </strong>
                                                </div>
                                                <div>
                                                    <span>{copy.communityMetaLabel}</span>
                                                    <strong>
                                                        {selectedSchedule.community_ids.length > 0
                                                            ? `${selectedSchedule.community_ids.length} 个园区`
                                                            : copy.communityMetaEmptyText}
                                                    </strong>
                                                </div>
                                                <div>
                                                    <span>模板匹配</span>
                                                    <strong>必须全量匹配，不允许部分推送</strong>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="voucher-config-card wide">
                                            <span className="voucher-stage-kicker">调度规则</span>
                                            <div className="voucher-config-rule-row">
                                                {selectedScheduleSummaryItems.map((item) => (
                                                    <div key={item.label}>
                                                        <span>{item.label}</span>
                                                        <strong>{item.value}</strong>
                                                    </div>
                                                ))}
                                            </div>
                                            <div className="voucher-config-note">
                                                计划执行时会遵循“账簿 / 关联园区 / 当天收款单 / 凭证预览校验 / JSON 推送”的顺序，
                                                且只允许完整成功，不允许部分成功后继续推送。
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    )}

                    {voucherPageStep === 'history' && (
                        <div className="voucher-history-layout">
                            <div className="execution-sidebar">
                                <div className="panel-header">
                                    <div>
                                        <h2>{selectedSchedule ? `步骤 3 · 执行追踪 · ${selectedSchedule.name}` : '步骤 3 · 执行追踪'}</h2>
                                        <p>{copy.executionSubtitle}</p>
                                    </div>
                                    <div className="panel-header-badge">
                                        <Settings2 size={14} />
                                        执行记录独立查看
                                    </div>
                                </div>
                                {selectedExecutionSection}
                            </div>

                            <aside className="execution-sidebar">
                                <div className="panel-header">
                                    <div>
                                        <h2>全局最近执行</h2>
                                        <p>这里保留最近一次次执行的全局视角，方便排查趋势与失败集中点。</p>
                                    </div>
                                </div>
                                {latestExecutionSection}
                            </aside>
                        </div>
                    )}
                    */}

                    <div className="schedule-board">
                        <div className="panel-header">
                            <div>
                                <h2>计划列表</h2>
                                <p>{copy.scheduleListDescription}</p>
                            </div>
                            <div className="panel-header-badge">
                                <Settings2 size={14} />
                                选择计划后在下方查看配置与记录
                            </div>
                        </div>
                        {scheduleListContent}
                    </div>

                    <div className="voucher-stage-layout single">
                        <div className="voucher-config-shell">
                            <div className="voucher-config-hero-card">
                                <div>
                                    <span className="voucher-stage-kicker">配置概览</span>
                                    <h2>{selectedSchedule ? selectedSchedule.name : '请选择一个计划'}</h2>
                                    <p>
                                        {selectedSchedule?.description ||
                                            '先从上面的计划列表中选中一个计划，再查看配置详情和执行记录。'}
                                    </p>
                                </div>
                                <div className="voucher-stage-actions">
                                    <button
                                        type="button"
                                        className="sync-action ghost"
                                        onClick={() => selectedSchedule && handleRunNow(selectedSchedule)}
                                        disabled={!selectedSchedule}
                                    >
                                        立即执行
                                    </button>
                                    <button
                                        type="button"
                                        className="sync-action primary"
                                        onClick={() => (selectedSchedule ? openEditModal(selectedSchedule) : openCreateModal())}
                                    >
                                        {selectedSchedule ? '编辑当前计划' : '新建计划'}
                                    </button>
                                </div>
                            </div>

                            {!selectedSchedule ? (
                                <div className="schedule-empty">先从上面的计划列表里选中一个计划。</div>
                            ) : (
                                <div className="voucher-config-grid">
                                    <div className="voucher-config-card">
                                        <span className="voucher-stage-kicker">计划定义</span>
                                        <div className="voucher-config-meta">
                                            <div>
                                                <span>计划状态</span>
                                                <strong>{selectedSchedule.enabled ? '启用' : '停用'}</strong>
                                            </div>
                                            <div>
                                                <span>时区</span>
                                                <strong>{selectedSchedule.timezone || 'Asia/Shanghai'}</strong>
                                            </div>
                                            <div>
                                                <span>最近结果</span>
                                                <strong>{formatExecutionStatus(selectedSchedule.last_status)}</strong>
                                            </div>
                                            <div>
                                                <span>下次执行</span>
                                                <strong>{formatDateTime(selectedSchedule.next_run_at)}</strong>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="voucher-config-card">
                                        <span className="voucher-stage-kicker">推送范围</span>
                                        <div className="voucher-config-meta">
                                            <div>
                                                <span>推送目标</span>
                                                <strong>{selectedScheduleTargetLabels.join('、') || '未配置'}</strong>
                                            </div>
                                            <div>
                                                <span>{copy.accountBookMetaLabel}</span>
                                                <strong>
                                                    {selectedSchedule.account_book_name ||
                                                        selectedSchedule.account_book_number ||
                                                        '未限定'}
                                                </strong>
                                            </div>
                                            <div>
                                                <span>{copy.communityMetaLabel}</span>
                                                <strong>
                                                    {selectedSchedule.community_ids.length > 0
                                                        ? `${selectedSchedule.community_ids.length} 个园区`
                                                        : copy.communityMetaEmptyText}
                                                </strong>
                                            </div>
                                            <div>
                                                <span>模板匹配</span>
                                                <strong>必须全量匹配，不允许部分推送</strong>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="voucher-config-card wide">
                                        <span className="voucher-stage-kicker">调度规则</span>
                                        <div className="voucher-config-rule-row">
                                            {selectedScheduleSummaryItems.map((item) => (
                                                <div key={item.label}>
                                                    <span>{item.label}</span>
                                                    <strong>{item.value}</strong>
                                                </div>
                                            ))}
                                        </div>
                                        <div className="voucher-config-note">
                                            计划执行时会遵循“账簿 / 关联园区 / 当天收款单 / 凭证预览校验 / JSON 推送”的顺序，
                                            且只允许完整成功，不允许部分成功后继续推送。
                                        </div>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>

                    <div className="voucher-history-layout">
                        <div className="execution-sidebar">
                            <div className="panel-header">
                                <div>
                                    <h2>{selectedSchedule ? `执行追踪 · ${selectedSchedule.name}` : '执行追踪'}</h2>
                                    <p>{copy.executionSubtitle}</p>
                                </div>
                                <div className="panel-header-badge">
                                    <Settings2 size={14} />
                                    执行记录独立查看
                                </div>
                            </div>
                            {selectedExecutionSection}
                        </div>

                        <aside className="execution-sidebar">
                            <div className="panel-header">
                                <div>
                                    <h2>全局最近执行</h2>
                                    <p>这里保留最近一次批次执行的全局视角，方便排查趋势与失败集中点。</p>
                                </div>
                            </div>
                            {latestExecutionSection}
                        </aside>
                    </div>
                </section>
            ) : (
                <section className="sync-layout">
                    <div className="schedule-board">
                        <div className="panel-header">
                            <div>
                                <h2>计划列表</h2>
                                <p>{copy.scheduleListDescription}</p>
                            </div>
                            <div className="panel-header-badge">
                                <Settings2 size={14} />
                                模块独立管理
                            </div>
                        </div>
                        {scheduleListContent}
                    </div>

                    <aside className="execution-sidebar">
                        <div className="panel-header">
                            <div>
                                <h2>{selectedSchedule ? `执行记录 · ${selectedSchedule.name}` : '执行记录'}</h2>
                                <p>{copy.executionSubtitle}</p>
                            </div>
                        </div>
                        {selectedExecutionSection}
                        {latestExecutionSection}
                    </aside>
                </section>
            )}

            {formOpen && (
                <div className="schedule-form-overlay" onClick={closeFormModal}>
                    <div className="schedule-form-modal" onClick={(event) => event.stopPropagation()}>
                        <div className="schedule-form-header">
                            <div>
                                <h2>{editingSchedule ? copy.editTitle : copy.createTitle}</h2>
                                <p>{copy.modalDescription}</p>
                            </div>
                            <button type="button" className="modal-close-button" onClick={closeFormModal}>
                                ×
                            </button>
                        </div>

                        <form
                            className={`schedule-form-body ${isVoucherPushModule ? 'voucher-form-mode' : ''}`}
                            onSubmit={handleSubmit}
                        >
                            {isVoucherPushModule ? (
                                voucherFormContent
                            ) : (
                                <>
                            <div className="form-grid two-columns">
                                <label className="field">
                                    <span>计划名称</span>
                                    <input
                                        value={formState.name}
                                        onChange={(event) => setFormState((prev) => ({ ...prev, name: event.target.value }))}
                                        placeholder={copy.namePlaceholder}
                                        required
                                    />
                                </label>
                                <label className="field">
                                    <span>时区</span>
                                    <input
                                        value={formState.timezone}
                                        onChange={(event) =>
                                            setFormState((prev) => ({ ...prev, timezone: event.target.value }))
                                        }
                                        placeholder="Asia/Shanghai"
                                    />
                                </label>
                            </div>

                            <label className="field">
                                <span>计划说明</span>
                                <textarea
                                    value={formState.description}
                                    onChange={(event) =>
                                        setFormState((prev) => ({ ...prev, description: event.target.value }))
                                    }
                                    rows={3}
                                    placeholder={copy.descriptionPlaceholder}
                                />
                            </label>

                            <div className="target-group-panel">
                                <div className="target-group-header">
                                    <h3>{copy.targetPanelTitle}</h3>
                                    <p>{copy.targetPanelDescription}</p>
                                </div>

                                {groupedTargets.mark.length > 0 && groupedTargets.kingdee.length > 0 && (
                                    <div className="target-tab-nav">
                                        <button
                                            type="button"
                                            className={`target-tab ${visibleTargetTab === 'mark' ? 'active' : ''}`}
                                            onClick={() => setActiveTargetTab('mark')}
                                        >
                                            马克业务
                                            <span>
                                                {
                                                    formState.target_codes.filter(
                                                        (code) => targetMap.get(code)?.system === 'mark'
                                                    ).length
                                                }
                                            </span>
                                        </button>
                                        <button
                                            type="button"
                                            className={`target-tab ${visibleTargetTab === 'kingdee' ? 'active' : ''}`}
                                            onClick={() => setActiveTargetTab('kingdee')}
                                        >
                                            金蝶财务
                                            <span>
                                                {
                                                    formState.target_codes.filter(
                                                        (code) => targetMap.get(code)?.system === 'kingdee'
                                                    ).length
                                                }
                                            </span>
                                        </button>
                                    </div>
                                )}

                                {visibleTargetTab === 'mark' ? (
                                    <div className="target-tab-content">
                                        <div className="target-column">
                                            <div className="target-column-title">
                                                <Landmark size={16} />
                                                马克业务
                                            </div>
                                            {groupedTargets.mark.length === 0 ? (
                                                <p className="target-empty">{copy.markEmptyText}</p>
                                            ) : (
                                                groupedTargets.mark.map((target) => (
                                                    <label key={target.code} className="target-checkbox">
                                                        <input
                                                            type="checkbox"
                                                            checked={formState.target_codes.includes(target.code)}
                                                            onChange={() => toggleTarget(target.code)}
                                                        />
                                                        <span>
                                                            {target.label}
                                                            {target.forced_with?.length ? '（自动联动关联目标）' : ''}
                                                        </span>
                                                    </label>
                                                ))
                                            )}
                                        </div>

                                        <div className={`community-panel ${requiresCommunitySelection ? '' : 'disabled'}`}>
                                            <div className="target-group-header">
                                                <h3>园区范围</h3>
                                                <p>仅对需要园区范围的马克目标生效，未选择将无法保存相关计划。</p>
                                            </div>
                                            <div className="community-toolbar">
                                                <button
                                                    type="button"
                                                    className="community-action"
                                                    onClick={handleSelectAllCommunities}
                                                    disabled={!requiresCommunitySelection || projects.length === 0}
                                                >
                                                    全选
                                                </button>
                                                <button
                                                    type="button"
                                                    className="community-action"
                                                    onClick={handleClearCommunities}
                                                    disabled={!requiresCommunitySelection || formState.community_ids.length === 0}
                                                >
                                                    清空
                                                </button>
                                            </div>
                                            <div className="community-list">
                                                {projects.length === 0 ? (
                                                    <div className="community-empty">暂无园区数据</div>
                                                ) : (
                                                    projects.map((project) => {
                                                        const projectId = Number(project.proj_id);
                                                        if (Number.isNaN(projectId)) return null;
                                                        const checked = formState.community_ids.includes(projectId);
                                                        return (
                                                            <label
                                                                key={project.proj_id}
                                                                className={`community-checkbox ${checked ? 'checked' : ''} ${
                                                                    !requiresCommunitySelection ? 'disabled' : ''
                                                                }`}
                                                            >
                                                                <input
                                                                    type="checkbox"
                                                                    checked={checked}
                                                                    onChange={() => toggleCommunity(projectId)}
                                                                    disabled={!requiresCommunitySelection}
                                                                />
                                                                <span>{project.proj_name}</span>
                                                                <small>{project.proj_id}</small>
                                                            </label>
                                                        );
                                                    })
                                                )}
                                            </div>
                                            <small>已选择 {formState.community_ids.length} 个园区</small>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="target-tab-content">
                                        <div className="target-column">
                                            <div className="target-column-title">
                                                <Landmark size={16} />
                                                金蝶财务
                                            </div>
                                            {groupedTargets.kingdee.length === 0 ? (
                                                <p className="target-empty">{copy.kingdeeEmptyText}</p>
                                            ) : (
                                                groupedTargets.kingdee.map((target) => (
                                                    <label key={target.code} className="target-checkbox">
                                                        <input
                                                            type="checkbox"
                                                            checked={formState.target_codes.includes(target.code)}
                                                            onChange={() => toggleTarget(target.code)}
                                                        />
                                                        <span>
                                                            {target.label}
                                                            {''}
                                                        </span>
                                                    </label>
                                                ))
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>

                            <div className="form-grid two-columns">
                                <label className="field">
                                    <span>执行方式</span>
                                    <select
                                        value={formState.schedule_type}
                                        onChange={(event) =>
                                            setFormState((prev) => ({
                                                ...prev,
                                                schedule_type: event.target.value as ScheduleFormState['schedule_type'],
                                            }))
                                        }
                                    >
                                        <option value="interval">按间隔执行</option>
                                        <option value="daily">按天执行</option>
                                        <option value="weekly">按周执行</option>
                                    </select>
                                </label>

                                {formState.schedule_type === 'interval' ? (
                                    <label className="field">
                                        <span>间隔分钟</span>
                                        <input
                                            type="number"
                                            min={5}
                                            value={formState.interval_minutes}
                                            onChange={(event) =>
                                                setFormState((prev) => ({ ...prev, interval_minutes: event.target.value }))
                                            }
                                        />
                                    </label>
                                ) : (
                                    <label className="field">
                                        <span>执行时间</span>
                                        <input
                                            type="time"
                                            value={formState.daily_time}
                                            onChange={(event) =>
                                                setFormState((prev) => ({ ...prev, daily_time: event.target.value }))
                                            }
                                        />
                                    </label>
                                )}
                            </div>

                            {formState.schedule_type === 'weekly' && (
                                <div className="field">
                                    <span>每周执行日</span>
                                    <div className="weekday-grid">
                                        {(meta?.weekdays || []).map((item) => (
                                            <label key={item.value} className="weekday-chip">
                                                <input
                                                    type="checkbox"
                                                    checked={formState.weekly_days.includes(item.value)}
                                                    onChange={() => toggleWeekday(item.value)}
                                                />
                                                <span>{item.label}</span>
                                            </label>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div className="form-grid two-columns">
                                <label className="field">
                                    <span>{copy.accountBookLabel}</span>
                                    <select value={formState.account_book_number} onChange={handleAccountBookChange}>
                                        <option value="">
                                            {requiresAccountBookSelection ? '请选择账簿' : '不限定账簿'}
                                        </option>
                                        {accountBooks.map((book) => (
                                            <option key={book.id} value={book.number || ''}>
                                                {(book.number || '未编码') + ' · ' + book.name}
                                            </option>
                                        ))}
                                    </select>
                                    <small>
                                        {requiresAccountBookSelection
                                            ? copy.accountBookRequiredMessage
                                            : copy.accountBookHelperText}
                                    </small>
                                </label>

                                <label className="field toggle-field">
                                    <span>计划状态</span>
                                    <button
                                        type="button"
                                        className={`toggle-switch ${formState.enabled ? 'on' : 'off'}`}
                                        onClick={() => setFormState((prev) => ({ ...prev, enabled: !prev.enabled }))}
                                    >
                                        <span>{formState.enabled ? '启用' : '停用'}</span>
                                    </button>
                                </label>
                            </div>

                            {autoResolvedCommunityTargets.length > 0 && (
                                <div className="field">
                                    <span>{copy.autoResolvedTitle}</span>
                                    <div className="schedule-inline-note">{autoResolvedDescription}</div>
                                </div>
                            )}

                            <div className="schedule-form-footer">
                                <button type="button" className="sync-action ghost" onClick={closeFormModal}>
                                    取消
                                </button>
                                <button type="submit" className="sync-action primary" disabled={saving}>
                                    {saving ? <Loader2 size={16} className="spin" /> : <Plus size={16} />}
                                    {saving ? '保存中...' : editingSchedule ? '保存修改' : '创建计划'}
                                </button>
                            </div>
                                </>
                            )}
                        </form>
                    </div>
                </div>
            )}

            <ConfirmModal
                isOpen={confirmState.open}
                title={confirmState.title}
                message={confirmState.message}
                confirmText={confirmState.confirmText}
                loading={confirmLoading}
                variant={confirmState.variant}
                onCancel={closeConfirm}
                onConfirm={() => {
                    void handleConfirm();
                }}
            />

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

const DataSyncSchedulesPage = () => <SyncSchedulesPage moduleType="data-sync" />;

export default DataSyncSchedulesPage;
