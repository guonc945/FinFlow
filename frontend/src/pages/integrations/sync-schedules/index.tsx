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
    SyncScheduleMeta,
    SyncScheduleTargetMeta,
} from '../../../types';
import type { AccountBook } from '../../../types/accountBook';
import './SyncSchedules.css';

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

const weekdayOrder = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'];

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
        const labels = schedule.weekly_days.join(' / ') || 'MON';
        return `每周 ${labels} ${schedule.daily_time || '00:00'}`;
    }
    return `每日 ${schedule.daily_time || '00:00'}`;
};

const SyncSchedulesPage = () => {
    const { toasts, showToast, removeToast } = useToast();
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

    const targetMap = useMemo(() => {
        const map = new Map<string, SyncScheduleTargetMeta>();
        (meta?.targets || []).forEach((target) => map.set(target.code, target));
        return map;
    }, [meta]);

    const groupedTargets = useMemo(() => {
        const mark = (meta?.targets || []).filter((item) => item.system === 'mark');
        const kingdee = (meta?.targets || []).filter((item) => item.system === 'kingdee');
        return { mark, kingdee };
    }, [meta]);

    const requiresCommunitySelection = useMemo(
        () => formState.target_codes.some((code) => targetMap.get(code)?.requires_community_ids),
        [formState.target_codes, targetMap]
    );

    const markSelectedCount = useMemo(
        () => formState.target_codes.filter((code) => targetMap.get(code)?.system === 'mark').length,
        [formState.target_codes, targetMap]
    );

    const kingdeeSelectedCount = useMemo(
        () => formState.target_codes.filter((code) => targetMap.get(code)?.system === 'kingdee').length,
        [formState.target_codes, targetMap]
    );

    const summary = useMemo(() => {
        const total = schedules.length;
        const enabled = schedules.filter((item) => item.enabled).length;
        const running = schedules.filter((item) => item.is_running).length;
        const failed = schedules.filter((item) => item.last_status === 'failed' || item.last_status === 'partial').length;
        return { total, enabled, running, failed };
    }, [schedules]);

    const loadBaseData = async () => {
        const [metaRes, scheduleRes, latestRunRes, projectRes, accountBookRes] = await Promise.all([
            getSyncScheduleMeta(),
            getSyncSchedules(),
            getLatestSyncScheduleExecutions(20),
            getProjects({ skip: 0, limit: 500 }),
            getAccountBooks(0, 500),
        ]);

        setMeta(metaRes);
        setSchedules(scheduleRes);
        setLatestExecutions(latestRunRes);
        setProjects(Array.isArray(projectRes) ? projectRes : (projectRes?.items || []));
        setAccountBooks(accountBookRes?.items || []);

        setFormState((prev) => ({
            ...prev,
            timezone: prev.timezone || metaRes.default_timezone || 'Asia/Shanghai',
        }));

        setSelectedScheduleId((prev) => {
            if (prev && scheduleRes.some((item) => item.id === prev)) return prev;
            return scheduleRes[0]?.id ?? null;
        });
    };

    const loadSelectedExecutions = async (scheduleId: number) => {
        const data = await getSyncScheduleExecutions(scheduleId, 20);
        setSelectedExecutions(data);
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
                    showToast('error', '加载失败', '无法获取同步计划配置，请稍后重试');
                }
            } finally {
                if (mounted) setLoading(false);
            }
        };
        void boot();
        return () => {
            mounted = false;
        };
    }, [showToast]);

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
    }, [selectedScheduleId]);

    const refreshAll = async (scheduleId?: number | null) => {
        await loadBaseData();
        if (scheduleId) {
            await loadSelectedExecutions(scheduleId);
        }
    };

    const openCreateModal = () => {
        setEditingSchedule(null);
        setActiveTargetTab('mark');
        setFormState(createEmptyForm(meta?.default_timezone || 'Asia/Shanghai'));
        setFormOpen(true);
    };

    const openEditModal = (schedule: SyncSchedule) => {
        setEditingSchedule(schedule);
        const hasMarkTarget = schedule.target_codes.some((code) => targetMap.get(code)?.system === 'mark');
        const hasKingdeeTarget = schedule.target_codes.some((code) => targetMap.get(code)?.system === 'kingdee');
        if (hasMarkTarget || !hasKingdeeTarget) {
            setActiveTargetTab('mark');
        } else {
            setActiveTargetTab('kingdee');
        }
        setFormState({
            name: schedule.name,
            description: schedule.description || '',
            target_codes: [...schedule.target_codes],
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
        setFormOpen(true);
    };

    const closeFormModal = () => {
        if (saving) return;
        setFormOpen(false);
    };

    const toggleTarget = (code: string) => {
        setFormState((prev) => {
            const hasCode = prev.target_codes.includes(code);
            const requestedTargets = hasCode
                ? prev.target_codes.filter((item) => item !== code)
                : [...prev.target_codes, code];
            const nextTargets = applyForcedTargetCodes(requestedTargets, targetMap);
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
                weekly_days: weekdayOrder.filter((item) => next.includes(item)),
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
        setFormState((prev) => ({ ...prev, community_ids: [] }));
    };

    const handleAccountBookChange = (event: ChangeEvent<HTMLSelectElement>) => {
        const book = accountBooks.find((item) => item.number === event.target.value);
        setFormState((prev) => ({
            ...prev,
            account_book_number: book?.number || '',
            account_book_name: book?.name || '',
        }));
    };

    const buildPayload = () => ({
        name: formState.name.trim(),
        description: formState.description.trim() || null,
        target_codes: applyForcedTargetCodes(formState.target_codes, targetMap),
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
        setSaving(true);
        try {
            const payload = buildPayload();
            let saved: SyncSchedule;
            if (editingSchedule) {
                saved = await updateSyncSchedule(editingSchedule.id, payload);
                showToast('success', '保存成功', '同步计划已更新');
            } else {
                saved = await createSyncSchedule(payload);
                showToast('success', '创建成功', '新的同步计划已创建');
            }
            setFormOpen(false);
            setSelectedScheduleId(saved.id);
            await refreshAll(saved.id);
        } catch (error: any) {
            console.error(error);
            showToast('error', '保存失败', error.response?.data?.detail || error.message || '同步计划保存失败');
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
            title: '立即执行同步计划',
            message: `确定立即执行「${schedule.name}」吗？系统会按当前配置复用现有同步能力，并将各同步目标分发到独立进程处理。`,
            confirmText: '立即执行',
            onConfirm: async () => {
                await runSyncScheduleNow(schedule.id);
                showToast('success', '执行已提交', '同步计划已经进入执行队列');
                await refreshAll(schedule.id);
            },
        });
    };

    const handleToggle = async (schedule: SyncSchedule) => {
        try {
            await toggleSyncSchedule(schedule.id, !schedule.enabled);
            showToast('success', schedule.enabled ? '计划已停用' : '计划已启用');
            await refreshAll(schedule.id);
        } catch (error: any) {
            console.error(error);
            showToast('error', '操作失败', error.response?.data?.detail || error.message || '状态切换失败');
        }
    };

    const handleDelete = (schedule: SyncSchedule) => {
        openConfirm({
            title: '删除同步计划',
            message: `删除后将无法恢复「${schedule.name}」的计划配置，但历史执行记录会一并移除。`,
            confirmText: '删除计划',
            variant: 'danger',
            onConfirm: async () => {
                await deleteSyncSchedule(schedule.id);
                showToast('success', '删除成功', '同步计划已删除');
                await refreshAll();
            },
        });
    };

    const renderTargetPills = (schedule: SyncSchedule) => (
        <div className="schedule-pill-group">
            {schedule.target_codes.map((code) => (
                <span key={code} className="schedule-pill">
                    {targetMap.get(code)?.label || code}
                </span>
            ))}
        </div>
    );

    return (
        <div className="sync-schedules-page">
            <section className="sync-schedules-hero">
                <div>
                    <p className="sync-schedules-eyebrow">Unified Sync Control</p>
                    <h1>定时同步管理</h1>
                    <p className="sync-schedules-subtitle">
                        统一管理马克业务与金蝶基础档案同步计划，支持自定义频率、园区范围、账簿上下文和手动执行。
                    </p>
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
                    <small>当前已配置的同步计划数量</small>
                </div>
                <div className="sync-summary-card">
                    <span>启用中</span>
                    <strong>{summary.enabled}</strong>
                    <small>会被调度线程自动扫描执行</small>
                </div>
                <div className="sync-summary-card">
                    <span>执行中</span>
                    <strong>{summary.running}</strong>
                    <small>正在后台分发独立进程处理目标同步项</small>
                </div>
                <div className="sync-summary-card warning">
                    <span>需关注</span>
                    <strong>{summary.failed}</strong>
                    <small>最近一次执行失败或部分成功</small>
                </div>
            </section>

            <section className="sync-layout">
                <div className="schedule-board">
                    <div className="panel-header">
                        <div>
                            <h2>计划列表</h2>
                            <p>每个计划可组合多个同步目标，并独立配置运行频率</p>
                        </div>
                        <div className="panel-header-badge">
                            <Settings2 size={14} />
                            独立管理模块
                        </div>
                    </div>

                    {loading ? (
                        <div className="schedule-empty">
                            <Loader2 size={22} className="spin" />
                            <span>同步计划加载中...</span>
                        </div>
                    ) : schedules.length === 0 ? (
                        <div className="schedule-empty">
                            <CalendarClock size={22} />
                            <span>还没有同步计划，先创建一个吧。</span>
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

                                    {renderTargetPills(schedule)}

                                    <div className="schedule-meta-grid">
                                        <div>
                                            <span>执行频率</span>
                                            <strong>{describeSchedule(schedule)}</strong>
                                        </div>
                                        <div>
                                            <span>账簿上下文</span>
                                            <strong>{schedule.account_book_name || schedule.account_book_number || '未限定'}</strong>
                                        </div>
                                        <div>
                                            <span>园区范围</span>
                                            <strong>{schedule.community_ids.length > 0 ? `${schedule.community_ids.length} 个园区` : '不适用'}</strong>
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
                    )}
                </div>

                <aside className="execution-sidebar">
                    <div className="panel-header">
                        <div>
                            <h2>{selectedSchedule ? `执行记录 · ${selectedSchedule.name}` : '执行记录'}</h2>
                            <p>查看计划级明细与最新全局执行情况</p>
                        </div>
                    </div>

                    <div className="execution-section">
                        <div className="execution-section-title">当前计划</div>
                        {selectedSchedule ? (
                            selectedExecutions.length > 0 ? (
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
                                                <span><CheckCircle2 size={14} /> 成功 {execution.success_targets}</span>
                                                <span><XCircle size={14} /> 失败 {execution.failed_targets}</span>
                                            </div>
                                            {execution.result_payload.length > 0 && (
                                                <div className="execution-target-list">
                                                    {execution.result_payload.map((target) => (
                                                        <span key={`${execution.id}-${target.code}`} className={`execution-target-pill ${target.status}`}>
                                                            {(targetMap.get(target.code)?.label || target.code)} · {formatExecutionStatus(target.status)}
                                                        </span>
                                                    ))}
                                                </div>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="execution-empty">该计划还没有执行记录。</div>
                            )
                        ) : (
                            <div className="execution-empty">先从左侧选择一个计划。</div>
                        )}
                    </div>

                    <div className="execution-section">
                        <div className="execution-section-title">全局最近执行</div>
                        {latestExecutions.length > 0 ? (
                            <div className="timeline-list">
                                {latestExecutions.slice(0, 8).map((execution) => (
                                    <div key={execution.id} className="timeline-item">
                                        <div className={`timeline-dot ${execution.status}`}></div>
                                        <div className="timeline-content">
                                            <strong>{execution.schedule_name || `计划 #${execution.schedule_id}`}</strong>
                                            <span>{formatDateTime(execution.started_at)}</span>
                                            <p>{execution.summary || execution.error_message || '暂无摘要'}</p>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="execution-empty">暂无全局执行记录。</div>
                        )}
                    </div>
                </aside>
            </section>

            {formOpen && (
                <div className="schedule-form-overlay" onClick={closeFormModal}>
                    <div className="schedule-form-modal" onClick={(event) => event.stopPropagation()}>
                        <div className="schedule-form-header">
                            <div>
                                <h2>{editingSchedule ? '编辑同步计划' : '新建同步计划'}</h2>
                                <p>复用现有同步能力，配置独立的定时规则和数据范围。</p>
                            </div>
                            <button type="button" className="modal-close-button" onClick={closeFormModal}>×</button>
                        </div>

                        <form className="schedule-form-body" onSubmit={handleSubmit}>
                            <div className="form-grid two-columns">
                                <label className="field">
                                    <span>计划名称</span>
                                    <input
                                        value={formState.name}
                                        onChange={(event) => setFormState((prev) => ({ ...prev, name: event.target.value }))}
                                        placeholder="例如：凌晨业务主数据同步"
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
                                    rows={3}
                                    placeholder="说明这个计划负责同步哪些数据、用于什么场景"
                                />
                            </label>

                            <div className="target-group-panel">
                                <div className="target-group-header">
                                    <h3>同步目标</h3>
                                    <p>可跨马克与金蝶组合配置，同一计划会并行分发到多个独立进程执行。</p>
                                </div>
                                <div className="target-tab-nav">
                                    <button
                                        type="button"
                                        className={`target-tab ${activeTargetTab === 'mark' ? 'active' : ''}`}
                                        onClick={() => setActiveTargetTab('mark')}
                                    >
                                        马克业务
                                        <span>{markSelectedCount}</span>
                                    </button>
                                    <button
                                        type="button"
                                        className={`target-tab ${activeTargetTab === 'kingdee' ? 'active' : ''}`}
                                        onClick={() => setActiveTargetTab('kingdee')}
                                    >
                                        金蝶财务
                                        <span>{kingdeeSelectedCount}</span>
                                    </button>
                                </div>

                                {activeTargetTab === 'mark' ? (
                                    <div className="target-tab-content">
                                        <div className="target-column">
                                            <div className="target-column-title">
                                                <Landmark size={16} />
                                                马克业务
                                            </div>
                                            {groupedTargets.mark.length > 0 ? (
                                                groupedTargets.mark.map((target) => (
                                                    <label key={target.code} className="target-checkbox">
                                                        <input
                                                            type="checkbox"
                                                            checked={formState.target_codes.includes(target.code)}
                                                            onChange={() => toggleTarget(target.code)}
                                                        />
                                                        <span>{target.label}{target.forced_with?.length ? '（自动联动关联模块）' : ''}</span>
                                                    </label>
                                                ))
                                            ) : (
                                                <p className="target-empty">暂无可用的马克业务同步目标</p>
                                            )}
                                        </div>

                                        <div className={`community-panel ${requiresCommunitySelection ? '' : 'disabled'}`}>
                                            <div className="target-group-header">
                                                <h3>园区范围</h3>
                                                <p>仅对马克同步目标生效，可多选；未选择则无法保存涉及马克数据的计划。</p>
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
                                                {projects.length > 0 ? (
                                                    projects.map((project) => {
                                                        const projectId = Number(project.proj_id);
                                                        if (Number.isNaN(projectId)) return null;
                                                        const checked = formState.community_ids.includes(projectId);
                                                        return (
                                                            <label
                                                                key={project.proj_id}
                                                                className={`community-checkbox ${checked ? 'checked' : ''} ${!requiresCommunitySelection ? 'disabled' : ''}`}
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
                                                ) : (
                                                    <div className="community-empty">暂无园区数据</div>
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
                                            {groupedTargets.kingdee.length > 0 ? (
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
                                            ) : (
                                                <p className="target-empty">暂无可用的金蝶财务同步目标</p>
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
                                            onChange={(event) => setFormState((prev) => ({ ...prev, interval_minutes: event.target.value }))}
                                        />
                                    </label>
                                ) : (
                                    <label className="field">
                                        <span>执行时间</span>
                                        <input
                                            type="time"
                                            value={formState.daily_time}
                                            onChange={(event) => setFormState((prev) => ({ ...prev, daily_time: event.target.value }))}
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
                                    <span>账簿上下文</span>
                                    <select value={formState.account_book_number} onChange={handleAccountBookChange}>
                                        <option value="">不限定账簿</option>
                                        {accountBooks.map((book) => (
                                            <option key={book.id} value={book.number || ''}>
                                                {(book.number || '未编码')} · {book.name}
                                            </option>
                                        ))}
                                    </select>
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

                            <div className="schedule-form-footer">
                                <button type="button" className="sync-action ghost" onClick={closeFormModal}>
                                    取消
                                </button>
                                <button type="submit" className="sync-action primary" disabled={saving}>
                                    {saving ? <Loader2 size={16} className="spin" /> : <Plus size={16} />}
                                    {saving ? '保存中...' : editingSchedule ? '保存修改' : '创建计划'}
                                </button>
                            </div>
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

export default SyncSchedulesPage;
