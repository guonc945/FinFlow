import { useCallback, useEffect, useRef, useState } from 'react';
import { Clock3, DatabaseZap, History } from 'lucide-react';
import axios from 'axios';
import { API_BASE_URL } from '../../services/apiBase';

interface FinanceSyncStatusResponse {
    module_code: string;
    module_key: string;
    label: string;
    description: string;
    status: 'idle' | 'running' | 'success' | 'failed' | string;
    message: string | null;
    started_at: string | null;
    finished_at: string | null;
    last_modifytime_sync_at: string | null;
    last_success_at: string | null;
    last_full_sync_at: string | null;
    has_status: boolean;
}

interface FinanceSyncStatusProps {
    moduleCode: string;
    pollIntervalMs?: number;
}

const FAST_POLL_INTERVAL_MS = 5000;
const FAST_POLL_DURATION_MS = 2 * 60 * 1000;
const TERMINAL_STATUSES = new Set(['success', 'failed']);

export const notifyFinanceSyncStarted = (moduleCode: string) => {
    window.dispatchEvent(new CustomEvent('finance-sync-started', { detail: { moduleCode } }));
};

export const FINANCE_SYNC_FINISHED_EVENT = 'finance-sync-finished';

const formatDateTime = (value?: string | null) => {
    if (!value) return '尚未同步';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return new Intl.DateTimeFormat('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
    }).format(parsed);
};

const STATUS_ITEMS = [
    { key: 'last_success_at', label: '最近成功同步', icon: Clock3 },
    { key: 'last_modifytime_sync_at', label: '当前增量水位', icon: DatabaseZap },
    { key: 'last_full_sync_at', label: '最近全量同步', icon: History },
] as const;

const getStatusSignature = (status: FinanceSyncStatusResponse | null) => (
    status
        ? [
            status.status || '',
            status.message || '',
            status.started_at || '',
            status.finished_at || '',
            status.last_success_at || '',
            status.last_modifytime_sync_at || '',
            status.last_full_sync_at || '',
        ].join('|')
        : ''
);

const FinanceSyncStatus = ({ moduleCode, pollIntervalMs = 30000 }: FinanceSyncStatusProps) => {
    const [status, setStatus] = useState<FinanceSyncStatusResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [activePollIntervalMs, setActivePollIntervalMs] = useState(pollIntervalMs);
    const lastSignatureRef = useRef('');
    const waitingForSyncResultRef = useRef(false);
    const triggerSignatureRef = useRef('');
    const observedRunningRef = useRef(false);

    const applyStatus = useCallback((nextStatus: FinanceSyncStatusResponse) => {
        const nextSignature = getStatusSignature(nextStatus);

        setStatus(nextStatus);

        if (waitingForSyncResultRef.current && nextStatus.status === 'running') {
            observedRunningRef.current = true;
        }

        const shouldDispatchFinished = waitingForSyncResultRef.current
            && TERMINAL_STATUSES.has(nextStatus.status)
            && (observedRunningRef.current || triggerSignatureRef.current !== nextSignature);

        if (shouldDispatchFinished) {
            waitingForSyncResultRef.current = false;
            observedRunningRef.current = false;
            setActivePollIntervalMs(pollIntervalMs);
            window.dispatchEvent(new CustomEvent(FINANCE_SYNC_FINISHED_EVENT, {
                detail: {
                    moduleCode,
                    status: nextStatus,
                },
            }));
        }

        lastSignatureRef.current = nextSignature;
    }, [moduleCode, pollIntervalMs]);

    useEffect(() => {
        setActivePollIntervalMs(pollIntervalMs);
    }, [pollIntervalMs]);

    useEffect(() => {
        let alive = true;
        let fastPollResetTimer: number | null = null;

        const fetchStatus = async () => {
            try {
                const res = await axios.get<FinanceSyncStatusResponse>(`${API_BASE_URL}/finance/sync-modules/${moduleCode}/status`);
                if (alive) {
                    applyStatus(res.data);
                }
            } catch (error) {
                console.error('Failed to load finance sync status:', error);
            } finally {
                if (alive) {
                    setLoading(false);
                }
            }
        };

        const startFastPolling = () => {
            setActivePollIntervalMs(FAST_POLL_INTERVAL_MS);
            if (fastPollResetTimer) {
                window.clearTimeout(fastPollResetTimer);
            }
            fastPollResetTimer = window.setTimeout(() => {
                if (alive) {
                    setActivePollIntervalMs(pollIntervalMs);
                }
            }, FAST_POLL_DURATION_MS);
        };

        const handleSyncStarted = (event: Event) => {
            const customEvent = event as CustomEvent<{ moduleCode?: string }>;
            if (customEvent.detail?.moduleCode !== moduleCode) {
                return;
            }
            triggerSignatureRef.current = lastSignatureRef.current;
            observedRunningRef.current = false;
            waitingForSyncResultRef.current = true;
            startFastPolling();
            void fetchStatus();
        };

        void fetchStatus();
        window.addEventListener('finance-sync-started', handleSyncStarted as EventListener);

        return () => {
            alive = false;
            window.removeEventListener('finance-sync-started', handleSyncStarted as EventListener);
            if (fastPollResetTimer) {
                window.clearTimeout(fastPollResetTimer);
            }
        };
    }, [applyStatus, moduleCode, pollIntervalMs]);

    useEffect(() => {
        const timer = window.setInterval(() => {
            void axios.get<FinanceSyncStatusResponse>(`${API_BASE_URL}/finance/sync-modules/${moduleCode}/status`)
                .then((res) => applyStatus(res.data))
                .catch((error) => console.error('Failed to poll finance sync status:', error))
                .finally(() => setLoading(false));
        }, activePollIntervalMs);

        return () => window.clearInterval(timer);
    }, [activePollIntervalMs, applyStatus, moduleCode]);

    return (
        <div className="finance-sync-status-panel">
            <div className="finance-sync-status-title">
                <span className="finance-sync-status-dot" />
                <span>{status?.label || '同步状态'}</span>
                <span className="finance-sync-status-hint">
                    {loading ? '状态加载中...' : (status?.status === 'running' ? '同步中...' : '自动刷新')}
                </span>
            </div>
            {STATUS_ITEMS.map((item) => {
                const Icon = item.icon;
                const value = status?.[item.key] ?? null;
                return (
                    <div key={item.key} className="finance-sync-status-item">
                        <div className="finance-sync-status-label">
                            <Icon size={12} />
                            <span>{item.label}</span>
                        </div>
                        <div className="finance-sync-status-value">{formatDateTime(value)}</div>
                    </div>
                );
            })}
        </div>
    );
};

export default FinanceSyncStatus;
