import { useState, useEffect, useRef, useCallback, type ReactNode } from 'react';
import {
    Car,
    RefreshCw,
    Pencil,
    CloudSync,
    Search,
    Filter,
    ChevronDown,
    ChevronUp,
    X,
    Info,
    CheckCircle,
    AlertCircle,
    Save,
    Loader2
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import { getParks, syncParks, getProjects, getSyncStatus, updatePark } from '../../services/api';
import type { Park, Project } from '../../types';
import KingdeeHouseSelector from '../../components/finance/KingdeeHouseSelector';
import '../bills/Bills.css'; // Reuse bills styles
import '../houses/Houses.css';

// --- Sub-component: SyncProgressModal (Same as Bills) ---
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
                            <h3>{isCompleted ? (isFailed ? '同步失败' : '同步完成') : '正在同步车位数据'}</h3>
                            <p className="text-secondary text-sm">
                                {isCompleted
                                    ? (isFailed ? '同步过程中发生错误' : `已成功完成 ${total} 个园区的同步`)
                                    : `正在处理园区数据 (共 ${total} 个)`}
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

const Parks = () => {
    const [parks, setParks] = useState<Park[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearchTerm, setDebouncedSearchTerm] = useState('');
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);

    // Multi-select state
    const [selectedProjectIds, setSelectedProjectIds] = useState<string[]>([]);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [projectSearch, setProjectSearch] = useState('');
    const dropdownRef = useRef<HTMLDivElement>(null);

    // Sync state
    const [isSyncing, setIsSyncing] = useState(false);
    const [syncState, setSyncState] = useState({
        taskId: '',
        total: 0,
        current: 0,
        logs: [] as { message: string; type: 'success' | 'error' | 'info'; time: string }[],
        status: 'idle'
    });
    const pollingTimer = useRef<any>(null);

    // Editing & Mapping state
    const [editingPark, setEditingPark] = useState<Park | null>(null);
    const [isSaving, setIsSaving] = useState(false);

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchTerm(searchTerm);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    const fetchParksData = useCallback(async () => {
        setIsLoading(true);
        try {
            const data = await getParks({ search: debouncedSearchTerm });
            setParks(data);
        } catch (error) {
            console.error('Failed to fetch parks:', error);
        } finally {
            setIsLoading(false);
        }
    }, [debouncedSearchTerm]);

    const handleSaveMapping = async () => {
        if (!editingPark) return;
        setIsSaving(true);
        try {
            await updatePark(editingPark.id, { kingdee_house_id: editingPark.kingdee_house_id });
            await fetchParksData();
            setEditingPark(null);
        } catch (error) {
            console.error('Failed to save park mapping:', error);
            alert('保存关联失败，请重试');
        } finally {
            setIsSaving(false);
        }
    };

    useEffect(() => {
        if (!editingPark) return;

        const prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setEditingPark(null);
        };

        document.addEventListener('keydown', handleKeyDown);
        return () => {
            document.body.style.overflow = prevOverflow;
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [editingPark]);

    useEffect(() => {
        fetchParksData();
        loadProjects();

        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsDropdownOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
            if (pollingTimer.current) clearTimeout(pollingTimer.current);
        };
    }, [fetchParksData]);

    const loadProjects = async () => {
        try {
            const data = await getProjects();
            setProjects(data.items || data);
        } catch (e) {
            console.error('Failed to load projects:', e);
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
                    fetchParksData();
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
            alert('请至少选择一个园区进行同步');
            return;
        }
        setIsSyncing(true);
        try {
            const result = await syncParks(selectedProjectIds);
            setSyncState({
                taskId: result.task_id,
                total: selectedProjectIds.length,
                current: 0,
                logs: [{ message: '正在初始化同步任务...', type: 'info', time: new Date().toLocaleTimeString() }],
                status: 'pending'
            });
            startPolling(result.task_id);
        } catch (e) {
            console.error('Sync trigger failed:', e);
            setIsSyncing(false);
            alert('启动同步任务失败。');
        }
    };

    const toggleProject = (id: string) => {
        setSelectedProjectIds(prev =>
            prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]
        );
    };

    // 不再使用前端过滤，因为搜索条件已提交给后端
    const filteredParks = parks;

    const filteredProjectsList = projects.filter(p =>
        p.proj_name.toLowerCase().includes(projectSearch.toLowerCase()) ||
        String(p.proj_id).includes(projectSearch)
    );

    const columns = [
        { key: 'park_id' as keyof Park, title: '车位ID', width: 120 },
        {
            key: 'community_name' as keyof Park,
            title: '所属园区',
            width: 150,
            render: (val: any) => <span className="text-secondary font-medium">{val || '-'}</span>
        },
        {
            key: 'name' as keyof Park,
            title: '车位信息',
            render: (val: any, row: Park): ReactNode => (
                <div className="flex flex-col">
                    <span className="font-medium text-primary-dark">{String(val)}</span>
                    <span className="text-xs text-secondary-light">
                        {row.park_type_name || '类型未知'}
                    </span>
                </div>
            )
        },
        {
            key: 'user_name' as keyof Park,
            title: '持有人/租户',
            render: (val: any) => val ? val : <span className="text-secondary">-</span>
        },
        {
            key: 'house_name' as keyof Park,
            title: '关联房屋',
            render: (val: any) => val ? val : <span className="text-secondary">-</span>
        },
        {
            key: 'created_at' as keyof Park,
            title: '采集时间',
            render: (val: any): ReactNode => new Date(val).toLocaleString()
        },
        {
            key: 'kingdee_house_id' as keyof Park,
            title: '财务系统房号映射',
            width: 220,
            render: (_: any, row: Park): ReactNode => (
                <div
                    className="flex items-center gap-2 cursor-pointer hover:bg-gray-50 p-1 rounded transition-colors group"
                    onClick={() => setEditingPark(row)}
                >
                    {row.kingdee_house ? (
                        <div className="flex flex-col">
                            <span className="text-xs font-semibold text-brand group-hover:text-primary">{row.kingdee_house.name}</span>
                            <span className="text-[10px] text-gray-400 font-mono">wtw8: {row.kingdee_house.wtw8_number || row.kingdee_house.number}</span>
                        </div>
                    ) : (
                        <span className="text-xs text-secondary-light italic group-hover:text-primary flex items-center gap-1">
                            <CloudSync size={12} /> 未设置
                        </span>
                    )}
                </div>
            )
        },
        {
            key: 'actions' as keyof Park,
            title: '操作',
            render: (_: any, row: Park): ReactNode => (
                <div className="flex gap-2">
                    <button
                        className={`icon-action ${row.kingdee_house ? 'text-brand' : 'hover:text-primary'}`}
                        onClick={() => setEditingPark(row)}
                        title="配置金蝶映射"
                    >
                        <Pencil size={16} />
                    </button>
                </div>
            )
        }
    ];

    return (
        <div className="page-container fade-in">
            {/* Filter Section - Consistent with Bills.tsx style */}
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">车位档案同步与筛选</h4>
                    </div>
                    <button className="collapse-toggle" onClick={() => setIsFilterCollapsed(!isFilterCollapsed)}>
                        {isFilterCollapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                    </button>
                </div>

                {!isFilterCollapsed && (
                    <div className="filter-content-wrapper fade-in">
                        <div className="selection-row">
                            <div className="selection-group" ref={dropdownRef}>
                                <div className={`custom-select-trigger ${isDropdownOpen ? 'active' : ''}`} onClick={() => setIsDropdownOpen(!isDropdownOpen)}>
                                    <div className="trigger-content">
                                        <Car size={14} />
                                        <span className={selectedProjectIds.length === 0 ? 'placeholder' : ''}>
                                            {selectedProjectIds.length === 0 ? '选择待同步园区...' : `已选 ${selectedProjectIds.length}`}
                                        </span>
                                    </div>
                                    <ChevronDown size={14} className={`arrow ${isDropdownOpen ? 'rotate' : ''}`} />
                                </div>
                                {isDropdownOpen && (
                                    <div className="custom-dropdown card-shadow slide-up">
                                        <div className="p-2 border-b border-gray-100 flex items-center gap-2">
                                            <Search size={14} className="text-tertiary" />
                                            <input
                                                autoFocus
                                                type="text"
                                                placeholder="搜索园区..."
                                                className="dropdown-search"
                                                value={projectSearch}
                                                onChange={(e) => setProjectSearch(e.target.value)}
                                                onClick={(e) => e.stopPropagation()}
                                            />
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
                            <button
                                className={`btn-primary ${selectedProjectIds.length === 0 ? 'disabled' : ''}`}
                                onClick={handleSync}
                                disabled={selectedProjectIds.length === 0 || (isSyncing && !["completed", "failed"].includes(syncState.status))}
                            >
                                <CloudSync size={14} className={isSyncing && !["completed", "failed"].includes(syncState.status) ? 'animate-spin' : ''} />
                                {isSyncing && !["completed", "failed"].includes(syncState.status) ? '同步中...' : '同步车位档案'}
                            </button>
                        </div>

                        <div className="action-row">
                            <div className="search-group flex-1">
                                <Search size={14} className="search-icon" />
                                <input
                                    type="text"
                                    placeholder="搜索车位名称、持有人..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                />
                            </div>
                            <div className="divider-v"></div>
                            <button className="btn-outline" onClick={fetchParksData}>
                                <RefreshCw size={14} /> 刷新档案
                            </button>
                        </div>
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns}
                    data={filteredParks}
                    loading={isLoading}
                    tableId="parks-list"
                    title={
                        <div className="flex items-center gap-2">
                            <Car size={18} className="text-primary" />
                            <span>车位基本信息档案库</span>
                            <span className="text-xs font-normal text-secondary ml-2">
                                当前加载 {filteredParks.length} 条
                            </span>
                        </div>
                    }
                />
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

            {/* Edit/Mapping Modal */}
            {editingPark && (
                <div
                    className="kd-house-mapping-overlay"
                    role="dialog"
                    aria-modal="true"
                    onMouseDown={(e) => {
                        if (e.target === e.currentTarget) setEditingPark(null);
                    }}
                >
                    <div className="kd-house-mapping-modal kd-house-edit-modal">
                        <div className="kd-house-mapping-header">
                            <div>
                                <div className="kd-house-mapping-title">
                                    <AlertCircle size={18} className="text-primary" />
                                    <h3>配置车位与金蝶房号映射</h3>
                                </div>
                                <div className="kd-house-mapping-subtitle">
                                    选择对应的金蝶房号；保存后用于费用对账/推送的档案关联。
                                </div>
                            </div>
                            <button className="modal-close-btn kd-house-mapping-close" onClick={() => setEditingPark(null)} type="button" aria-label="关闭">
                                <X size={20} />
                            </button>
                        </div>

                        <div className="kd-house-mapping-body">
                            <div className="kd-house-mapping-housecard">
                                <span className="kd-house-mapping-housecard-label">当前车位</span>
                                <div className="kd-house-mapping-housecard-name">{editingPark.name}</div>
                                <div className="kd-house-mapping-housecard-meta flex flex-col gap-1">
                                    <span>所属园区：{editingPark.community_name || '-'}</span>
                                    <span>关联房屋：{editingPark.house_name || '-'}</span>
                                    <span>持有/租户：{editingPark.user_name || '-'}</span>
                                </div>
                            </div>

                            <div className="flex flex-col gap-4">
                                <KingdeeHouseSelector
                                    label="财务系统房号映射"
                                    placeholder="搜索或手动输入金蝶房号(wtw8)..."
                                    value={
                                        editingPark.kingdee_house
                                            ? `${editingPark.kingdee_house.wtw8_number || editingPark.kingdee_house.number} ${editingPark.kingdee_house.name}`
                                            : editingPark.kingdee_house_id
                                    }
                                    defaultSearch={editingPark.house_name || editingPark.name}
                                    autoOpen={true}
                                    onSelect={(house) => {
                                        setEditingPark({
                                            ...editingPark,
                                            kingdee_house_id: house?.id || undefined,
                                            kingdee_house: house || undefined
                                        });
                                    }}
                                />
                                <div className="kd-house-mapping-hint">
                                    提示：可直接搜索 <code>wtw8 房号</code> / <code>系统内码</code> / <code>房屋名称</code>
                                </div>
                            </div>
                        </div>

                        <div className="kd-house-mapping-footer">
                            <button className="btn btn-outline" onClick={() => setEditingPark(null)} type="button">
                                取消
                            </button>
                            <button className="btn btn-primary" onClick={handleSaveMapping} disabled={isSaving} type="button">
                                {isSaving ? <Loader2 className="animate-spin" size={16} /> : <Save size={16} />}
                                保存映射配置
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Parks;
