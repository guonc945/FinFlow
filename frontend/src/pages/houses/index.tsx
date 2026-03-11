import { useState, useEffect, useRef, useCallback, type ReactNode } from 'react';
import {
    Home,
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
    Save
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import { getHouses, syncHouses, getProjects, getSyncStatus, updateHouse } from '../../services/api';
import type { House, Project } from '../../types';
import KingdeeHouseSelector from '../../components/finance/KingdeeHouseSelector';
import '../bills/Bills.css'; // Reuse bills styles
import '../charge-items/ChargeItems.css'; // For the modal style
import './Houses.css';

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
                            <h3>{isCompleted ? (isFailed ? '同步失败' : '同步完成') : '正在同步房屋数据'}</h3>
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

const Houses = () => {
    const [houses, setHouses] = useState<House[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearchTerm, setDebouncedSearchTerm] = useState('');
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);

    // Editing mapping
    const [editingHouse, setEditingHouse] = useState<House | null>(null);
    const [isSaving, setIsSaving] = useState(false);

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

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchTerm(searchTerm);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    const fetchHousesData = useCallback(async () => {
        setIsLoading(true);
        try {
            const data = await getHouses({ search: debouncedSearchTerm });
            setHouses(data);
        } catch (error) {
            console.error('Failed to fetch houses:', error);
        } finally {
            setIsLoading(false);
        }
    }, [debouncedSearchTerm]);

    const handleSaveEdit = async () => {
        if (!editingHouse) return;
        setIsSaving(true);
        try {
            await updateHouse(editingHouse.id, {
                kingdee_house_id: editingHouse.kingdee_house_id,
            });
            setEditingHouse(null);
            fetchHousesData();
        } catch (error) {
            console.error('Failed to update house mapping:', error);
        } finally {
            setIsSaving(false);
        }
    };

    useEffect(() => {
        if (!editingHouse) return;

        const prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setEditingHouse(null);
        };

        document.addEventListener('keydown', handleKeyDown);
        return () => {
            document.body.style.overflow = prevOverflow;
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [editingHouse]);


    useEffect(() => {
        fetchHousesData();
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
    }, [fetchHousesData]);

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
                    fetchHousesData();
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
            const result = await syncHouses(selectedProjectIds);
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
    const filteredHouses = houses;

    const filteredProjectsList = projects.filter(p =>
        p.proj_name.toLowerCase().includes(projectSearch.toLowerCase()) ||
        String(p.proj_id).includes(projectSearch)
    );

    const columns = [
        { key: 'house_id' as keyof House, title: '房屋ID', width: 120 },
        {
            key: 'community_name' as keyof House,
            title: '所属园区',
            width: 150,
            render: (val: any) => <span className="text-secondary font-medium">{val || '-'}</span>
        },
        {
            key: 'house_name' as keyof House,
            title: '房屋名称',
            render: (val: any, row: House): ReactNode => (
                <div className="flex flex-col">
                    <span className="font-medium text-primary-dark">{String(val)}</span>
                    <span className="text-xs text-secondary-light">
                        {row.building_name || '-'}
                        {row.unit_name ? ` / ${row.unit_name}` : ''}
                        {row.layer != null ? ` / ${row.layer}层` : (row.floor_name ? ` / ${row.floor_name}层` : '')}
                    </span>
                </div>
            )
        },
        {
            key: 'area' as keyof House,
            title: '面积 (m²)',
            width: 120,
            render: (val: any) => val ? `${Number(val).toFixed(2)}` : '-'
        },
        {
            key: 'user_num' as keyof House,
            title: '住户',
            width: 220,
            render: (_: any, row: House): ReactNode => {
                const names = (row.user_list || []).map(u => u.name).filter(Boolean) as string[];
                const text = names.join('、') || '-';
                const display = text.length > 22 ? `${text.slice(0, 22)}...` : text;
                const count = row.user_num ?? names.length;
                return (
                    <div className="flex flex-col">
                        <span className="text-sm font-medium">{count ?? 0} 人</span>
                        <span className="text-xs text-secondary-light" title={text}>{display}</span>
                    </div>
                );
            }
        },
        {
            key: 'park_num' as keyof House,
            title: '车位',
            width: 220,
            render: (_: any, row: House): ReactNode => {
                const names = (row.park_list || []).map(p => p.name).filter(Boolean) as string[];
                const text = names.join('、') || '-';
                const display = text.length > 22 ? `${text.slice(0, 22)}...` : text;
                const count = row.park_num ?? names.length;
                return (
                    <div className="flex flex-col">
                        <span className="text-sm font-medium">{count ?? 0} 个</span>
                        <span className="text-xs text-secondary-light" title={text}>{display}</span>
                    </div>
                );
            }
        },
        {
            key: 'kingdee_house' as keyof House,
            title: '财务系统房号映射',
            width: 250,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">wtw8: {val.wtw8_number}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'created_at' as keyof House,
            title: '采集时间',
            render: (val: any): ReactNode => new Date(val).toLocaleString()
        },
        {
            key: 'actions' as keyof House,
            title: '操作',
            render: (_: any, item: House): ReactNode => (
                <div className="flex gap-2">
                    <button className="icon-action hover:text-primary" onClick={() => setEditingHouse(item)} title="配置金蝶映射"><Pencil size={16} /></button>
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
                        <h4 className="text-sm font-semibold">房屋档案同步与筛选</h4>
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
                                        <Home size={14} />
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
                                {isSyncing && !["completed", "failed"].includes(syncState.status) ? '同步中...' : '同步房屋档案'}
                            </button>
                        </div>

                        <div className="action-row">
                            <div className="search-group flex-1">
                                <Search size={14} className="search-icon" />
                                <input
                                    type="text"
                                    placeholder="搜索房屋名称、房号、楼栋..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                />
                            </div>
                            <div className="divider-v"></div>
                            <button className="btn-outline" onClick={fetchHousesData}>
                                <RefreshCw size={14} /> 刷新档案
                            </button>
                        </div>
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns}
                    data={filteredHouses}
                    loading={isLoading}
                    title={
                        <div className="flex items-center gap-2">
                            <Home size={18} className="text-primary" />
                            <span>房屋基本信息档案库</span>
                            <span className="text-xs font-normal text-secondary ml-2">
                                当前加载 {filteredHouses.length} 条
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

            {/* Edit Modal */}
            {editingHouse && (
                <div
                    className="kd-house-mapping-overlay"
                    role="dialog"
                    aria-modal="true"
                    onMouseDown={(e) => {
                        if (e.target === e.currentTarget) setEditingHouse(null);
                    }}
                >
                    <div className="kd-house-mapping-modal kd-house-edit-modal">
                        <div className="kd-house-mapping-header">
                            <div>
                                <div className="kd-house-mapping-title">
                                    <AlertCircle size={18} className="text-primary" />
                                    <h3>配置金蝶系统房号映射</h3>
                                </div>
                                <div className="kd-house-mapping-subtitle">
                                    选择对应的金蝶房号；保存后用于凭证/账单推送的档案关联。
                                </div>
                            </div>
                            <button className="modal-close-btn kd-house-mapping-close" onClick={() => setEditingHouse(null)} type="button">
                                <X size={20} />
                            </button>
                        </div>

                        <div className="kd-house-mapping-body">
                            <div className="kd-house-mapping-housecard">
                                <span className="kd-house-mapping-housecard-label">当前房屋</span>
                                <div className="kd-house-mapping-housecard-name">{editingHouse.house_name}</div>
                                <div className="kd-house-mapping-housecard-meta flex flex-col gap-1">
                                    <span>所属园区：{editingHouse.community_name || '-'}</span>
                                    <span>
                                        住户：
                                        {(editingHouse.user_list || []).length > 0
                                            ? (editingHouse.user_list || []).map(u => u.name).filter(Boolean).join('、')
                                            : '-'}
                                    </span>
                                </div>
                            </div>

                            <div className="flex flex-col gap-4">
                                <KingdeeHouseSelector
                                    label="财务系统房号映射"
                                    placeholder="搜索或手动输入金蝶房号(wtw8)..."
                                    value={editingHouse.kingdee_house ? `${editingHouse.kingdee_house.wtw8_number || editingHouse.kingdee_house.number} ${editingHouse.kingdee_house.name}` : editingHouse.kingdee_house_id}
                                    defaultSearch={editingHouse.house_name}
                                    autoOpen={true}
                                    onSelect={(house) => {
                                        setEditingHouse({
                                            ...editingHouse,
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
                            <button className="btn btn-outline" onClick={() => setEditingHouse(null)}>
                                取消
                            </button>
                            <button
                                className="btn btn-primary"
                                onClick={handleSaveEdit}
                                disabled={isSaving}
                            >
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

// Simple loader helper
const Loader2 = ({ className, size }: { className?: string, size?: number }) => (
    <RefreshCw size={size || 16} className={`animate-spin ${className}`} />
);

export default Houses;
