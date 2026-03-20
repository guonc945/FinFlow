import { useState, useEffect, useRef, useCallback, type ReactNode } from 'react';
import {
    Users,
    RefreshCw,
    Pencil,
    Trash,
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
    Loader2,
    ChevronsLeft,
    ChevronLeft,
    ChevronRight,
    ChevronsRight
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import { getResidents, syncResidents, getProjects, getSyncStatus, updateResident } from '../../services/api';
import type { Resident, Project } from '../../types';
import KingdeeCustomerSelector from '../../components/finance/KingdeeCustomerSelector';
import '../bills/Bills.css'; // Reuse bills styles
import '../users/Users.css'; // Reuse modal styles
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
                            <h3>{isCompleted ? (isFailed ? '同步失败' : '同步完成') : '正在同步住户数据'}</h3>
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

const Residents = () => {
    const [residents, setResidents] = useState<Resident[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearchTerm, setDebouncedSearchTerm] = useState('');
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);

    // Pagination state
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);
    const [totalRecords, setTotalRecords] = useState(0);

    // Multi-select state
    const [selectedProjectIds, setSelectedProjectIds] = useState<string[]>([]);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [projectSearch, setProjectSearch] = useState('');
    const dropdownRef = useRef<HTMLDivElement>(null);

    // Edit Modal State
    const [editingResident, setEditingResident] = useState<Resident | null>(null);
    const [isSaving, setIsSaving] = useState(false);

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
            setPage(1);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    const fetchResidentsData = useCallback(async () => {
        setIsLoading(true);
        try {
            const params: any = {
                skip: (page - 1) * pageSize,
                limit: pageSize
            };
            if (debouncedSearchTerm) {
                params.search = debouncedSearchTerm;
            }
            const data = await getResidents(params);
            if (data && data.items) {
                setResidents(data.items);
                setTotalRecords(data.total);
            } else {
                setResidents(Array.isArray(data) ? data : []);
                setTotalRecords(Array.isArray(data) ? data.length : 0);
            }
        } catch (error) {
            console.error('Failed to fetch residents:', error);
        } finally {
            setIsLoading(false);
        }
    }, [page, pageSize, debouncedSearchTerm]);

    useEffect(() => {
        fetchResidentsData();
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
    }, [fetchResidentsData]);

    const handleSaveResident = async () => {
        if (!editingResident) return;
        setIsSaving(true);
        try {
            await updateResident(editingResident.id, {
                kingdee_customer_id: editingResident.kingdee_customer_id
            });
            setEditingResident(null);
            fetchResidentsData();
        } catch (error) {
            console.error('Failed to update resident:', error);
            alert('保存失败，请重试');
        } finally {
            setIsSaving(false);
        }
    };

    useEffect(() => {
        if (!editingResident) return;

        const prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setEditingResident(null);
        };

        document.addEventListener('keydown', handleKeyDown);
        return () => {
            document.body.style.overflow = prevOverflow;
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [editingResident]);

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
                    fetchResidentsData();
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
            const result = await syncResidents(selectedProjectIds);
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
    const filteredResidents = residents;

    const filteredProjectsList = projects.filter(p =>
        p.proj_name.toLowerCase().includes(projectSearch.toLowerCase()) ||
        String(p.proj_id).includes(projectSearch)
    );

    const getHousesText = (houses?: string) => {
        if (!houses) return '-';
        try {
            const housesList = JSON.parse(houses);
            if (!Array.isArray(housesList)) return '-';
            const names = housesList.map((h: any) => h?.name).filter(Boolean);
            return names.length > 0 ? names.join('、') : '-';
        } catch {
            return '-';
        }
    };

    const columns = [
        { key: 'resident_id' as keyof Resident, title: '住户ID', width: 90 },
        {
            key: 'community_name' as keyof Resident,
            title: '所属园区',
            width: 130,
            render: (val: any) => <span className="text-secondary font-medium">{val || '-'}</span>
        },
        {
            key: 'name' as keyof Resident,
            title: '姓名',
            width: 130,
            render: (val: any, row: Resident): ReactNode => (
                <div className="flex flex-col">
                    <span className="font-medium text-primary-dark">{String(val)}</span>
                    <span className="text-xs text-secondary-light">
                        {row.phone || '无电话'}
                    </span>
                </div>
            )
        },
        {
            key: 'houses' as keyof Resident,
            title: '关联房屋',
            width: 200,
            render: (val: any) => {
                const names = getHousesText(val);
                if (names === '-') return <span className="text-secondary">-</span>;
                return (
                    <div className="truncate text-xs" title={names} style={{ maxWidth: '180px' }}>
                        {names}
                    </div>
                );
            }
        },
        {
            key: 'kingdee_customer_id' as keyof Resident,
            title: '财务系统客户映射',
            width: 220,
            render: (_: any, row: Resident): ReactNode => (
                <div
                    className="flex items-center gap-2 cursor-pointer hover:bg-gray-50 p-1 rounded transition-colors group"
                    onClick={() => setEditingResident(row)}
                >
                    {row.kingdee_customer ? (
                        <div className="flex flex-col">
                            <span className="text-xs font-semibold text-brand group-hover:text-primary">{row.kingdee_customer.name}</span>
                            <span className="text-[10px] text-gray-400 font-mono">{row.kingdee_customer.number}</span>
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
            key: 'created_at' as keyof Resident,
            title: '采集时间',
            width: 160,
            render: (val: any): ReactNode => new Date(val).toLocaleString()
        },
        {
            key: 'actions' as keyof Resident,
            title: '操作',
            width: 80,
            render: (_: any, row: Resident): ReactNode => (
                <div className="flex gap-2">
                    <button className="icon-action hover:text-primary" onClick={() => setEditingResident(row)} title="配置财务映射"><Pencil size={16} /></button>
                    <button className="icon-action hover:text-danger text-secondary"><Trash size={16} /></button>
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
                        <h4 className="text-sm font-semibold">住户档案同步与筛选</h4>
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
                                        <Users size={14} />
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
                                {isSyncing && !["completed", "failed"].includes(syncState.status) ? '同步中...' : '同步住户档案'}
                            </button>
                        </div>

                        <div className="action-row">
                            <div className="search-group flex-1">
                                <Search size={14} className="search-icon" />
                                <input
                                    type="text"
                                    placeholder="搜索住户姓名、电话、ID..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                />
                            </div>
                            <div className="divider-v"></div>
                            <button className="btn-outline" onClick={fetchResidentsData}>
                                <RefreshCw size={14} /> 刷新档案
                            </button>
                        </div>
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns}
                    data={filteredResidents}
                    loading={isLoading}
                    serialStart={(page - 1) * pageSize + 1}
                    tableId="residents-list"
                    title={
                        <div className="flex items-center gap-2">
                            <Users size={18} className="text-primary" />
                            <span>住户基本信息档案库</span>
                            <span className="text-xs font-normal text-secondary ml-2">
                                显示第 {(page - 1) * pageSize + 1} - {Math.min(page * pageSize, totalRecords)} 条，共 {totalRecords} 条
                            </span>
                        </div>
                    }
                />

                <div className="pagination-footer">
                    <div className="pagination-info">
                        共 {totalRecords} 条记录
                    </div>

                    <div className="pagination-controls">
                        <select
                            className="page-select"
                            value={pageSize}
                            onChange={(e) => {
                                setPageSize(Number(e.target.value));
                                setPage(1);
                            }}
                        >
                            <option value={10}>10 条/页</option>
                            <option value={25}>25 条/页</option>
                            <option value={50}>50 条/页</option>
                            <option value={100}>100 条/页</option>
                        </select>

                        <div className="flex gap-1 ml-2">
                            <button className="page-btn" disabled={page === 1} onClick={() => setPage(1)}>
                                <ChevronsLeft size={16} />
                            </button>
                            <button className="page-btn" disabled={page === 1} onClick={() => setPage(p => Math.max(1, p - 1))}>
                                <ChevronLeft size={16} />
                            </button>

                            <button className="page-btn active">{page}</button>
                            {page < Math.ceil(totalRecords / pageSize) && (
                                <button className="page-btn" onClick={() => setPage(p => p + 1)}>{page + 1}</button>
                            )}
                            {page + 1 < Math.ceil(totalRecords / pageSize) && <span className="px-2 text-secondary">...</span>}
                            {page + 1 < Math.ceil(totalRecords / pageSize) && (
                                <button className="page-btn" onClick={() => setPage(Math.ceil(totalRecords / pageSize))}>
                                    {Math.ceil(totalRecords / pageSize)}
                                </button>
                            )}

                            <button
                                className="page-btn"
                                disabled={page === Math.ceil(totalRecords / pageSize)}
                                onClick={() => setPage(p => Math.min(Math.ceil(totalRecords / pageSize), p + 1))}
                            >
                                <ChevronRight size={16} />
                            </button>
                            <button
                                className="page-btn"
                                disabled={page === Math.ceil(totalRecords / pageSize)}
                                onClick={() => setPage(Math.ceil(totalRecords / pageSize))}
                            >
                                <ChevronsRight size={16} />
                            </button>
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

            {/* Edit Modal */}
            {editingResident && (
                <div
                    className="kd-house-mapping-overlay"
                    role="dialog"
                    aria-modal="true"
                    onMouseDown={(e) => {
                        if (e.target === e.currentTarget) setEditingResident(null);
                    }}
                >
                    <div className="kd-house-mapping-modal kd-house-edit-modal">
                        <div className="kd-house-mapping-header">
                            <div>
                                <div className="kd-house-mapping-title">
                                    <AlertCircle size={18} className="text-primary" />
                                    <h3>配置住户财务映射</h3>
                                </div>
                                <div className="kd-house-mapping-subtitle">
                                    选择对应的金蝶客户；保存后用于费用对账/推送的档案关联。
                                </div>
                            </div>
                            <button className="modal-close-btn kd-house-mapping-close" onClick={() => setEditingResident(null)} type="button" aria-label="关闭">
                                <X size={20} />
                            </button>
                        </div>

                        <div className="kd-house-mapping-body">
                            <div className="kd-house-mapping-housecard">
                                <span className="kd-house-mapping-housecard-label">当前住户</span>
                                <div className="kd-house-mapping-housecard-name">{editingResident.name}</div>
                                <div className="kd-house-mapping-housecard-meta flex flex-col gap-1">
                                    <span>所属园区：{editingResident.community_name || '-'}</span>
                                    <span>联系电话：{editingResident.phone || '-'}</span>
                                    <span title={getHousesText(editingResident.houses)}>关联房屋：{getHousesText(editingResident.houses)}</span>
                                </div>
                            </div>

                            <div className="flex flex-col gap-4">
                                <KingdeeCustomerSelector
                                    label="财务系统客户映射"
                                    placeholder="搜索或手动输入金蝶客户编码/名称..."
                                    value={
                                        editingResident.kingdee_customer
                                            ? `${editingResident.kingdee_customer.number} ${editingResident.kingdee_customer.name}`
                                            : editingResident.kingdee_customer_id
                                    }
                                    defaultSearch={editingResident.name || editingResident.phone || ""}
                                    autoOpen={true}
                                    onSelect={(customer) => {
                                        setEditingResident({
                                            ...editingResident,
                                            kingdee_customer_id: customer?.id || undefined,
                                            kingdee_customer: customer || undefined
                                        });
                                    }}
                                />
                                <div className="kd-house-mapping-hint">
                                    提示：可直接搜索 <code>客户编码</code> / <code>客户名称</code>；也可清除映射表示“暂不关联”。
                                </div>
                            </div>
                        </div>

                        <div className="kd-house-mapping-footer">
                            <button className="btn btn-outline" onClick={() => setEditingResident(null)} type="button">
                                取消
                            </button>
                            <button className="btn btn-primary" onClick={handleSaveResident} disabled={isSaving} type="button">
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

export default Residents;
