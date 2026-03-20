import { useState, useEffect, useCallback } from 'react';
import {
    RefreshCw,
    FileText,
    Pencil,
    AlertCircle,
    Save,
    X,
    ChevronsLeft,
    ChevronLeft,
    ChevronRight,
    ChevronsRight
} from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import type { Project } from '../../types';
import { getProjects, syncProjects, updateProject } from '../../services/api';
import KingdeeProjectSelector from '../../components/finance/KingdeeProjectSelector';
import BankAccountSelector from '../../components/finance/BankAccountSelector';
import KingdeeAccountBookSelector from '../../components/finance/KingdeeAccountBookSelector';
import '../bills/Bills.css'; // Reuse styles
import '../houses/Houses.css';

const Projects = () => {
    const [projects, setProjects] = useState<Project[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [isSyncing, setIsSyncing] = useState(false);

    // Pagination state
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);
    const [totalRecords, setTotalRecords] = useState(0);

    // Editing mapping
    const [editingProject, setEditingProject] = useState<Project | null>(null);
    const [isSaving, setIsSaving] = useState(false);

    const fetchProjects = useCallback(async () => {
        setIsLoading(true);
        try {
            const data = await getProjects({
                skip: (page - 1) * pageSize,
                limit: pageSize
            });
            // Handle both array (old) and object (new paginated) response
            if (data && data.items) {
                setProjects(data.items);
                setTotalRecords(data.total);
            } else {
                setProjects(Array.isArray(data) ? data : []);
                setTotalRecords(Array.isArray(data) ? data.length : 0);
            }
        } catch (error) {
            console.error('Failed to fetch projects:', error);
        } finally {
            setIsLoading(false);
        }
    }, [page, pageSize]);

    useEffect(() => {
        fetchProjects();
    }, [fetchProjects]);

    useEffect(() => {
        if (!editingProject) return;

        const prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setEditingProject(null);
        };

        document.addEventListener('keydown', handleKeyDown);
        return () => {
            document.body.style.overflow = prevOverflow;
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [editingProject]);

    const handleSync = async () => {
        setIsSyncing(true);
        try {
            await syncProjects();
            await fetchProjects();
        } catch (error) {
            console.error('Sync failed:', error);
        } finally {
            setIsSyncing(false);
        }
    };

    const handleSaveEdit = async () => {
        if (!editingProject) return;
        setIsSaving(true);
        try {
            await updateProject(editingProject.proj_id, {
                kingdee_project_id: editingProject.kingdee_project_id,
                default_receive_bank_id: editingProject.default_receive_bank_id ?? '',
                default_pay_bank_id: editingProject.default_pay_bank_id ?? '',
                kingdee_account_book_id: editingProject.kingdee_account_book_id ?? '',
            });
            setEditingProject(null);
            fetchProjects();
        } catch (error) {
            console.error('Failed to update project mapping:', error);
        } finally {
            setIsSaving(false);
        }
    };

    const columns = [
        { key: 'proj_id' as keyof Project, title: '项目ID', width: 120 },
        { key: 'proj_name' as keyof Project, title: '园区名称' },
        {
            key: 'kingdee_project' as keyof Project,
            title: '财务系统管理项目映射',
            width: 250,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">内码: {val.number}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'kingdee_account_book' as keyof Project,
            title: '金蝶核算账簿映射',
            width: 200,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">内码: {val.number}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'default_receive_bank' as keyof Project,
            title: '默认收款账户',
            width: 200,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">{val.bankaccountnumber}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'default_pay_bank' as keyof Project,
            title: '默认付款账户',
            width: 200,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">{val.bankaccountnumber}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'created_at' as keyof Project,
            title: '创建时间',
            width: 120,
            render: (val: any) => val ? new Date(val).toLocaleDateString() : '-'
        },
        {
            key: 'actions' as keyof Project,
            title: '操作',
            width: 100,
            render: (_: any, item: Project) => (
                <div className="flex gap-2">
                    <button className="icon-action hover:text-primary" onClick={() => setEditingProject(item)} title="配置映射"><Pencil size={16} /></button>
                    <button className="icon-action"><FileText size={16} /></button>
                </div>
            )
        }
    ];

    const totalPages = Math.ceil(totalRecords / pageSize);

    return (
        <div className="page-container fade-in">
            <div className="flex justify-end gap-3 mb-4">
                <div className="header-actions">
                    <button
                        className={`btn ${isSyncing ? 'btn-disabled' : 'btn-primary'}`}
                        onClick={handleSync}
                        disabled={isSyncing}
                    >
                        <RefreshCw size={16} className={isSyncing ? 'animate-spin' : ''} />
                        {isSyncing ? '同步中...' : '同步园区数据'}
                    </button>
                    <button className="btn btn-outline" onClick={fetchProjects}>
                        <RefreshCw size={16} /> 刷新列表
                    </button>
                </div>
            </div>

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns}
                    data={projects}
                    loading={isLoading}
                    serialStart={(page - 1) * pageSize + 1}
                    tableId="projects-list"
                    title={
                        <div className="flex items-center gap-2">
                            <span>园区档案列表</span>
                            {totalRecords > 0 && (
                                <span className="text-xs font-normal text-slate-400 ml-2">
                                    显示第 {(page - 1) * pageSize + 1} - {Math.min(page * pageSize, totalRecords)} 条，共 {totalRecords} 条
                                </span>
                            )}
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
                            {page < totalPages && (
                                <button className="page-btn" onClick={() => setPage(p => p + 1)}>{page + 1}</button>
                            )}
                            {page + 1 < totalPages && <span className="px-2 text-slate-400">...</span>}
                            {page + 1 < totalPages && (
                                <button className="page-btn" onClick={() => setPage(totalPages)}>
                                    {totalPages}
                                </button>
                            )}

                            <button
                                className="page-btn"
                                disabled={page === totalPages || totalPages === 0}
                                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                            >
                                <ChevronRight size={16} />
                            </button>
                            <button
                                className="page-btn"
                                disabled={page === totalPages || totalPages === 0}
                                onClick={() => setPage(totalPages)}
                            >
                                <ChevronsRight size={16} />
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            {/* Edit Modal */}
            {editingProject && (
                <div
                    className="kd-house-mapping-overlay"
                    role="dialog"
                    aria-modal="true"
                    onMouseDown={(e) => {
                        if (e.target === e.currentTarget) setEditingProject(null);
                    }}
                >
                    <div className="kd-house-mapping-modal">
                        <div className="kd-house-mapping-header">
                            <div>
                                <div className="kd-house-mapping-title">
                                    <AlertCircle size={18} className="text-primary" />
                                    <h3>配置金蝶系统园区映射</h3>
                                </div>
                                <div className="kd-house-mapping-subtitle">为园区设置金蝶管理项目、核算账簿及默认银行账户。</div>
                            </div>
                            <button className="kd-house-mapping-close" onClick={() => setEditingProject(null)} type="button" aria-label="关闭">
                                <X size={20} />
                            </button>
                        </div>

                        <div className="kd-house-mapping-body">
                            <div className="kd-house-mapping-housecard">
                                <span className="kd-house-mapping-housecard-label">当前园区</span>
                                <div className="kd-house-mapping-housecard-name">{editingProject.proj_name}</div>
                                <div className="kd-house-mapping-housecard-meta">ID: {editingProject.proj_id}</div>
                            </div>

                            <div className="flex flex-col gap-4">
                                <KingdeeProjectSelector
                                    label="财务系统管理项目映射"
                                    placeholder="搜索或手动输入金蝶管理项目..."
                                    value={editingProject.kingdee_project ? `${editingProject.kingdee_project.number} ${editingProject.kingdee_project.name}` : editingProject.kingdee_project_id}
                                    onSelect={(project) => {
                                        setEditingProject({
                                            ...editingProject,
                                            kingdee_project_id: project?.id || undefined,
                                            kingdee_project: project || undefined
                                        });
                                    }}
                                />

                                <KingdeeAccountBookSelector
                                    label="金蝶系统核算账簿映射"
                                    placeholder="搜索或手动选择对应的核算账簿..."
                                    value={editingProject.kingdee_account_book ? `${editingProject.kingdee_account_book.number} ${editingProject.kingdee_account_book.name}` : editingProject.kingdee_account_book_id}
                                    onSelect={(book) => {
                                        setEditingProject({
                                            ...editingProject,
                                            kingdee_account_book_id: book?.id || undefined,
                                            kingdee_account_book: book || undefined
                                        });
                                    }}
                                />

                                <BankAccountSelector
                                    label="默认收款银行账户"
                                    placeholder="点击选择该园区的收款账户..."
                                    value={editingProject.default_receive_bank ? `${editingProject.default_receive_bank.name} (${editingProject.default_receive_bank.bankaccountnumber})` : ''}
                                    onSelect={(account) => {
                                        setEditingProject({
                                            ...editingProject,
                                            default_receive_bank_id: account?.id || undefined,
                                            default_receive_bank: account || undefined
                                        });
                                    }}
                                />

                                <BankAccountSelector
                                    label="默认付款银行账户"
                                    placeholder="点击选择该园区的付款账户..."
                                    value={editingProject.default_pay_bank ? `${editingProject.default_pay_bank.name} (${editingProject.default_pay_bank.bankaccountnumber})` : ''}
                                    onSelect={(account) => {
                                        setEditingProject({
                                            ...editingProject,
                                            default_pay_bank_id: account?.id || undefined,
                                            default_pay_bank: account || undefined
                                        });
                                    }}
                                />
                            </div>

                            <div className="kd-house-mapping-hint">
                                提示：如不设置默认银行账户，推送时可在业务侧选择或回退到系统默认逻辑。
                            </div>
                        </div>

                        <div className="kd-house-mapping-footer">
                            <button className="btn btn-outline" onClick={() => setEditingProject(null)} type="button">
                                取消
                            </button>
                            <button className="btn btn-primary" onClick={handleSaveEdit} disabled={isSaving} type="button">
                                {isSaving ? <RefreshCw className="animate-spin" size={16} /> : <Save size={16} />}
                                保存映射配置
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Projects;
