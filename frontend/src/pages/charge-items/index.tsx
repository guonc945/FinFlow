import { useState, useEffect, useRef } from 'react';
import { RefreshCw, Pencil, CloudSync, X, Save, AlertCircle, Search, ChevronDown } from 'lucide-react';
import { ChevronsLeft, ChevronLeft, ChevronRight, ChevronsRight } from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import { getChargeItems, updateChargeItem, getProjects, syncChargeItems } from '../../services/api';
import type { ChargeItem } from '../../types';
import AccountingSubjectSelector from '../../components/finance/AccountingSubjectSelector';
import TaxRateSelector from '../../components/finance/TaxRateSelector';
import { useToast, ToastContainer } from '../../components/Toast';
import '../bills/Bills.css';
import '../houses/Houses.css';
import './ChargeItems.css';

const ChargeItems = () => {
    const [items, setItems] = useState<ChargeItem[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [isSyncing, setIsSyncing] = useState(false);
    const [editingItem, setEditingItem] = useState<ChargeItem | null>(null);
    const [isSaving, setIsSaving] = useState(false);
    const { toasts, showToast, removeToast } = useToast();

    // Pagination state
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(25);

    const [projects, setProjects] = useState<any[]>([]);
    const [selectedProjectIds, setSelectedProjectIds] = useState<string[]>([]);
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [projectSearch, setProjectSearch] = useState('');
    const dropdownRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        fetchItems();
        fetchProjects();

        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsDropdownOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, []);

    useEffect(() => {
        if (!editingItem) return;

        const prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setEditingItem(null);
        };

        document.addEventListener('keydown', handleKeyDown);
        return () => {
            document.body.style.overflow = prevOverflow;
            document.removeEventListener('keydown', handleKeyDown);
        };
    }, [editingItem]);

    const fetchProjects = async () => {
        try {
            const data = await getProjects();
            setProjects(data.items || data);
        } catch (error) {
            console.error('Failed to load projects:', error);
        }
    };

    const fetchItems = async () => {
        setIsLoading(true);
        try {
            const data = await getChargeItems();
            setItems(data);
            setPage(1);
        } catch (error) {
            console.error('Failed to fetch items:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSync = async () => {
        if (selectedProjectIds.length === 0) {
            showToast('info', '请至少选择一个园区进行同步');
            return;
        }

        setIsSyncing(true);
        try {
            await syncChargeItems(selectedProjectIds.map(id => parseInt(id, 10)));
            await fetchItems();
            showToast('success', '收费项目同步完成');
        } catch (error) {
            console.error('Sync failed:', error);
            showToast('error', '同步失败，请重试');
        } finally {
            setIsSyncing(false);
        }
    };

    const handleEditItem = (item: ChargeItem) => {
        setEditingItem({ ...item });
    };

    const handleSaveEdit = async () => {
        if (!editingItem) return;
        setIsSaving(true);
        try {
            await updateChargeItem(editingItem.item_id, {
                current_account_subject_id: editingItem.current_account_subject_id,
                profit_loss_subject_id: editingItem.profit_loss_subject_id,
                kingdee_tax_rate_id: editingItem.kingdee_tax_rate_id,
            });
            setEditingItem(null);
            await fetchItems();
        } catch (error) {
            console.error('Failed to update item:', error);
        } finally {
            setIsSaving(false);
        }
    };

    const columns = [
        { key: 'item_id' as keyof ChargeItem, title: 'ID', width: 80 },
        {
            key: 'communityid' as keyof ChargeItem,
            title: '园区名称',
            width: 160,
            render: (val: any) => {
                const project = projects.find((p) => String(p.proj_id) === String(val));
                return project?.proj_name || '-';
            }
        },
        { key: 'category_name' as keyof ChargeItem, title: '分类名称', width: 150 },
        { key: 'item_name' as keyof ChargeItem, title: '收费项名称', width: 220 },
        { key: 'charge_type_str' as keyof ChargeItem, title: '收费类型', width: 120 },
        { key: 'period_type_str' as keyof ChargeItem, title: '周期规则', width: 180 },
        {
            key: 'current_account_subject' as keyof ChargeItem,
            title: '往来科目映射',
            width: 250,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">{val.number}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'profit_loss_subject' as keyof ChargeItem,
            title: '损益科目映射',
            width: 250,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">{val.number}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'kingdee_tax_rate' as keyof ChargeItem,
            title: '金蝶税率档案',
            width: 220,
            render: (val: any) => val ? (
                <div className="flex flex-col">
                    <span className="text-sm font-medium">{val.name}</span>
                    <span className="text-xs text-slate-400 font-mono">{val.number}</span>
                </div>
            ) : <span className="text-slate-300">未设置</span>
        },
        {
            key: 'created_at' as keyof ChargeItem,
            title: '同步时间',
            width: 120,
            render: (val: any) => new Date(val).toLocaleDateString()
        },

        {
            key: 'actions' as keyof ChargeItem,
            title: '操作',
            width: 80,
            render: (_: any, item: ChargeItem) => (
                <div className="flex gap-2">
                    <button className="icon-action" onClick={() => handleEditItem(item)} title="设置映射">
                        <Pencil size={16} />
                    </button>
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

    const totalRecords = items.length;
    const totalPages = Math.max(1, Math.ceil(totalRecords / pageSize));
    const startIndex = (page - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, totalRecords);
    const pagedItems = items.slice(startIndex, startIndex + pageSize);

    useEffect(() => {
        if (page > totalPages) setPage(totalPages);
    }, [page, totalPages]);

    return (
        <div className="page-container fade-in">
            <div className="bills-filter-section mb-6">
                <div className="filter-content-wrapper fade-in">
                    <div className="selection-row">
                        <div className="selection-group" ref={dropdownRef}>
                            <div className={`custom-select-trigger ${isDropdownOpen ? 'active' : ''}`} onClick={() => setIsDropdownOpen(!isDropdownOpen)}>
                                <div className="trigger-content">
                                    <CloudSync size={14} />
                                    <span className={selectedProjectIds.length === 0 ? 'placeholder' : ''}>
                                        {selectedProjectIds.length === 0 ? '选择同步园区...' : `已选 ${selectedProjectIds.length}`}
                                    </span>
                                </div>
                                <ChevronDown size={14} className={`arrow ${isDropdownOpen ? 'rotate' : ''}`} />
                            </div>
                            {isDropdownOpen && (
                                <div className="custom-dropdown card-shadow slide-up">
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
                            {selectedProjectIds.slice(0, 5).map(id => (
                                <div key={id} className="selected-chip">
                                    <span>{projects.find(p => p.proj_id === id)?.proj_name || id}</span>
                                    <button onClick={() => toggleProject(id)}><X size={10} /></button>
                                </div>
                            ))}
                            {selectedProjectIds.length > 5 && <span className="text-xs text-secondary">+{selectedProjectIds.length - 5}</span>}
                        </div>
                        <button className={`btn-primary ${selectedProjectIds.length === 0 || isSyncing ? 'disabled' : ''}`} onClick={handleSync} disabled={selectedProjectIds.length === 0 || isSyncing}>
                            <RefreshCw size={14} className={isSyncing ? 'animate-spin' : ''} /> {isSyncing ? '同步中...' : '开始同步'}
                        </button>

                        <div className="ml-auto">
                            <button className="btn btn-outline" onClick={fetchItems}>
                                <RefreshCw size={16} /> 刷新
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            <div className="table-area-wrapper">
                <DataTable
                    columns={columns}
                    data={pagedItems}
                    loading={isLoading}
                    serialStart={startIndex + 1}
                    tableId="charge-items-list"
                    title={
                        <div className="flex items-center justify-between w-full">
                            <span>收费项目列表</span>
                            <span className="text-xs font-normal text-secondary">
                                {totalRecords === 0 ? '暂无数据' : `显示第 ${startIndex + 1} - ${endIndex} 条，共 ${totalRecords} 条`}
                            </span>
                        </div>
                    }
                />

                <div className="pagination-footer">
                    <div className="pagination-info">共 {totalRecords} 条记录</div>

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
                            <button className="page-btn" disabled={page === 1} onClick={() => setPage((p) => Math.max(1, p - 1))}>
                                <ChevronLeft size={16} />
                            </button>

                            <button className="page-btn active">{page}</button>
                            {page < totalPages && (
                                <button className="page-btn" onClick={() => setPage((p) => p + 1)}>
                                    {page + 1}
                                </button>
                            )}
                            {page + 1 < totalPages && <span className="px-2 text-secondary">...</span>}
                            {page + 1 < totalPages && (
                                <button className="page-btn" onClick={() => setPage(totalPages)}>
                                    {totalPages}
                                </button>
                            )}

                            <button className="page-btn" disabled={page === totalPages} onClick={() => setPage((p) => Math.min(totalPages, p + 1))}>
                                <ChevronRight size={16} />
                            </button>
                            <button className="page-btn" disabled={page === totalPages} onClick={() => setPage(totalPages)}>
                                <ChevronsRight size={16} />
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            {/* Edit Modal */}
            {editingItem && (
                <div
                    className="kd-house-mapping-overlay"
                    role="dialog"
                    aria-modal="true"
                    onMouseDown={(e) => {
                        if (e.target === e.currentTarget) setEditingItem(null);
                    }}
                >
                    <div className="kd-house-mapping-modal">
                        <div className="kd-house-mapping-header">
                            <div>
                                <div className="kd-house-mapping-title">
                                    <AlertCircle size={18} className="text-primary" />
                                    <h3>配置会计核算映射</h3>
                                </div>
                                <div className="kd-house-mapping-subtitle">为收费项设置往来科目、损益科目和税率档案映射，用于后续自动制证。</div>
                            </div>
                            <button className="kd-house-mapping-close" onClick={() => setEditingItem(null)} type="button" aria-label="关闭">
                                <X size={20} />
                            </button>
                        </div>

                        <div className="kd-house-mapping-body">
                            <div className="kd-house-mapping-housecard">
                                <span className="kd-house-mapping-housecard-label">当前收费项</span>
                                <div className="kd-house-mapping-housecard-name">{editingItem.item_name}</div>
                                {editingItem.item_id && (
                                    <div className="kd-house-mapping-housecard-meta">ID: {editingItem.item_id}</div>
                                )}
                            </div>

                            <div className="flex flex-col gap-4">
                                <AccountingSubjectSelector
                                    label="往来科目映射"
                                    placeholder="搜索或手动输入往来科目..."
                                    value={editingItem.current_account_subject ? `${editingItem.current_account_subject.number} ${editingItem.current_account_subject.name}` : editingItem.current_account_subject_id}
                                    onSelect={(subject) => {
                                        setEditingItem({
                                            ...editingItem,
                                            current_account_subject_id: subject?.id || undefined,
                                            current_account_subject: subject || undefined
                                        });
                                    }}
                                />

                                <AccountingSubjectSelector
                                    label="损益科目映射"
                                    placeholder="搜索或手动输入损益科目..."
                                    value={editingItem.profit_loss_subject ? `${editingItem.profit_loss_subject.number} ${editingItem.profit_loss_subject.name}` : editingItem.profit_loss_subject_id}
                                    onSelect={(subject) => {
                                        setEditingItem({
                                            ...editingItem,
                                            profit_loss_subject_id: subject?.id || undefined,
                                            profit_loss_subject: subject || undefined
                                        });
                                    }}
                                />

                                <TaxRateSelector
                                    label="金蝶税率档案映射"
                                    placeholder="搜索或选择金蝶税率档案..."
                                    value={editingItem.kingdee_tax_rate ? `${editingItem.kingdee_tax_rate.number} ${editingItem.kingdee_tax_rate.name}` : editingItem.kingdee_tax_rate_id}
                                    onSelect={(taxRate) => {
                                        setEditingItem({
                                            ...editingItem,
                                            kingdee_tax_rate_id: taxRate?.id || undefined,
                                            kingdee_tax_rate: taxRate || undefined
                                        });
                                    }}
                                />
                            </div>

                            <div className="kd-house-mapping-hint">
                                {'提示：这里配置的金蝶税率档案，会在凭证模板字段选择中的“金蝶税率编码 / 金蝶税率名称”里按“运营账单.收费项目 -> 收费项目 -> 金蝶税率档案”自动解析。'}
                            </div>
                        </div>

                        <div className="kd-house-mapping-footer">
                            <button className="btn btn-outline" onClick={() => setEditingItem(null)} type="button">
                                取消
                            </button>
                            <button className="btn btn-primary" onClick={handleSaveEdit} disabled={isSaving} type="button">
                                {isSaving ? <Loader2 className="animate-spin" size={16} /> : <Save size={16} />}
                                保存映射配置
                            </button>
                        </div>
                    </div>
                </div>
            )}

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
};

// Simple loader helper
const Loader2 = ({ className, size }: { className?: string, size?: number }) => (
    <RefreshCw size={size || 16} className={`animate-spin ${className}`} />
);

export default ChargeItems;





