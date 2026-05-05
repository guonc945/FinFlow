import { useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import {
    ChevronRight,
    Edit2,
    GitBranch,
    List,
    Plus,
    Save,
    Search,
    SquareStack,
    Trash2,
    X,
} from 'lucide-react';
import {
    createBusinessDictionary,
    createBusinessDictionaryItem,
    deleteBusinessDictionary,
    deleteBusinessDictionaryItem,
    getBusinessDictionaries,
    getBusinessDictionaryItems,
    updateBusinessDictionary,
    updateBusinessDictionaryItem,
} from '../../services/api';

type DictionaryType = 'enum' | 'hierarchy';

type BusinessDictionary = {
    id: number;
    key: string;
    name: string;
    dict_type: DictionaryType;
    category?: string | null;
    description?: string | null;
    is_active: boolean;
    item_count: number;
};

type BusinessDictionaryItem = {
    id: number;
    dictionary_id: number;
    code: string;
    label: string;
    value?: string | null;
    parent_id?: number | null;
    sort_order?: number;
    status?: number;
    description?: string | null;
    extra_json?: string | null;
    level: number;
    path?: string | null;
};

type DictionaryFormState = {
    id?: number;
    key: string;
    name: string;
    dict_type: DictionaryType;
    category: string;
    description: string;
    is_active: boolean;
};

type DraftItem = {
    client_id: string;
    id?: number;
    code: string;
    label: string;
    value: string;
    parent_ref: string;
    sort_order: string;
    status: string;
    description: string;
    extra_json: string;
};

type DraftTreeItem = DraftItem & {
    children: DraftTreeItem[];
};

type ApiErrorDetail = {
    errors?: unknown;
    message?: unknown;
};

type ApiErrorLike = {
    response?: {
        data?: {
            detail?: string | ApiErrorDetail;
        };
    };
    message?: unknown;
};

const emptyDictionaryForm = (): DictionaryFormState => ({
    key: '',
    name: '',
    dict_type: 'enum',
    category: 'common',
    description: '',
    is_active: true,
});

const createClientId = () => `draft-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const createEmptyDraftItem = (): DraftItem => ({
    client_id: createClientId(),
    code: '',
    label: '',
    value: '',
    parent_ref: '',
    sort_order: '0',
    status: '1',
    description: '',
    extra_json: '',
});

const toDraftItem = (item: BusinessDictionaryItem): DraftItem => ({
    client_id: `item-${item.id}`,
    id: item.id,
    code: item.code,
    label: item.label,
    value: item.value || '',
    parent_ref: item.parent_id ? `item-${item.parent_id}` : '',
    sort_order: String(item.sort_order ?? 0),
    status: String(item.status ?? 1),
    description: item.description || '',
    extra_json: item.extra_json || '',
});

const getErrorMessage = (error: unknown) => {
    const detail = (error as ApiErrorLike)?.response?.data?.detail;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail?.errors) && detail.errors.length) return detail.errors.join('\n');
    return (typeof detail?.message === 'string' ? detail.message : undefined)
        || (typeof (error as ApiErrorLike)?.message === 'string' ? (error as ApiErrorLike).message : undefined)
        || '发生未知错误';
};

const buildIndentedOptions = (items: DraftItem[]) => {
    const itemMap = new Map(items.map((item) => [item.client_id, item]));
    const getLevel = (item: DraftItem) => {
        let level = 0;
        let currentParent = item.parent_ref;
        let guard = 0;
        while (currentParent && itemMap.has(currentParent) && guard < items.length + 1) {
            level += 1;
            currentParent = itemMap.get(currentParent)?.parent_ref || '';
            guard += 1;
        }
        return level;
    };

    return items.map((item) => ({
        id: item.client_id,
        label: `${'　'.repeat(getLevel(item))}${item.label || item.code || '未命名节点'}`,
    }));
};

const buildDraftTree = (items: DraftItem[]): DraftTreeItem[] => {
    const nodeMap = new Map<string, DraftTreeItem>();
    items.forEach((item) => {
        nodeMap.set(item.client_id, { ...item, children: [] });
    });

    const roots: DraftTreeItem[] = [];
    items.forEach((item) => {
        const node = nodeMap.get(item.client_id);
        if (!node) return;
        if (item.parent_ref && nodeMap.has(item.parent_ref)) {
            nodeMap.get(item.parent_ref)?.children.push(node);
        } else {
            roots.push(node);
        }
    });

    return roots;
};

const renderDraftTreeNodes = (
    items: DraftTreeItem[],
    onEdit: (item: DraftItem) => void,
    onDelete: (item: DraftItem) => void
): ReactNode[] =>
    items.map((item) => (
        <div key={item.client_id} className="dictionary-tree-node">
            <div className="dictionary-tree-row">
                <div className="dictionary-tree-main">
                    <span className="dictionary-tree-arrow">
                        {item.children.length ? <ChevronRight size={14} /> : <span className="dictionary-tree-dot" />}
                    </span>
                    <div>
                        <div className="dictionary-tree-title-row">
                            <strong>{item.label || '未命名节点'}</strong>
                            <code>{item.code || '-'}</code>
                            {item.status === '1' ? (
                                <span className="formula-status-badge active">启用</span>
                            ) : (
                                <span className="formula-status-badge inactive">停用</span>
                            )}
                        </div>
                        <div className="dictionary-tree-meta">
                            <span>排序 {item.sort_order || '0'}</span>
                            {item.value ? <span>值 {item.value}</span> : null}
                        </div>
                        {item.description ? <p className="dictionary-tree-desc">{item.description}</p> : null}
                    </div>
                </div>
                <div className="var-actions">
                    <button type="button" className="action-btn-pro hover:bg-slate-100" onClick={() => onEdit(item)}>
                        <Edit2 size={14} />
                    </button>
                    <button type="button" className="action-btn-pro text-red-400 hover:bg-red-50" onClick={() => onDelete(item)}>
                        <Trash2 size={14} />
                    </button>
                </div>
            </div>
            {item.children.length ? <div className="dictionary-tree-children">{renderDraftTreeNodes(item.children, onEdit, onDelete)}</div> : null}
        </div>
    ));

export default function BusinessDictionaryManager() {
    const [dictionaries, setDictionaries] = useState<BusinessDictionary[]>([]);
    const [searchQuery, setSearchQuery] = useState('');
    const [dictTypeFilter, setDictTypeFilter] = useState<'all' | DictionaryType>('all');
    const [isLoading, setIsLoading] = useState(false);
    const [isEditorOpen, setIsEditorOpen] = useState(false);
    const [isSaving, setIsSaving] = useState(false);
    const [dictionaryForm, setDictionaryForm] = useState<DictionaryFormState>(emptyDictionaryForm());
    const [draftItems, setDraftItems] = useState<DraftItem[]>([]);
    const [originalItems, setOriginalItems] = useState<BusinessDictionaryItem[]>([]);
    const [itemEditor, setItemEditor] = useState<DraftItem>(createEmptyDraftItem());
    const [editingItemClientId, setEditingItemClientId] = useState<string | null>(null);

    const filteredDictionaries = useMemo(() => {
        const keyword = searchQuery.trim().toLowerCase();
        return dictionaries.filter((item) => {
            if (dictTypeFilter !== 'all' && item.dict_type !== dictTypeFilter) return false;
            if (!keyword) return true;
            return [item.key, item.name, item.category, item.description]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(keyword));
        });
    }, [dictionaries, dictTypeFilter, searchQuery]);

    const draftTree = useMemo(() => buildDraftTree(draftItems), [draftItems]);

    const parentOptions = useMemo(() => {
        if (dictionaryForm.dict_type !== 'hierarchy') return [];
        return buildIndentedOptions(draftItems.filter((item) => item.client_id !== editingItemClientId));
    }, [dictionaryForm.dict_type, draftItems, editingItemClientId]);

    const loadDictionaries = async () => {
        setIsLoading(true);
        try {
            const result = await getBusinessDictionaries();
            setDictionaries(Array.isArray(result) ? result : []);
        } catch (error) {
            alert(`加载业务字典失败: ${getErrorMessage(error)}`);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        void loadDictionaries();
    }, []);

    const resetItemEditor = () => {
        setEditingItemClientId(null);
        setItemEditor(createEmptyDraftItem());
    };

    const openCreateDictionary = () => {
        setDictionaryForm(emptyDictionaryForm());
        setOriginalItems([]);
        setDraftItems([]);
        resetItemEditor();
        setIsEditorOpen(true);
    };

    const openEditDictionary = async (dictionary: BusinessDictionary) => {
        setIsLoading(true);
        try {
            const items = await getBusinessDictionaryItems(dictionary.id, false);
            const nextItems = Array.isArray(items) ? items : [];
            setDictionaryForm({
                id: dictionary.id,
                key: dictionary.key,
                name: dictionary.name,
                dict_type: dictionary.dict_type,
                category: dictionary.category || 'common',
                description: dictionary.description || '',
                is_active: dictionary.is_active,
            });
            setOriginalItems(nextItems);
            setDraftItems(nextItems.map(toDraftItem));
            resetItemEditor();
            setIsEditorOpen(true);
        } catch (error) {
            alert(`加载字典配置失败: ${getErrorMessage(error)}`);
        } finally {
            setIsLoading(false);
        }
    };

    const handleDeleteDictionary = async (dictionary: BusinessDictionary) => {
        if (!window.confirm(`确定删除业务字典“${dictionary.name}”吗？其下字典项会一并删除。`)) return;
        try {
            await deleteBusinessDictionary(dictionary.id);
            await loadDictionaries();
        } catch (error) {
            alert(`删除业务字典失败: ${getErrorMessage(error)}`);
        }
    };

    const setDictionaryType = (nextType: DictionaryType) => {
        setDictionaryForm((prev) => ({ ...prev, dict_type: nextType }));
        if (nextType === 'enum') {
            setDraftItems((prev) => prev.map((item) => ({ ...item, parent_ref: '' })));
            setItemEditor((prev) => ({ ...prev, parent_ref: '' }));
        }
    };

    const openEditDraftItem = (item: DraftItem) => {
        setEditingItemClientId(item.client_id);
        setItemEditor({ ...item });
    };

    const handleRemoveDraftItem = (item: DraftItem) => {
        const nextItems = draftItems.filter((entry) => entry.client_id !== item.client_id);
        const clearedParentItems = nextItems.map((entry) =>
            entry.parent_ref === item.client_id ? { ...entry, parent_ref: '' } : entry
        );
        setDraftItems(clearedParentItems);
        if (editingItemClientId === item.client_id) {
            resetItemEditor();
        }
    };

    const handleSaveDraftItem = () => {
        if (!itemEditor.code.trim()) {
            alert('请填写字典键');
            return;
        }
        if (!itemEditor.label.trim()) {
            alert('请填写显示名称');
            return;
        }

        if (editingItemClientId) {
            setDraftItems((prev) =>
                prev.map((item) =>
                    item.client_id === editingItemClientId
                        ? { ...itemEditor, client_id: editingItemClientId }
                        : item
                )
            );
        } else {
            setDraftItems((prev) => [...prev, itemEditor]);
        }
        resetItemEditor();
    };

    const syncDictionaryItems = async (dictionaryId: number, dictType: DictionaryType) => {
        const originalIdSet = new Set(originalItems.map((item) => item.id));
        const currentIdSet = new Set(draftItems.map((item) => item.id).filter((item): item is number => typeof item === 'number'));
        const deletedIds = [...originalIdSet].filter((id) => !currentIdSet.has(id));

        const resolvedIdMap = new Map<string, number>();
        draftItems.forEach((item) => {
            if (item.id) resolvedIdMap.set(item.client_id, item.id);
        });

        const pendingItems = draftItems.map((item) => ({ ...item }));
        let guard = 0;
        while (pendingItems.length) {
            let progressed = false;
            for (let index = 0; index < pendingItems.length; index += 1) {
                const item = pendingItems[index];
                const parentId =
                    dictType === 'hierarchy' && item.parent_ref
                        ? resolvedIdMap.get(item.parent_ref)
                        : null;

                if (dictType === 'hierarchy' && item.parent_ref && !parentId) {
                    continue;
                }

                const payload = {
                    code: item.code.trim(),
                    label: item.label.trim(),
                    value: item.value.trim() || null,
                    parent_id: dictType === 'hierarchy' ? parentId || null : null,
                    sort_order: Number(item.sort_order || 0),
                    status: Number(item.status || 1),
                    description: item.description.trim() || null,
                    extra_json: item.extra_json.trim() || null,
                };

                if (item.id) {
                    await updateBusinessDictionaryItem(item.id, payload);
                    resolvedIdMap.set(item.client_id, item.id);
                } else {
                    const created = await createBusinessDictionaryItem(dictionaryId, payload);
                    if (created?.id) {
                        resolvedIdMap.set(item.client_id, created.id);
                    }
                }

                pendingItems.splice(index, 1);
                progressed = true;
                index -= 1;
            }

            guard += 1;
            if (!progressed || guard > draftItems.length + 2) {
                throw new Error('层级字典存在无法解析的父子关系，请检查父级配置。');
            }
        }

        for (const id of deletedIds) {
            await deleteBusinessDictionaryItem(id);
        }
    };

    const handleSaveDictionary = async () => {
        if (!dictionaryForm.key.trim()) {
            alert('请填写字典标识');
            return;
        }
        if (!dictionaryForm.name.trim()) {
            alert('请填写字典名称');
            return;
        }

        setIsSaving(true);
        try {
            const payload = {
                key: dictionaryForm.key.trim(),
                name: dictionaryForm.name.trim(),
                dict_type: dictionaryForm.dict_type,
                category: dictionaryForm.category.trim() || 'common',
                description: dictionaryForm.description.trim() || null,
                is_active: dictionaryForm.is_active,
            };

            let dictionaryId = dictionaryForm.id;
            if (dictionaryId) {
                await updateBusinessDictionary(dictionaryId, payload);
            } else {
                const created = await createBusinessDictionary(payload);
                dictionaryId = created?.id;
            }

            if (!dictionaryId) {
                throw new Error('未能获取业务字典 ID');
            }

            await syncDictionaryItems(dictionaryId, dictionaryForm.dict_type);
            setIsEditorOpen(false);
            resetItemEditor();
            await loadDictionaries();
        } catch (error) {
            alert(`保存业务字典失败: ${getErrorMessage(error)}`);
        } finally {
            setIsSaving(false);
        }
    };

    return (
        <>
            <div className="section-toolbar mt-6">
                <div className="settings-toolbar-cluster">
                    <div className="search-box-pro">
                        <Search size={18} className="text-slate-400" />
                        <input value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} placeholder="搜索字典标识、名称、描述..." />
                    </div>
                    <select
                        className="settings-filter-select"
                        value={dictTypeFilter}
                        onChange={(e) => setDictTypeFilter(e.target.value as 'all' | DictionaryType)}
                    >
                        <option value="all">全部类型</option>
                        <option value="enum">键值字典</option>
                        <option value="hierarchy">层级字典</option>
                    </select>
                </div>
                <button type="button" className="btn-primary-clean flex items-center gap-2" onClick={openCreateDictionary}>
                    <Plus size={18} />
                    新增字典
                </button>
            </div>

            <div className="variables-grid settings-dictionary-grid mt-6">
                {filteredDictionaries.map((item) => (
                    <div key={item.id} className="dictionary-card-pro settings-dictionary-card">
                        <div className="var-header">
                            <div className="flex items-center gap-2">
                                <div className="var-icon">{item.dict_type === 'hierarchy' ? <GitBranch size={16} /> : <List size={16} />}</div>
                                <span className="var-key font-mono">{item.key}</span>
                            </div>
                            <div className="var-actions">
                                <button type="button" className="action-btn-pro hover:bg-slate-100" onClick={() => void openEditDictionary(item)}>
                                    <Edit2 size={14} />
                                </button>
                                <button type="button" className="action-btn-pro text-red-400 hover:bg-red-50" onClick={() => void handleDeleteDictionary(item)}>
                                    <Trash2 size={14} />
                                </button>
                            </div>
                        </div>
                        <div className="dictionary-name">{item.name}</div>
                        <div className="dictionary-badge-row">
                            <span className="formula-status-badge type">{item.dict_type === 'hierarchy' ? '层级字典' : '键值字典'}</span>
                            <span className="formula-status-badge muted">
                                <SquareStack size={12} />
                                {item.item_count || 0} 项
                            </span>
                            {item.is_active ? (
                                <span className="formula-status-badge active">启用</span>
                            ) : (
                                <span className="formula-status-badge inactive">停用</span>
                            )}
                        </div>
                        <p className="dictionary-card-desc">{item.description || '暂无描述，可用于说明适用范围和业务语义。'}</p>
                    </div>
                ))}

                {!filteredDictionaries.length ? (
                    <div className="settings-panel-intro" style={{ padding: '12px 0 0', border: 'none', minHeight: 'auto' }}>
                        <div>
                            <p>{isLoading ? '业务字典加载中...' : '当前没有匹配的业务字典，点击右上角新增字典即可。'}</p>
                        </div>
                    </div>
                ) : null}
            </div>

            {isEditorOpen ? (
                <div className="modal-overlay">
                    <div className="modal-content-pro dictionary-modal">
                        <header className="modal-header-clean dictionary-modal-header-sticky">
                            <div>
                                <h3 className="font-bold text-slate-900">{dictionaryForm.id ? '编辑字典' : '新增字典'}</h3>
                                <p className="dictionary-modal-subtitle">在同一个窗口里完成字典基础属性和字典项维护。</p>
                            </div>
                            <div className="dictionary-header-actions">
                                <button className="btn-secondary-clean px-4 text-sm" onClick={() => setIsEditorOpen(false)} disabled={isSaving}>
                                    取消
                                </button>
                                <button className="btn-primary-clean px-6 flex items-center gap-2" onClick={() => void handleSaveDictionary()} disabled={isSaving}>
                                    <Save size={16} />
                                    {isSaving ? '保存中...' : '保存字典'}
                                </button>
                                <button onClick={() => setIsEditorOpen(false)} className="text-slate-400 hover:text-slate-600" disabled={isSaving}>
                                    <X size={20} />
                                </button>
                            </div>
                        </header>

                        <div className="dictionary-modal-body-scroll">
                            <div className="dictionary-layout compact">
                                <div className="dictionary-main">
                                    <div className="dictionary-section-card">
                                        <div className="dictionary-section-head">
                                            <div>
                                                <strong>基础属性</strong>
                                                <p>配置字典标识、名称、类型和基本状态。</p>
                                            </div>
                                        </div>
                                        <div className="form-grid two">
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">字典标识</label>
                                                <input
                                                    className="modern-input-pro font-mono text-sm"
                                                    value={dictionaryForm.key}
                                                    onChange={(e) => setDictionaryForm((prev) => ({ ...prev, key: e.target.value }))}
                                                    disabled={!!dictionaryForm.id}
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">字典名称</label>
                                                <input
                                                    className="modern-input-pro text-sm"
                                                    value={dictionaryForm.name}
                                                    onChange={(e) => setDictionaryForm((prev) => ({ ...prev, name: e.target.value }))}
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">字典类型</label>
                                                <select
                                                    value={dictionaryForm.dict_type}
                                                    onChange={(e) => setDictionaryType(e.target.value as DictionaryType)}
                                                >
                                                    <option value="enum">键值字典</option>
                                                    <option value="hierarchy">层级字典</option>
                                                </select>
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">启用状态</label>
                                                <label className="settings-switch-row">
                                                    <input
                                                        type="checkbox"
                                                        checked={dictionaryForm.is_active}
                                                        onChange={(e) => setDictionaryForm((prev) => ({ ...prev, is_active: e.target.checked }))}
                                                    />
                                                    <em>{dictionaryForm.is_active ? '当前启用' : '当前停用'}</em>
                                                </label>
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">分类</label>
                                                <input
                                                    className="modern-input-pro text-sm"
                                                    value={dictionaryForm.category}
                                                    onChange={(e) => setDictionaryForm((prev) => ({ ...prev, category: e.target.value }))}
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">描述</label>
                                                <input
                                                    className="modern-input-pro text-sm"
                                                    value={dictionaryForm.description}
                                                    onChange={(e) => setDictionaryForm((prev) => ({ ...prev, description: e.target.value }))}
                                                />
                                            </div>
                                        </div>
                                    </div>

                                    <div className="dictionary-section-card">
                                        <div className="dictionary-section-head">
                                            <div>
                                                <strong>{dictionaryForm.dict_type === 'hierarchy' ? '层级项配置' : '键值项配置'}</strong>
                                                <p>新增、编辑和调整字典项都在这里完成，不再拆分成独立窗口。</p>
                                            </div>
                                        </div>
                                        <div className="dictionary-config-grid">
                                            <div className="form-grid two">
                                                <div className="field-container horizontal">
                                                        <label className="modern-label text-xs">字典键</label>
                                                    <input
                                                        className="modern-input-pro font-mono text-sm"
                                                        value={itemEditor.code}
                                                        onChange={(e) => setItemEditor((prev) => ({ ...prev, code: e.target.value }))}
                                                    />
                                                </div>
                                                <div className="field-container horizontal">
                                                        <label className="modern-label text-xs">显示名称</label>
                                                    <input
                                                        className="modern-input-pro text-sm"
                                                        value={itemEditor.label}
                                                        onChange={(e) => setItemEditor((prev) => ({ ...prev, label: e.target.value }))}
                                                    />
                                                </div>
                                                <div className="field-container horizontal">
                                                        <label className="modern-label text-xs">业务值</label>
                                                    <input
                                                        className="modern-input-pro text-sm"
                                                        value={itemEditor.value}
                                                        onChange={(e) => setItemEditor((prev) => ({ ...prev, value: e.target.value }))}
                                                    />
                                                </div>
                                                <div className="field-container horizontal">
                                                    <label className="modern-label text-xs">状态</label>
                                                    <select
                                                        value={itemEditor.status}
                                                        onChange={(e) => setItemEditor((prev) => ({ ...prev, status: e.target.value }))}
                                                    >
                                                        <option value="1">启用</option>
                                                        <option value="0">停用</option>
                                                    </select>
                                                </div>
                                                {dictionaryForm.dict_type === 'hierarchy' ? (
                                                    <div className="field-container horizontal">
                                                        <label className="modern-label text-xs">父级节点</label>
                                                        <select
                                                            value={itemEditor.parent_ref}
                                                            onChange={(e) => setItemEditor((prev) => ({ ...prev, parent_ref: e.target.value }))}
                                                        >
                                                            <option value="">作为根节点</option>
                                                            {parentOptions.map((option) => (
                                                                <option key={option.id} value={option.id}>
                                                                    {option.label}
                                                                </option>
                                                            ))}
                                                        </select>
                                                    </div>
                                                ) : null}
                                                <div className="field-container horizontal">
                                                    <label className="modern-label text-xs">排序</label>
                                                    <input
                                                        className="modern-input-pro text-sm"
                                                        value={itemEditor.sort_order}
                                                        onChange={(e) => setItemEditor((prev) => ({ ...prev, sort_order: e.target.value }))}
                                                    />
                                                </div>
                                            </div>

                                            <div className="field-container">
                                                <label className="modern-label text-xs">描述</label>
                                                <textarea
                                                    className="modern-textarea-pro min-h-[80px]"
                                                    value={itemEditor.description}
                                                    onChange={(e) => setItemEditor((prev) => ({ ...prev, description: e.target.value }))}
                                                />
                                            </div>
                                            <div className="field-container">
                                                <label className="modern-label text-xs">扩展 JSON</label>
                                                <textarea
                                                    className="modern-textarea-pro min-h-[100px] font-mono text-sm"
                                                    value={itemEditor.extra_json}
                                                    onChange={(e) => setItemEditor((prev) => ({ ...prev, extra_json: e.target.value }))}
                                                    placeholder='例如 {"color":"green","tag":"default"}'
                                                />
                                            </div>

                                            <div className="dictionary-section-head">
                                                <div>
                                                    <strong>当前字典项</strong>
                                                    <p>{draftItems.length} 项，保存字典时会一并提交。</p>
                                                </div>
                                                <div className="dictionary-header-actions">
                                                    {editingItemClientId ? (
                                                        <button type="button" className="btn-secondary-clean px-4 text-sm" onClick={resetItemEditor}>
                                                            取消编辑
                                                        </button>
                                                    ) : null}
                                                    <button type="button" className="btn-outline" onClick={handleSaveDraftItem}>
                                                        <Plus size={14} />
                                                        {editingItemClientId ? '更新字典项' : '加入字典项'}
                                                    </button>
                                                </div>
                                            </div>

                                            {dictionaryForm.dict_type === 'hierarchy' ? (
                                                <div className="settings-tree-shell">
                                                    {draftTree.length ? (
                                                        renderDraftTreeNodes(draftTree, openEditDraftItem, handleRemoveDraftItem)
                                                    ) : (
                                                        <div className="empty-box">当前还没有层级节点，可以先新增一个根节点。</div>
                                                    )}
                                                </div>
                                            ) : (
                                                <div className="settings-item-list">
                                                    {draftItems.length ? (
                                                        draftItems.map((item) => (
                                                            <div key={item.client_id} className="settings-item-card">
                                                                <div className="settings-item-main">
                                                                    <div className="dictionary-tree-title-row">
                                                                        <strong>{item.label}</strong>
                                                                        <code>{item.code}</code>
                                                                        {item.status === '1' ? (
                                                                            <span className="formula-status-badge active">启用</span>
                                                                        ) : (
                                                                            <span className="formula-status-badge inactive">停用</span>
                                                                        )}
                                                                    </div>
                                                                    <div className="dictionary-tree-meta">
                                                                        <span>排序 {item.sort_order || '0'}</span>
                                                                        {item.value ? <span>值 {item.value}</span> : null}
                                                                    </div>
                                                                    {item.description ? <p className="dictionary-tree-desc">{item.description}</p> : null}
                                                                </div>
                                                                <div className="var-actions">
                                                                    <button type="button" className="action-btn-pro hover:bg-slate-100" onClick={() => openEditDraftItem(item)}>
                                                                        <Edit2 size={14} />
                                                                    </button>
                                                                    <button type="button" className="action-btn-pro text-red-400 hover:bg-red-50" onClick={() => handleRemoveDraftItem(item)}>
                                                                        <Trash2 size={14} />
                                                                    </button>
                                                                </div>
                                                            </div>
                                                        ))
                                                    ) : (
                                                        <div className="empty-box">当前还没有键值项，可以先录入几项。</div>
                                                    )}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            ) : null}
        </>
    );
}
