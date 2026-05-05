import { useCallback, useEffect, useState } from 'react';
import { ChevronDown, ChevronRight, Edit3, FolderPlus, Plus, RefreshCw, Trash2, X } from 'lucide-react';
import Select from '../../../components/common/Select';
import {
    getReportingReportCategories,
    getReportingReportCategoriesTree,
    createReportingReportCategory,
    updateReportingReportCategory,
    deleteReportingReportCategory,
} from '../../../services/api';
import type { ReportCategory } from '../types';

type CategoryFormData = {
    name: string;
    parent_id: number | null;
    sort_order: number;
    status: number;
    description: string;
};

type ApiErrorLike = {
    response?: {
        data?: {
            detail?: unknown;
        };
    };
};

const emptyForm: CategoryFormData = {
    name: '',
    parent_id: null,
    sort_order: 0,
    status: 1,
    description: '',
};

export default function CategoriesSection() {
    const [categories, setCategories] = useState<ReportCategory[]>([]);
    const [tree, setTree] = useState<ReportCategory[]>([]);
    const [loading, setLoading] = useState(true);
    const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());
    const [editingId, setEditingId] = useState<number | null>(null);
    const [form, setForm] = useState<CategoryFormData>(emptyForm);
    const [showForm, setShowForm] = useState(false);
    const [saving, setSaving] = useState(false);

    const loadData = useCallback(async () => {
        setLoading(true);
        try {
            const [flatData, treeData] = await Promise.all([
                getReportingReportCategories(),
                getReportingReportCategoriesTree(),
            ]);
            setCategories(flatData || []);
            setTree(treeData || []);
            // Auto-expand all
            const allIds = new Set<number>((flatData || []).map((c: ReportCategory) => c.id));
            setExpandedIds(allIds);
        } catch {
            setCategories([]);
            setTree([]);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        void loadData();
    }, [loadData]);

    const toggleExpand = (id: number) => {
        setExpandedIds((prev) => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    const handleAdd = (parentId: number | null = null) => {
        setEditingId(null);
        setForm({ ...emptyForm, parent_id: parentId });
        setShowForm(true);
    };

    const handleEdit = (category: ReportCategory) => {
        setEditingId(category.id);
        setForm({
            name: category.name,
            parent_id: category.parent_id,
            sort_order: category.sort_order || 0,
            status: category.status ?? 1,
            description: category.description || '',
        });
        setShowForm(true);
    };

    const handleSave = async () => {
        if (!form.name.trim()) { alert('分类名称不能为空'); return; }
        setSaving(true);
        try {
            const payload = {
                name: form.name.trim(),
                parent_id: form.parent_id || null,
                sort_order: form.sort_order,
                status: form.status,
                description: form.description.trim() || null,
            };
            if (editingId) {
                await updateReportingReportCategory(editingId, payload);
            } else {
                await createReportingReportCategory(payload);
            }
            setEditingId(null);
            setForm(emptyForm);
            await loadData();
        } catch (err: unknown) {
            const detail = (err as ApiErrorLike)?.response?.data?.detail;
            alert(typeof detail === 'string' ? detail : '保存失败');
        } finally {
            setSaving(false);
        }
    };

    const handleDelete = async (id: number, name: string) => {
        if (!window.confirm(`确定删除分类「${name}」吗？子分类需先移走。`)) return;
        try {
            await deleteReportingReportCategory(id);
            await loadData();
        } catch (err: unknown) {
            const detail = (err as ApiErrorLike)?.response?.data?.detail;
            alert(typeof detail === 'string' ? detail : '删除失败');
        }
    };

    const handleCancel = () => {
        setEditingId(null);
        setForm(emptyForm);
        setShowForm(false);
    };

    const renderTreeItem = (item: ReportCategory, depth: number) => {
        const hasChildren = item.children && item.children.length > 0;
        const isExpanded = expandedIds.has(item.id);
        const isEditing = editingId === item.id;

        return (
            <div key={item.id}>
                <div
                    className={`category-tree-item${isEditing ? ' editing' : ''}`}
                    style={{ paddingLeft: `${depth * 24 + 8}px` }}
                >
                    <span
                        className="category-tree-toggle"
                        onClick={() => hasChildren && toggleExpand(item.id)}
                    >
                        {hasChildren ? (isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />) : <span style={{ width: 14, display: 'inline-block' }} />}
                    </span>
                    <span className="category-tree-name">{item.name}</span>
                    {item.status === 0 && <span className="category-tree-badge inactive">停用</span>}
                    <span className="category-tree-actions">
                        <button className="ghost-btn" title="添加子分类" onClick={() => handleAdd(item.id)}>
                            <Plus size={13} />
                        </button>
                        <button className="ghost-btn" title="编辑" onClick={() => handleEdit(item)}>
                            <Edit3 size={13} />
                        </button>
                        <button className="ghost-btn danger" title="删除" onClick={() => handleDelete(item.id, item.name)}>
                            <Trash2 size={13} />
                        </button>
                    </span>
                </div>
                {hasChildren && isExpanded && item.children!.map((child) => renderTreeItem(child, depth + 1))}
            </div>
        );
    };

    return (
        <div className="categories-section">
            <div className="categories-toolbar">
                <div className="categories-toolbar-left">
                    <span className="resource-meta">共 {categories.length} 个分类</span>
                </div>
                <div className="dataset-toolbar-actions">
                    <button className="btn-primary" onClick={() => handleAdd(null)}>
                        <FolderPlus size={14} />
                        新建根分类
                    </button>
                    <button className="btn-outline" onClick={() => void loadData()}>
                        <RefreshCw size={14} />
                        刷新
                    </button>
                </div>
            </div>

            {showForm && (
                <div className="category-form-bar">
                    <div className="category-form-fields">
                        <input
                            type="text"
                            placeholder="分类名称"
                            value={form.name}
                            onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                            className="category-form-input"
                            autoFocus
                        />
                        <Select
                            value={String(form.parent_id ?? '')}
                            onChange={(v) => setForm((f) => ({ ...f, parent_id: v ? Number(v) : null }))}
                            className="category-form-select"
                            options={[
                                { value: '', label: '无（根分类）' },
                                ...categories
                                    .filter((c) => c.id !== editingId)
                                    .map((c) => ({ value: String(c.id), label: c.path || c.name })),
                            ]}
                        />
                        <input
                            type="number"
                            placeholder="排序"
                            value={form.sort_order}
                            onChange={(e) => setForm((f) => ({ ...f, sort_order: Number(e.target.value) || 0 }))}
                            className="category-form-input small"
                        />
                        <Select
                            value={String(form.status)}
                            onChange={(v) => setForm((f) => ({ ...f, status: Number(v) }))}
                            className="category-form-select"
                            options={[
                                { value: '1', label: '启用' },
                                { value: '0', label: '停用' },
                            ]}
                        />
                        <input
                            type="text"
                            placeholder="描述（可选）"
                            value={form.description}
                            onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
                            className="category-form-input"
                        />
                    </div>
                    <div className="category-form-actions">
                        <button className="btn-primary" onClick={() => void handleSave()} disabled={saving}>
                            {saving ? '保存中...' : '保存'}
                        </button>
                        <button className="btn-outline" onClick={handleCancel}>
                            <X size={14} />
                            取消
                        </button>
                    </div>
                </div>
            )}

            <div className="category-tree-container">
                {loading ? (
                    <div className="empty-box">加载中...</div>
                ) : tree.length ? (
                    tree.map((item) => renderTreeItem(item, 0))
                ) : (
                    <div className="empty-box">暂无分类，点击上方按钮创建第一个分类。</div>
                )}
            </div>
        </div>
    );
}
