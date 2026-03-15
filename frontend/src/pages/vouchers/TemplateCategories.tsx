import { useEffect, useMemo, useState } from 'react';
import { Plus, RefreshCw, Edit, Trash2, ChevronRight, ChevronDown, FolderTree } from 'lucide-react';
import { createVoucherTemplateCategory, deleteVoucherTemplateCategory, getVoucherTemplateCategoriesTree, updateVoucherTemplateCategory } from '../../services/api';
import '../bills/Bills.css';
import '../users/Users.css';
import '../organizations/Organizations.css';

interface TemplateCategory {
    id: number;
    name: string;
    parent_id?: number | null;
    sort_order?: number;
    status?: number;
    description?: string | null;
    path?: string | null;
    children?: TemplateCategory[];
}

interface CategoryNodeProps {
    category: TemplateCategory;
    level: number;
    onEdit: (category: TemplateCategory) => void;
    onDelete: (category: TemplateCategory) => void;
    onAddChild: (parent: TemplateCategory) => void;
}

const flattenCategories = (nodes: TemplateCategory[] = [], parentPath: string = ''): Array<{ id: number; path: string }> => {
    const result: Array<{ id: number; path: string }> = [];
    nodes.forEach(node => {
        const path = parentPath ? `${parentPath} / ${node.name}` : node.name;
        result.push({ id: node.id, path });
        if (node.children && node.children.length > 0) {
            result.push(...flattenCategories(node.children, path));
        }
    });
    return result;
};

const collectDescendantIds = (node: TemplateCategory | null, acc: Set<number>) => {
    if (!node || !node.children) return;
    node.children.forEach(child => {
        acc.add(child.id);
        collectDescendantIds(child, acc);
    });
};

const CategoryNode = ({ category, level, onEdit, onDelete, onAddChild }: CategoryNodeProps) => {
    const [expanded, setExpanded] = useState(true);
    const hasChildren = category.children && category.children.length > 0;

    return (
        <div className="org-tree-node">
            <div className="org-tree-item" style={{ paddingLeft: `${level * 24 + 12}px` }}>
                <span className="org-expand" onClick={() => setExpanded(!expanded)}>
                    {hasChildren ? (
                        expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />
                    ) : <span className="expand-placeholder" />}
                </span>
                <FolderTree size={16} className="org-icon" />
                <span className="org-name">{category.name}</span>
                {category.path && category.path !== category.name && (
                    <span className="org-code">({category.path})</span>
                )}
                <span className={`org-status ${category.status === 1 ? 'active' : 'inactive'}`}>
                    {category.status === 1 ? '启用' : '停用'}
                </span>
                <div className="org-actions">
                    <button className="icon-action small" onClick={() => onAddChild(category)} title="新增子分类">
                        <Plus size={14} />
                    </button>
                    <button className="icon-action small" onClick={() => onEdit(category)} title="编辑">
                        <Edit size={14} />
                    </button>
                    <button className="icon-action small danger" onClick={() => onDelete(category)} title="删除">
                        <Trash2 size={14} />
                    </button>
                </div>
            </div>
            {hasChildren && expanded && (
                <div className="org-children">
                    {category.children!.map(child => (
                        <CategoryNode
                            key={child.id}
                            category={child}
                            level={level + 1}
                            onEdit={onEdit}
                            onDelete={onDelete}
                            onAddChild={onAddChild}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

const TemplateCategories = () => {
    const [categories, setCategories] = useState<TemplateCategory[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [showModal, setShowModal] = useState(false);
    const [editingCategory, setEditingCategory] = useState<TemplateCategory | null>(null);
    const [parentCategory, setParentCategory] = useState<TemplateCategory | null>(null);
    const [formData, setFormData] = useState({
        name: '',
        description: '',
        status: 1,
        sort_order: 0,
        parent_id: null as number | null,
    });

    const categoryOptions = useMemo(() => flattenCategories(categories), [categories]);
    const categoryPathMap = useMemo(() => {
        const map: Record<number, string> = {};
        categoryOptions.forEach(opt => { map[opt.id] = opt.path; });
        return map;
    }, [categoryOptions]);
    const disabledParentIds = useMemo(() => {
        const ids = new Set<number>();
        if (editingCategory) {
            ids.add(editingCategory.id);
            collectDescendantIds(editingCategory, ids);
        }
        return ids;
    }, [editingCategory]);

    const fetchCategories = async () => {
        setIsLoading(true);
        try {
            const data = await getVoucherTemplateCategoriesTree();
            setCategories(Array.isArray(data) ? data : []);
        } catch (error) {
            console.error('Failed to fetch template categories:', error);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        fetchCategories();
    }, []);

    const handleAdd = () => {
        setEditingCategory(null);
        setParentCategory(null);
        setFormData({ name: '', description: '', status: 1, sort_order: 0, parent_id: null });
        setShowModal(true);
    };

    const handleAddChild = (parent: TemplateCategory) => {
        setEditingCategory(null);
        setParentCategory(parent);
        setFormData({ name: '', description: '', status: 1, sort_order: 0, parent_id: parent.id });
        setShowModal(true);
    };

    const handleEdit = (category: TemplateCategory) => {
        setEditingCategory(category);
        setParentCategory(null);
        setFormData({
            name: category.name,
            description: category.description || '',
            status: category.status !== undefined ? category.status : 1,
            sort_order: category.sort_order !== undefined ? category.sort_order : 0,
            parent_id: category.parent_id ?? null,
        });
        setShowModal(true);
    };

    const handleDelete = async (category: TemplateCategory) => {
        if (!confirm(`确定要删除分类 "${category.name}" 吗？`)) return;
        try {
            await deleteVoucherTemplateCategory(category.id);
            fetchCategories();
        } catch (error: any) {
            console.error('Failed to delete category:', error);
            alert(error.response?.data?.detail || '删除失败');
        }
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            if (editingCategory) {
                await updateVoucherTemplateCategory(editingCategory.id, formData);
            } else {
                await createVoucherTemplateCategory({
                    ...formData,
                    parent_id: formData.parent_id ?? null,
                });
            }
            setShowModal(false);
            fetchCategories();
        } catch (error: any) {
            console.error('Failed to save category:', error);
            alert(error.response?.data?.detail || '保存失败');
        }
    };

    const selectedParentPath = formData.parent_id ? categoryPathMap[formData.parent_id] : '';

    return (
        <div className="page-container fade-in">
            <div className="page-header-row">
                <div className="header-actions">
                    <button className="btn btn-primary" onClick={handleAdd}>
                        <Plus size={16} /> 新增分类
                    </button>
                    <button className="btn btn-outline" onClick={fetchCategories}>
                        <RefreshCw size={16} /> 刷新
                    </button>
                </div>
            </div>

            <div className="org-tree-container">
                <div className="org-tree-header">
                    <h3>模板分类树</h3>
                </div>
                {isLoading ? (
                    <div className="loading-placeholder">加载中...</div>
                ) : categories.length === 0 ? (
                    <div className="empty-placeholder">
                        <FolderTree size={48} />
                        <p>暂无模板分类</p>
                        <button className="btn btn-primary" onClick={handleAdd}>
                            创建第一个分类
                        </button>
                    </div>
                ) : (
                    <div className="org-tree">
                        {categories.map(category => (
                            <CategoryNode
                                key={category.id}
                                category={category}
                                level={0}
                                onEdit={handleEdit}
                                onDelete={handleDelete}
                                onAddChild={handleAddChild}
                            />
                        ))}
                    </div>
                )}
            </div>

            {showModal && (
                <div className="modal-overlay" onClick={() => setShowModal(false)}>
                    <div className="modal-content" onClick={e => e.stopPropagation()}>
                        <h3>
                            {editingCategory
                                ? '编辑分类'
                                : parentCategory
                                    ? `在 "${parentCategory.name}" 下新增子分类`
                                    : '新增分类'}
                        </h3>
                        {parentCategory && !editingCategory && (
                            <p style={{ marginTop: '0.25rem', color: '#64748b', fontSize: '0.85rem' }}>
                                归属路径：{parentCategory.path || parentCategory.name}
                            </p>
                        )}
                        <form onSubmit={handleSubmit}>
                            <div className="form-group">
                                <label>分类名称</label>
                                <input
                                    type="text"
                                    value={formData.name}
                                    onChange={e => setFormData(prev => ({ ...prev, name: e.target.value }))}
                                    required
                                />
                            </div>
                            <div className="form-group">
                                <label>父级分类</label>
                                <select
                                    value={formData.parent_id ?? ''}
                                    onChange={e => {
                                        const nextId = e.target.value ? Number(e.target.value) : null;
                                        setFormData(prev => ({ ...prev, parent_id: nextId }));
                                    }}
                                >
                                    <option value="">无（顶级）</option>
                                    {categoryOptions.map(opt => (
                                        <option key={opt.id} value={opt.id} disabled={disabledParentIds.has(opt.id)}>
                                            {opt.path}
                                        </option>
                                    ))}
                                </select>
                                {selectedParentPath && (
                                    <div style={{ marginTop: '0.35rem', fontSize: '0.75rem', color: '#64748b' }}>
                                        当前路径：{selectedParentPath}
                                    </div>
                                )}
                            </div>
                            <div className="form-group">
                                <label>排序</label>
                                <input
                                    type="number"
                                    value={formData.sort_order}
                                    onChange={e => setFormData(prev => ({ ...prev, sort_order: Number(e.target.value) }))}
                                    min={0}
                                />
                            </div>
                            <div className="form-group">
                                <label>状态</label>
                                <select
                                    value={formData.status}
                                    onChange={e => setFormData(prev => ({ ...prev, status: Number(e.target.value) }))}
                                >
                                    <option value={1}>启用</option>
                                    <option value={0}>停用</option>
                                </select>
                            </div>
                            <div className="form-group">
                                <label>描述</label>
                                <input
                                    type="text"
                                    value={formData.description}
                                    onChange={e => setFormData(prev => ({ ...prev, description: e.target.value }))}
                                    placeholder="可选"
                                />
                            </div>
                            <div className="modal-actions">
                                <button type="button" className="btn btn-outline" onClick={() => setShowModal(false)}>取消</button>
                                <button type="submit" className="btn btn-primary">保存</button>
                            </div>
                        </form>
                    </div>
                </div>
            )}
        </div>
    );
};

export default TemplateCategories;
