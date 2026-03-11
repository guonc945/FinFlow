import { useState, useEffect } from 'react';
import { Plus, RefreshCw, Edit, Trash2, ChevronRight, ChevronDown, Building2 } from 'lucide-react';
import { getOrganizationsTree, createOrganization, updateOrganization, deleteOrganization } from '../../services/api';
import type { Organization } from '../../types';
import '../bills/Bills.css';
import '../users/Users.css';
import './Organizations.css';

interface OrgTreeNodeProps {
    org: Organization;
    level: number;
    onEdit: (org: Organization) => void;
    onDelete: (org: Organization) => void;
    onAddChild: (parent: Organization) => void;
}

const OrgTreeNode = ({ org, level, onEdit, onDelete, onAddChild }: OrgTreeNodeProps) => {
    const [expanded, setExpanded] = useState(true);
    const hasChildren = org.children && org.children.length > 0;

    return (
        <div className="org-tree-node">
            <div className="org-tree-item" style={{ paddingLeft: `${level * 24 + 12}px` }}>
                <span className="org-expand" onClick={() => setExpanded(!expanded)}>
                    {hasChildren ? (
                        expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />
                    ) : <span className="expand-placeholder" />}
                </span>
                <Building2 size={16} className="org-icon" />
                <span className="org-name">{org.name}</span>
                {org.code && <span className="org-code">({org.code})</span>}
                <span className={`org-status ${org.status === 1 ? 'active' : 'inactive'}`}>
                    {org.status === 1 ? '正常' : '禁用'}
                </span>
                <div className="org-actions">
                    <button className="icon-action small" onClick={() => onAddChild(org)} title="添加子组织">
                        <Plus size={14} />
                    </button>
                    <button className="icon-action small" onClick={() => onEdit(org)} title="编辑">
                        <Edit size={14} />
                    </button>
                    <button className="icon-action small danger" onClick={() => onDelete(org)} title="删除">
                        <Trash2 size={14} />
                    </button>
                </div>
            </div>
            {hasChildren && expanded && (
                <div className="org-children">
                    {org.children!.map(child => (
                        <OrgTreeNode
                            key={child.id}
                            org={child}
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

const Organizations = () => {
    const [organizations, setOrganizations] = useState<Organization[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [showModal, setShowModal] = useState(false);
    const [editingOrg, setEditingOrg] = useState<Organization | null>(null);
    const [parentOrg, setParentOrg] = useState<Organization | null>(null);
    const [formData, setFormData] = useState({
        name: '',
        code: '',
        description: '',
        status: 1
    });

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        setIsLoading(true);
        try {
            const data = await getOrganizationsTree();
            setOrganizations(data);
        } catch (error) {
            console.error('Failed to fetch organizations:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleAdd = () => {
        setEditingOrg(null);
        setParentOrg(null);
        setFormData({ name: '', code: '', description: '', status: 1 });
        setShowModal(true);
    };

    const handleAddChild = (parent: Organization) => {
        setEditingOrg(null);
        setParentOrg(parent);
        setFormData({ name: '', code: '', description: '', status: 1 });
        setShowModal(true);
    };

    const handleEdit = (org: Organization) => {
        setEditingOrg(org);
        setParentOrg(null);
        setFormData({
            name: org.name,
            code: org.code || '',
            description: org.description || '',
            status: org.status !== undefined ? org.status : 1
        });
        setShowModal(true);
    };

    const handleDelete = async (org: Organization) => {
        if (!confirm(`确定要删除组织 "${org.name}" 吗？`)) return;
        try {
            await deleteOrganization(org.id);
            fetchData();
        } catch (error: any) {
            console.error('Failed to delete organization:', error);
            alert(error.response?.data?.detail || '删除失败');
        }
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            if (editingOrg) {
                await updateOrganization(editingOrg.id, formData);
            } else {
                await createOrganization({
                    ...formData,
                    parent_id: parentOrg?.id
                });
            }
            setShowModal(false);
            fetchData();
        } catch (error: any) {
            console.error('Failed to save organization:', error);
            alert(error.response?.data?.detail || '保存失败');
        }
    };

    return (
        <div className="page-container fade-in">
            {/* Headers and List - no changes needed there */}
            <div className="page-header-row">
                <div className="header-actions">
                    <button className="btn btn-primary" onClick={handleAdd}>
                        <Plus size={16} /> 新增组织
                    </button>
                    <button className="btn btn-outline" onClick={fetchData}>
                        <RefreshCw size={16} /> 刷新
                    </button>
                </div>
            </div>

            <div className="org-tree-container">
                {/* Tree content - no changes needed there */}
                <div className="org-tree-header">
                    <h3>组织架构</h3>
                </div>
                {isLoading ? (
                    <div className="loading-placeholder">加载中...</div>
                ) : organizations.length === 0 ? (
                    <div className="empty-placeholder">
                        <Building2 size={48} />
                        <p>暂无组织数据</p>
                        <button className="btn btn-primary" onClick={handleAdd}>
                            创建第一个组织
                        </button>
                    </div>
                ) : (
                    <div className="org-tree">
                        {organizations.map(org => (
                            <OrgTreeNode
                                key={org.id}
                                org={org}
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
                            {editingOrg ? '编辑组织' : parentOrg ? `在 "${parentOrg.name}" 下新增子组织` : '新增组织'}
                        </h3>
                        <form onSubmit={handleSubmit}>
                            <div className="form-group">
                                <label>组织名称 *</label>
                                <input
                                    type="text"
                                    value={formData.name}
                                    onChange={e => setFormData({ ...formData, name: e.target.value })}
                                    required
                                />
                            </div>
                            <div className="form-group">
                                <label>组织编码</label>
                                <input
                                    type="text"
                                    value={formData.code}
                                    onChange={e => setFormData({ ...formData, code: e.target.value })}
                                    placeholder="唯一标识，如 ORG001"
                                />
                            </div>
                            <div className="form-group">
                                <label>状态</label>
                                <select
                                    value={formData.status}
                                    onChange={e => setFormData({ ...formData, status: Number(e.target.value) })}
                                >
                                    <option value={1}>正常</option>
                                    <option value={0}>禁用</option>
                                </select>
                            </div>
                            <div className="form-group">
                                <label>描述</label>
                                <textarea
                                    value={formData.description}
                                    onChange={e => setFormData({ ...formData, description: e.target.value })}
                                    placeholder="组织描述信息"
                                />
                            </div>
                            <div className="modal-actions">
                                <button type="button" className="btn btn-outline" onClick={() => setShowModal(false)}>
                                    取消
                                </button>
                                <button type="submit" className="btn btn-primary">
                                    保存
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Organizations;
