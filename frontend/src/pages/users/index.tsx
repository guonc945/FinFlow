import { useEffect, useState } from 'react';
import { Edit, Plus, RefreshCw, Search, Trash2, UserCheck, UserX } from 'lucide-react';
import ConfirmModal from '../../components/common/ConfirmModal';
import DataTable from '../../components/data/DataTable';
import { ToastContainer, useToast } from '../../components/Toast';
import {
    createUser,
    deleteUser,
    getAccountBooks,
    getOrganizations,
    getUsers,
    updateUser,
} from '../../services/api';
import type { Organization, User } from '../../types';
import '../bills/Bills.css';
import './Users.css';

type AccountBookOption = {
    id: string;
    number?: string;
    name: string;
    accountingsys_name?: string;
};

type UserFormData = {
    username: string;
    password: string;
    email: string;
    phone: string;
    real_name: string;
    org_id: number;
    status: number;
    role: string;
    account_book_ids: string[];
};

const EMPTY_ORG_ID = 0;

const DEFAULT_FORM_DATA: UserFormData = {
    username: '',
    password: '',
    email: '',
    phone: '',
    real_name: '',
    org_id: EMPTY_ORG_ID,
    status: 1,
    role: 'user',
    account_book_ids: [],
};

const normalizeOptionalText = (value: string) => {
    const trimmed = value.trim();
    return trimmed || undefined;
};

const formatAccountBookLabel = (book: AccountBookOption) => {
    const number = (book.number || '-').trim() || '-';
    const name = (book.name || '-').trim() || '-';
    const structure = (book.accountingsys_name || '-').trim() || '-';
    return `${number}_${name} ${structure}`;
};

const getUserErrorMessage = (error: any) => {
    const detail = error?.response?.data?.detail;
    if (typeof detail !== 'string' || !detail.trim()) {
        return '请检查输入信息后重试。';
    }

    if (detail === 'Username already exists') {
        return '用户名已存在，请更换后重试。';
    }

    if (detail === 'Username is required') {
        return '用户名不能为空。';
    }

    if (detail === 'User save failed') {
        return '用户保存失败，请稍后重试。';
    }

    return detail;
};

const Users = () => {
    const { toasts, showToast, removeToast } = useToast();
    const [users, setUsers] = useState<User[]>([]);
    const [organizations, setOrganizations] = useState<Organization[]>([]);
    const [accountBooks, setAccountBooks] = useState<AccountBookOption[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [showModal, setShowModal] = useState(false);
    const [editingUser, setEditingUser] = useState<User | null>(null);
    const [formData, setFormData] = useState<UserFormData>(DEFAULT_FORM_DATA);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState<User | null>(null);
    const [isDeleting, setIsDeleting] = useState(false);
    const [accountBookSearch, setAccountBookSearch] = useState('');

    const resetForm = () => {
        setFormData(DEFAULT_FORM_DATA);
        setAccountBookSearch('');
    };

    const closeFormModal = (force = false) => {
        if (isSubmitting && !force) return;
        setShowModal(false);
        setEditingUser(null);
        resetForm();
    };

    const fetchData = async (options?: { silent?: boolean }) => {
        setIsLoading(true);
        try {
            const [usersData, orgsData, accountBookData] = await Promise.all([
                getUsers(),
                getOrganizations(),
                getAccountBooks({ limit: 1000 }),
            ]);

            setUsers(usersData);
            setOrganizations(orgsData);
            setAccountBooks(accountBookData.items || []);
        } catch (error) {
            console.error('Failed to fetch users page data:', error);
            if (!options?.silent) {
                showToast('error', '加载失败', '无法获取用户、组织或账簿数据。');
            }
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        void fetchData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const handleAdd = () => {
        setEditingUser(null);
        resetForm();
        setShowModal(true);
    };

    const handleEdit = (user: User) => {
        setEditingUser(user);
        setFormData({
            username: user.username || '',
            password: '',
            email: user.email || '',
            phone: user.phone || '',
            real_name: user.real_name || '',
            org_id: user.org_id || EMPTY_ORG_ID,
            status: user.status ?? 1,
            role: user.role || 'user',
            account_book_ids: user.account_book_ids || [],
        });
        setAccountBookSearch('');
        setShowModal(true);
    };

    const handleDeleteRequest = (user: User) => {
        setDeleteTarget(user);
    };

    const handleDeleteConfirm = async () => {
        if (!deleteTarget) return;

        setIsDeleting(true);
        try {
            await deleteUser(deleteTarget.id);
            setDeleteTarget(null);
            await fetchData({ silent: true });
            showToast('success', '删除成功', `用户“${deleteTarget.username}”已删除。`);
        } catch (error) {
            console.error('Failed to delete user:', error);
            showToast('error', '删除失败', '删除用户时发生错误，请稍后重试。');
        } finally {
            setIsDeleting(false);
        }
    };

    const handleSubmit = async (event: React.FormEvent) => {
        event.preventDefault();

        const isEditing = Boolean(editingUser);
        const username = formData.username.trim();
        const password = formData.password;
        const hasPassword = password.trim().length > 0;

        if (!username) {
            showToast('info', '请完善信息', '用户名不能为空。');
            return;
        }

        if (!isEditing && !hasPassword) {
            showToast('info', '请完善信息', '新增用户时必须填写密码。');
            return;
        }

        setIsSubmitting(true);
        try {
            if (editingUser) {
                await updateUser(editingUser.id, {
                    username,
                    password: hasPassword ? password : undefined,
                    email: normalizeOptionalText(formData.email) ?? null,
                    phone: normalizeOptionalText(formData.phone) ?? null,
                    real_name: normalizeOptionalText(formData.real_name) ?? null,
                    org_id: formData.org_id === EMPTY_ORG_ID ? null : formData.org_id,
                    status: formData.status,
                    role: formData.role,
                    account_book_ids: formData.account_book_ids,
                });
            } else {
                await createUser({
                    username,
                    password,
                    email: normalizeOptionalText(formData.email),
                    phone: normalizeOptionalText(formData.phone),
                    real_name: normalizeOptionalText(formData.real_name),
                    org_id: formData.org_id === EMPTY_ORG_ID ? undefined : formData.org_id,
                    status: formData.status,
                    role: formData.role,
                    account_book_ids: formData.account_book_ids,
                });
            }

            closeFormModal(true);
            await fetchData({ silent: true });
            showToast(
                'success',
                isEditing ? '保存成功' : '创建成功',
                isEditing ? '用户信息已更新。' : '新用户已创建。'
            );
        } catch (error) {
            console.error('Failed to save user:', error);
            showToast('error', '保存失败', getUserErrorMessage(error));
        } finally {
            setIsSubmitting(false);
        }
    };

    const filteredAccountBooks = accountBooks.filter((book) => {
        const keyword = accountBookSearch.trim().toLowerCase();
        if (!keyword) return true;

        return [book.number, book.name, book.accountingsys_name, formatAccountBookLabel(book)]
            .filter(Boolean)
            .some((value) => String(value).toLowerCase().includes(keyword));
    });

    const getStatusBadge = (status: number) => {
        if (status === 1) {
            return (
                <span className="badge success">
                    <UserCheck size={12} /> 正常
                </span>
            );
        }

        if (status === 0) {
            return (
                <span className="badge warning">
                    <UserX size={12} /> 禁用
                </span>
            );
        }

        return <span className="badge error">锁定</span>;
    };

    const getRoleLabel = (role?: string) => (role === 'admin' ? '管理员' : '普通用户');

    const columns = [
        { key: 'id' as keyof User, title: 'ID', width: 60 },
        { key: 'username' as keyof User, title: '用户名' },
        { key: 'real_name' as keyof User, title: '姓名' },
        {
            key: 'role' as keyof User,
            title: '角色',
            render: (value: string) => getRoleLabel(value),
        },
        { key: 'email' as keyof User, title: '邮箱' },
        { key: 'phone' as keyof User, title: '手机号' },
        { key: 'org_name' as keyof User, title: '所属组织' },
        {
            key: 'status' as keyof User,
            title: '状态',
            render: (value: number) => getStatusBadge(value),
        },
        {
            key: 'created_at' as keyof User,
            title: '创建时间',
            render: (value: string) => new Date(value).toLocaleDateString(),
        },
        {
            key: 'actions' as keyof User,
            title: '操作',
            render: (_value: unknown, row: User) => (
                <div className="flex gap-2">
                    <button type="button" className="icon-action" onClick={() => handleEdit(row)} title="编辑用户">
                        <Edit size={16} />
                    </button>
                    <button
                        type="button"
                        className="icon-action danger"
                        onClick={() => handleDeleteRequest(row)}
                        title="删除用户"
                    >
                        <Trash2 size={16} />
                    </button>
                </div>
            ),
        },
    ];

    return (
        <div className="page-container fade-in">
            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <div className="page-header-row">
                <div className="header-actions">
                    <button type="button" className="btn btn-primary" onClick={handleAdd}>
                        <Plus size={16} /> 新增用户
                    </button>
                    <button type="button" className="btn btn-outline" onClick={() => void fetchData()}>
                        <RefreshCw size={16} /> 刷新
                    </button>
                </div>
            </div>

            <DataTable
                columns={columns}
                data={users}
                loading={isLoading}
                tableId="users-list"
                title="用户列表"
            />

            {showModal && (
                <div className="modal-overlay" onClick={() => closeFormModal()}>
                    <div className="modal-content large-modal" onClick={(event) => event.stopPropagation()}>
                        <h3>{editingUser ? '编辑用户' : '新增用户'}</h3>
                        <form className="user-form-layout" onSubmit={handleSubmit}>
                            <div className="user-info-section">
                                <h4>基础信息</h4>
                                <div className="user-info-grid">
                                    <div className="form-group">
                                        <label>用户名 *</label>
                                        <input
                                            type="text"
                                            value={formData.username}
                                            onChange={(event) => setFormData({ ...formData, username: event.target.value })}
                                            required
                                            disabled={isSubmitting}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>{editingUser ? '密码（留空则不修改）' : '密码 *'}</label>
                                        <input
                                            type="password"
                                            value={formData.password}
                                            onChange={(event) => setFormData({ ...formData, password: event.target.value })}
                                            required={!editingUser}
                                            disabled={isSubmitting}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>姓名</label>
                                        <input
                                            type="text"
                                            value={formData.real_name}
                                            onChange={(event) => setFormData({ ...formData, real_name: event.target.value })}
                                            disabled={isSubmitting}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>邮箱（选填）</label>
                                        <input
                                            type="email"
                                            value={formData.email}
                                            onChange={(event) => setFormData({ ...formData, email: event.target.value })}
                                            disabled={isSubmitting}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>手机号</label>
                                        <input
                                            type="text"
                                            value={formData.phone}
                                            onChange={(event) => setFormData({ ...formData, phone: event.target.value })}
                                            disabled={isSubmitting}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>所属组织</label>
                                        <select
                                            value={formData.org_id}
                                            onChange={(event) => setFormData({ ...formData, org_id: Number(event.target.value) })}
                                            disabled={isSubmitting}
                                        >
                                            <option value={EMPTY_ORG_ID}>-- 请选择 --</option>
                                            {organizations.map((org) => (
                                                <option key={org.id} value={org.id}>
                                                    {org.name}
                                                </option>
                                            ))}
                                        </select>
                                    </div>
                                    <div className="form-group">
                                        <label>状态</label>
                                        <select
                                            value={formData.status}
                                            onChange={(event) => setFormData({ ...formData, status: Number(event.target.value) })}
                                            disabled={isSubmitting}
                                        >
                                            <option value={1}>正常</option>
                                            <option value={0}>禁用</option>
                                        </select>
                                    </div>
                                    <div className="form-group">
                                        <label>角色</label>
                                        <select
                                            value={formData.role}
                                            onChange={(event) => setFormData({ ...formData, role: event.target.value })}
                                            disabled={isSubmitting}
                                        >
                                            <option value="user">普通用户</option>
                                            <option value="admin">管理员</option>
                                        </select>
                                    </div>
                                </div>
                            </div>

                            <div className="modal-separator"></div>

                            <div className="account-book-section">
                                <h4>可操作账簿权限</h4>
                                <div className="account-book-toolbar">
                                    <div className="account-book-search">
                                        <Search size={14} className="account-book-search-icon" />
                                        <input
                                            type="text"
                                            value={accountBookSearch}
                                            onChange={(event) => setAccountBookSearch(event.target.value)}
                                            placeholder="快速搜索账簿编码、名称或结构"
                                            disabled={isSubmitting}
                                        />
                                    </div>
                                    <div className="account-book-meta">
                                        共 {filteredAccountBooks.length} / {accountBooks.length} 个账簿
                                    </div>
                                </div>
                                <div className="account-books-grid" style={{ maxHeight: 'calc(100vh - 350px)' }}>
                                    {filteredAccountBooks.map((book) => (
                                        <label
                                            key={book.id}
                                            className={`account-book-card ${formData.account_book_ids.includes(book.id) ? 'selected' : ''}`}
                                            title={formatAccountBookLabel(book)}
                                        >
                                            <input
                                                type="checkbox"
                                                className="hidden-checkbox"
                                                checked={formData.account_book_ids.includes(book.id)}
                                                disabled={isSubmitting}
                                                onChange={(event) => {
                                                    if (event.target.checked) {
                                                        setFormData((prev) => ({
                                                            ...prev,
                                                            account_book_ids: [...prev.account_book_ids, book.id],
                                                        }));
                                                    } else {
                                                        setFormData((prev) => ({
                                                            ...prev,
                                                            account_book_ids: prev.account_book_ids.filter((id) => id !== book.id),
                                                        }));
                                                    }
                                                }}
                                            />
                                            <div className="checkbox-custom"></div>
                                            <span className="checkbox-label">{formatAccountBookLabel(book)}</span>
                                        </label>
                                    ))}
                                    {accountBooks.length === 0 && (
                                        <span style={{ color: '#94a3b8', fontSize: '14px' }}>暂无账簿数据</span>
                                    )}
                                    {accountBooks.length > 0 && filteredAccountBooks.length === 0 && (
                                        <span style={{ color: '#94a3b8', fontSize: '14px' }}>未找到匹配的账簿，请尝试其他关键词</span>
                                    )}
                                </div>
                            </div>

                            <div className="modal-actions" style={{ gridColumn: '1 / -1' }}>
                                <button type="button" className="btn btn-outline" onClick={() => closeFormModal()} disabled={isSubmitting}>
                                    取消
                                </button>
                                <button type="submit" className="btn btn-primary" disabled={isSubmitting}>
                                    {isSubmitting ? '保存中...' : '保存'}
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
            )}

            <ConfirmModal
                isOpen={Boolean(deleteTarget)}
                title="删除用户"
                message={deleteTarget ? `确定要删除用户“${deleteTarget.username}”吗？此操作不可撤销。` : ''}
                confirmText="删除"
                cancelText="取消"
                variant="danger"
                loading={isDeleting}
                onConfirm={() => void handleDeleteConfirm()}
                onCancel={() => {
                    if (!isDeleting) {
                        setDeleteTarget(null);
                    }
                }}
            />
        </div>
    );
};

export default Users;
