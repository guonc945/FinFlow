import { useState, useEffect } from 'react';
import { Plus, RefreshCw, Edit, Trash2, UserCheck, UserX } from 'lucide-react';
import DataTable from '../../components/data/DataTable';
import { getUsers, getOrganizations, createUser, updateUser, deleteUser, getAccountBooks } from '../../services/api';
import type { User, Organization } from '../../types';
import '../bills/Bills.css';
import './Users.css';

const Users = () => {
    const [users, setUsers] = useState<User[]>([]);
    const [organizations, setOrganizations] = useState<Organization[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const [showModal, setShowModal] = useState(false);
    const [editingUser, setEditingUser] = useState<User | null>(null);
    const [formData, setFormData] = useState({
        username: '',
        password: '',
        email: '',
        phone: '',
        real_name: '',
        org_id: 0,
        status: 1,
        account_book_ids: [] as string[]
    });

    const [accountBooks, setAccountBooks] = useState<any[]>([]);

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        setIsLoading(true);
        try {
            const [usersData, orgsData, accBooksData] = await Promise.all([
                getUsers(),
                getOrganizations(),
                getAccountBooks({ limit: 1000 })
            ]);
            setUsers(usersData);
            setOrganizations(orgsData);
            setAccountBooks(accBooksData.items || []);
        } catch (error) {
            console.error('Failed to fetch data:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleAdd = () => {
        setEditingUser(null);
        setFormData({
            username: '',
            password: '',
            email: '',
            phone: '',
            real_name: '',
            org_id: 0,
            status: 1,
            account_book_ids: []
        });
        setShowModal(true);
    };

    const handleEdit = (user: User) => {
        setEditingUser(user);
        setFormData({
            username: user.username,
            password: '',
            email: user.email || '',
            phone: user.phone || '',
            real_name: user.real_name || '',
            org_id: user.org_id || 0,
            status: user.status !== undefined ? user.status : 1,
            account_book_ids: user.account_book_ids || []
        });
        setShowModal(true);
    };

    const handleDelete = async (user: User) => {
        if (!confirm(`确定要删除用户 "${user.username}" 吗？`)) return;
        try {
            await deleteUser(user.id);
            fetchData();
        } catch (error) {
            console.error('Failed to delete user:', error);
            alert('删除失败');
        }
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            if (editingUser) {
                const updateData: any = { ...formData };
                if (!updateData.password) delete updateData.password;

                // Handle org_id: send null if it's 0 (Please select)
                if (updateData.org_id === 0) {
                    updateData.org_id = null;
                }

                await updateUser(editingUser.id, updateData);
            } else {
                const createData: any = { ...formData };
                if (createData.org_id === 0) {
                    delete createData.org_id; // For create, usually optional means null
                }
                await createUser(createData);
            }
            setShowModal(false);
            fetchData();
        } catch (error: any) {
            console.error('Failed to save user:', error);
            alert(error.response?.data?.detail || '保存失败');
        }
    };

    // ... existing getStatusBadge ...
    const getStatusBadge = (status: number) => {
        if (status === 1) return <span className="badge success"><UserCheck size={12} /> 正常</span>;
        if (status === 0) return <span className="badge warning"><UserX size={12} /> 禁用</span>;
        return <span className="badge error">锁定</span>;
    };

    const columns = [
        // ... columns ...
        { key: 'id' as keyof User, title: 'ID', width: 60 },
        { key: 'username' as keyof User, title: '用户名' },
        { key: 'real_name' as keyof User, title: '姓名' },
        { key: 'email' as keyof User, title: '邮箱' },
        { key: 'phone' as keyof User, title: '手机号' },
        { key: 'org_name' as keyof User, title: '所属组织' },
        {
            key: 'status' as keyof User,
            title: '状态',
            render: (val: any) => getStatusBadge(val)
        },
        {
            key: 'created_at' as keyof User,
            title: '创建时间',
            render: (val: any) => new Date(val).toLocaleDateString()
        },
        {
            key: 'actions' as keyof User,
            title: '操作',
            render: (_: any, row: User) => (
                <div className="flex gap-2">
                    <button className="icon-action" onClick={() => handleEdit(row)}>
                        <Edit size={16} />
                    </button>
                    <button className="icon-action danger" onClick={() => handleDelete(row)}>
                        <Trash2 size={16} />
                    </button>
                </div>
            )
        }
    ];

    return (
        <div className="page-container fade-in">
            <div className="page-header-row">
                <div className="header-actions">
                    <button className="btn btn-primary" onClick={handleAdd}>
                        <Plus size={16} /> 新增用户
                    </button>
                    <button className="btn btn-outline" onClick={fetchData}>
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
                <div className="modal-overlay" onClick={() => setShowModal(false)}>
                    <div className="modal-content large-modal" onClick={e => e.stopPropagation()}>
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
                                            onChange={e => setFormData({ ...formData, username: e.target.value })}
                                            required
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>{editingUser ? '密码 (留空不修改)' : '密码 *'}</label>
                                        <input
                                            type="password"
                                            value={formData.password}
                                            onChange={e => setFormData({ ...formData, password: e.target.value })}
                                            required={!editingUser}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>姓名</label>
                                        <input
                                            type="text"
                                            value={formData.real_name}
                                            onChange={e => setFormData({ ...formData, real_name: e.target.value })}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>邮箱</label>
                                        <input
                                            type="email"
                                            value={formData.email}
                                            onChange={e => setFormData({ ...formData, email: e.target.value })}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>手机号</label>
                                        <input
                                            type="text"
                                            value={formData.phone}
                                            onChange={e => setFormData({ ...formData, phone: e.target.value })}
                                        />
                                    </div>
                                    <div className="form-group">
                                        <label>所属组织</label>
                                        <select
                                            value={formData.org_id}
                                            onChange={e => setFormData({ ...formData, org_id: Number(e.target.value) })}
                                        >
                                            <option value={0}>-- 请选择 --</option>
                                            {organizations.map(org => (
                                                <option key={org.id} value={org.id}>{org.name}</option>
                                            ))}
                                        </select>
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
                                </div>
                            </div>

                            <div className="modal-separator"></div>

                            <div className="account-book-section">
                                <h4>可操作账套授权</h4>
                                <div className="account-books-grid" style={{ maxHeight: 'calc(100vh - 350px)' }}>
                                    {accountBooks.map(book => (
                                        <label key={book.id} className={`account-book-card ${formData.account_book_ids.includes(book.id) ? 'selected' : ''}`}>
                                            <input
                                                type="checkbox"
                                                className="hidden-checkbox"
                                                checked={formData.account_book_ids.includes(book.id)}
                                                onChange={(e) => {
                                                    if (e.target.checked) {
                                                        setFormData(prev => ({ ...prev, account_book_ids: [...prev.account_book_ids, book.id] }));
                                                    } else {
                                                        setFormData(prev => ({ ...prev, account_book_ids: prev.account_book_ids.filter(id => id !== book.id) }));
                                                    }
                                                }}
                                            />
                                            <div className="checkbox-custom"></div>
                                            <span className="checkbox-label">{book.name}</span>
                                        </label>
                                    ))}
                                    {accountBooks.length === 0 && <span style={{ color: '#94a3b8', fontSize: '14px' }}>暂无账套数据</span>}
                                </div>
                            </div>

                            <div className="modal-actions" style={{ gridColumn: '1 / -1' }}>
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

export default Users;
