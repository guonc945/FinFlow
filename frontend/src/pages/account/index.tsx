import { useEffect, useMemo, useState } from 'react';
import { BookOpen, KeyRound, LogOut, RefreshCw, Save, SlidersHorizontal, User } from 'lucide-react';
import './Account.css';
import { getMe, getUserById, login, updateUser } from '../../services/api';
import { useToast, ToastContainer } from '../../components/Toast';

type SectionKey = 'profile' | 'password' | 'accountBook' | 'prefs' | 'session';

type ProfileForm = {
    real_name: string;
    email: string;
    phone: string;
};

type PasswordForm = {
    current: string;
    next: string;
    confirm: string;
};

const Account = () => {
    const { toasts, showToast, removeToast } = useToast();
    const [loading, setLoading] = useState(true);
    const [me, setMe] = useState<any>(null);
    const [section, setSection] = useState<SectionKey>('profile');
    const [profile, setProfile] = useState<ProfileForm>({
        real_name: '',
        email: '',
        phone: '',
    });
    const [savingProfile, setSavingProfile] = useState(false);

    const [pwd, setPwd] = useState<PasswordForm>({ current: '', next: '', confirm: '' });
    const [changingPwd, setChangingPwd] = useState(false);

    const [defaultAccountBookId, setDefaultAccountBookId] = useState<string>('');
    const [uiDensity, setUiDensity] = useState<'comfortable' | 'compact'>(
        (localStorage.getItem('ff_ui_density') as any) || 'comfortable'
    );
    const [savingPrefs, setSavingPrefs] = useState(false);

    const accountBooks = useMemo(() => (me?.account_books || []) as Array<{ id: string; name: string; number?: string }>, [me]);

    useEffect(() => {
        const mapHash = (h: string): SectionKey | null => {
            const v = (h || '').replace(/^#/, '');
            if (v === 'profile' || v === 'password' || v === 'accountBook' || v === 'prefs' || v === 'session') return v;
            return null;
        };

        const applyFromHash = () => {
            const from = mapHash(window.location.hash);
            if (from) setSection(from);
        };

        applyFromHash();
        window.addEventListener('hashchange', applyFromHash);
        return () => window.removeEventListener('hashchange', applyFromHash);
    }, []);

    const fetchAll = async () => {
        setLoading(true);
        try {
            const meData = await getMe();
            setMe(meData);

            const full = await getUserById(meData.id);
            setProfile({
                real_name: full.real_name || '',
                email: full.email || '',
                phone: full.phone || '',
            });

            const storedBook = localStorage.getItem('active_account_book');
            const books = (meData.account_books || []) as Array<{ id: string; name: string; number?: string }>;
            const initialBook = storedBook && books.find(b => String(b.id) === String(storedBook))
                ? String(storedBook)
                : (books[0]?.id ? String(books[0].id) : '');
            setDefaultAccountBookId(initialBook);
        } catch (e: any) {
            console.error(e);
            showToast('error', '加载失败', e?.response?.data?.detail || '无法获取用户信息');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void fetchAll();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const handleSaveProfile = async () => {
        if (!me?.id) return;
        setSavingProfile(true);
        try {
            const payload = {
                real_name: profile.real_name.trim() ? profile.real_name.trim() : null,
                email: profile.email.trim() ? profile.email.trim() : null,
                phone: profile.phone.trim() ? profile.phone.trim() : null,
            };
            await updateUser(me.id, {
                ...payload,
            });
            showToast('success', '保存成功', '个人信息已更新');
        } catch (e: any) {
            showToast('error', '保存失败', e?.response?.data?.detail || '无法保存个人信息');
        } finally {
            setSavingProfile(false);
        }
    };

    const handleChangePassword = async () => {
        if (!me?.id || !me?.username) return;
        if (!pwd.current || !pwd.next) {
            showToast('info', '请填写完整', '请输入当前密码和新密码');
            return;
        }
        if (pwd.next.length < 6) {
            showToast('info', '新密码过短', '建议至少 6 位');
            return;
        }
        if (pwd.next !== pwd.confirm) {
            showToast('error', '两次输入不一致', '请确认新密码输入一致');
            return;
        }

        setChangingPwd(true);
        try {
            // Verify current password by re-login (backend doesn't require it on update).
            const loginRes = await login(me.username, pwd.current);
            if (loginRes?.access_token) {
                localStorage.setItem('token', loginRes.access_token);
                localStorage.setItem('user', JSON.stringify(loginRes.user || {}));
            }

            await updateUser(me.id, { password: pwd.next });
            setPwd({ current: '', next: '', confirm: '' });
            showToast('success', '修改成功', '密码已更新，请牢记新密码');
        } catch (e: any) {
            showToast('error', '修改失败', e?.response?.data?.detail || '无法修改密码');
        } finally {
            setChangingPwd(false);
        }
    };

    const applyAccountBook = () => {
        const selected = accountBooks.find(b => String(b.id) === String(defaultAccountBookId));
        if (!selected) return;

        localStorage.setItem('active_account_book', String(selected.id));
        localStorage.setItem('active_account_book_number', selected.number || '');
        localStorage.setItem('active_account_book_name', selected.name || '');

        showToast('success', '已应用', '已切换默认账套，即将刷新页面');
        window.setTimeout(() => window.location.reload(), 400);
    };

    const handleSavePrefs = async () => {
        setSavingPrefs(true);
        try {
            localStorage.setItem('ff_ui_density', uiDensity);
            showToast('success', '已保存', '偏好设置已更新（本地生效）');
        } finally {
            setSavingPrefs(false);
        }
    };

    const handleLogout = () => {
        localStorage.removeItem('token');
        localStorage.removeItem('user');
        localStorage.removeItem('active_account_book');
        localStorage.removeItem('active_account_book_number');
        localStorage.removeItem('active_account_book_name');
        window.location.href = '/login';
    };

    if (loading) {
        return (
            <div className="account-page">
                <div className="account-card">
                    <div className="account-card-title"><RefreshCw size={16} /> 加载中...</div>
                </div>
            </div>
        );
    }

    const navItems: Array<{ key: SectionKey; label: string; sub: string; icon: any }> = [
        { key: 'profile', label: '个人信息', sub: '姓名、邮箱、手机号', icon: User },
        { key: 'password', label: '安全', sub: '修改登录密码', icon: KeyRound },
        { key: 'accountBook', label: '账套', sub: '默认账套与切换', icon: BookOpen },
        { key: 'prefs', label: '偏好', sub: '界面与表格密度', icon: SlidersHorizontal },
        { key: 'session', label: '会话', sub: '账号信息与退出', icon: LogOut },
    ];

    const setActiveSection = (key: SectionKey) => {
        setSection(key);
        // keep URL shareable and back/forward-friendly
        if (window.location.hash !== `#${key}`) {
            window.history.replaceState(null, '', `${window.location.pathname}${window.location.search}#${key}`);
        }
    };

    return (
        <div className="account-page fade-in">
            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <div className="account-shell">
                <aside className="account-nav">
                    <div className="account-nav-title">个人设置</div>
                    <div className="account-nav-list">
                        {navItems.map(item => (
                            <button
                                key={item.key}
                                className={`account-nav-item ${section === item.key ? 'active' : ''}`}
                                onClick={() => setActiveSection(item.key)}
                            >
                                <item.icon size={16} />
                                <div style={{ minWidth: 0 }}>
                                    <div>{item.label}</div>
                                    <span className="sub">{item.sub}</span>
                                </div>
                            </button>
                        ))}
                    </div>
                </aside>

                <div className="account-content">
                    <div className="account-header">
                        <div>
                            <h2>{navItems.find(n => n.key === section)?.label || '个人设置'}</h2>
                            <p>{navItems.find(n => n.key === section)?.sub || '维护个人资料、安全与偏好配置'}</p>
                        </div>
                        <button className="account-btn-secondary" onClick={fetchAll} disabled={loading} title="刷新用户信息">
                            <RefreshCw size={14} style={{ marginRight: 6 }} /> 刷新
                        </button>
                    </div>

                    {section === 'profile' && (
                        <section className="account-card">
                            <div className="account-card-title">
                                <User size={16} /> 个人信息
                            </div>

                            <div className="account-form">
                                <div className="account-field">
                                    <label>用户名</label>
                                    <input value={me?.username || ''} disabled />
                                </div>
                                <div className="account-field">
                                    <label>组织</label>
                                    <input value={me?.org_name || ''} disabled />
                                </div>
                                <div className="account-field">
                                    <label>姓名</label>
                                    <input value={profile.real_name} onChange={(e) => setProfile(p => ({ ...p, real_name: e.target.value }))} />
                                </div>
                                <div className="account-field">
                                    <label>邮箱</label>
                                    <input type="email" value={profile.email} onChange={(e) => setProfile(p => ({ ...p, email: e.target.value }))} />
                                </div>
                                <div className="account-field">
                                    <label>手机</label>
                                    <input value={profile.phone} onChange={(e) => setProfile(p => ({ ...p, phone: e.target.value }))} />
                                </div>
                            </div>

                            <div className="account-actions">
                                <button className="account-btn-primary" onClick={handleSaveProfile} disabled={savingProfile}>
                                    <Save size={14} style={{ marginRight: 6 }} />
                                    {savingProfile ? '保存中...' : '保存信息'}
                                </button>
                            </div>
                        </section>
                    )}

                    {section === 'password' && (
                        <section className="account-card">
                            <div className="account-card-title">
                                <KeyRound size={16} /> 密码修改
                            </div>

                            <div className="account-form one-col">
                                <div className="account-field">
                                    <label>当前密码</label>
                                    <input type="password" value={pwd.current} onChange={(e) => setPwd(p => ({ ...p, current: e.target.value }))} />
                                </div>
                                <div className="account-field">
                                    <label>新密码</label>
                                    <input type="password" value={pwd.next} onChange={(e) => setPwd(p => ({ ...p, next: e.target.value }))} />
                                </div>
                                <div className="account-field">
                                    <label>确认新密码</label>
                                    <input type="password" value={pwd.confirm} onChange={(e) => setPwd(p => ({ ...p, confirm: e.target.value }))} />
                                </div>
                            </div>

                            <div className="account-actions">
                                <button className="account-btn-primary" onClick={handleChangePassword} disabled={changingPwd}>
                                    {changingPwd ? '提交中...' : '修改密码'}
                                </button>
                            </div>
                        </section>
                    )}

                    {section === 'accountBook' && (
                        <section className="account-card">
                            <div className="account-card-title">
                                <BookOpen size={16} /> 默认账套
                            </div>

                            <div className="account-form one-col">
                                <div className="account-field">
                                    <label>默认账套（会刷新页面）</label>
                                    <select value={defaultAccountBookId} onChange={(e) => setDefaultAccountBookId(e.target.value)} disabled={accountBooks.length === 0}>
                                        {accountBooks.length === 0 ? (
                                            <option value="">暂无账套权限</option>
                                        ) : (
                                            accountBooks.map(b => (
                                                <option key={b.id} value={String(b.id)}>{b.name}</option>
                                            ))
                                        )}
                                    </select>
                                </div>
                            </div>

                            <div className="account-actions">
                                <button className="account-btn-secondary" onClick={applyAccountBook} disabled={!defaultAccountBookId || accountBooks.length === 0}>
                                    应用
                                </button>
                            </div>
                        </section>
                    )}

                    {section === 'prefs' && (
                        <section className="account-card">
                            <div className="account-card-title">
                                <SlidersHorizontal size={16} /> 偏好设置
                            </div>

                            <div className="account-form one-col">
                                <div className="account-field">
                                    <label>表格密度</label>
                                    <select value={uiDensity} onChange={(e) => setUiDensity(e.target.value as any)}>
                                        <option value="comfortable">舒适</option>
                                        <option value="compact">紧凑</option>
                                    </select>
                                </div>
                                <div className="account-field">
                                    <label>说明</label>
                                    <input value="偏好仅保存在本地浏览器（localStorage）" disabled />
                                </div>
                            </div>

                            <div className="account-actions">
                                <button className="account-btn-primary" onClick={handleSavePrefs} disabled={savingPrefs}>
                                    {savingPrefs ? '保存中...' : '保存偏好'}
                                </button>
                            </div>
                        </section>
                    )}

                    {section === 'session' && (
                        <section className="account-card">
                            <div className="account-card-title">
                                <LogOut size={16} /> 账号与会话
                            </div>

                            <div className="account-kv">
                                <div className="k">用户ID</div>
                                <div className="v">{me?.id ?? '-'}</div>
                                <div className="k">角色</div>
                                <div className="v">{me?.role || '-'}</div>
                                <div className="k">组织</div>
                                <div className="v">{me?.org_name || '-'}</div>
                            </div>

                            <div className="account-actions">
                                <button className="account-btn-danger" onClick={handleLogout}>
                                    退出登录
                                </button>
                            </div>
                        </section>
                    )}
                </div>
            </div>
        </div>
    );
};

export default Account;
