import { useState, useEffect } from 'react';
import { Bell, Search, LogOut, Building2, ChevronDown } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { getMe } from '../../services/api';
import './Header.css';

const Header = () => {
    const [user, setUser] = useState<any>(null);
    const [activeAccountBook, setActiveAccountBook] = useState<string>('');
    const navigate = useNavigate();

    const handleLogout = () => {
        localStorage.removeItem('token');
        localStorage.removeItem('user');
        localStorage.removeItem('active_account_book');
        localStorage.removeItem('active_account_book_number');
        localStorage.removeItem('active_account_book_name');
        localStorage.removeItem('current_user_id');
        localStorage.removeItem('current_username');
        localStorage.removeItem('current_user_realname');
        localStorage.removeItem('current_org_id');
        localStorage.removeItem('current_org_name');
        navigate('/login');
    };

    useEffect(() => {
        const fetchUser = async () => {
            try {
                const data = await getMe();
                setUser(data);
                const persistedUser = localStorage.getItem('user');
                const parsedUser = persistedUser ? JSON.parse(persistedUser) : {};
                localStorage.setItem('user', JSON.stringify({
                    ...parsedUser,
                    ...data,
                    role: data.role || parsedUser?.role || 'user',
                    menu_keys: Array.isArray(data.menu_keys) ? data.menu_keys : parsedUser?.menu_keys || [],
                    api_keys: Array.isArray(data.api_keys) ? data.api_keys : parsedUser?.api_keys || [],
                }));

                // 将用户上下文信息写入 localStorage 供全局使用
                localStorage.setItem('current_user_id', String(data.id));
                localStorage.setItem('current_username', data.username);
                localStorage.setItem('current_user_realname', data.real_name || data.username);
                localStorage.setItem('current_org_id', String(data.org_id || ''));
                localStorage.setItem('current_org_name', data.org_name || '');

                // Initialize active account book
                if (data.account_books && data.account_books.length > 0) {
                    const stored = localStorage.getItem('active_account_book');
                    const valid = data.account_books.find((b: any) => b.id === stored);
                    if (valid) {
                        setActiveAccountBook(valid.id);
                        localStorage.setItem('active_account_book', valid.id);
                        localStorage.setItem('active_account_book_number', valid.number || '');
                        localStorage.setItem('active_account_book_name', valid.name);
                    } else {
                        const first = data.account_books[0];
                        setActiveAccountBook(first.id);
                        localStorage.setItem('active_account_book', first.id);
                        localStorage.setItem('active_account_book_number', first.number || '');
                        localStorage.setItem('active_account_book_name', first.name);
                    }
                }
            } catch (error) {
                console.error('Failed to fetch user:', error);
            }
        };
        fetchUser();
    }, []);

    const handleAccountBookChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
        const val = e.target.value;
        const selected = user?.account_books?.find((b: any) => String(b.id) === val);
        setActiveAccountBook(val);
        localStorage.setItem('active_account_book', val);
        localStorage.setItem('active_account_book_number', selected?.number || ''); // 存储账簿号
        localStorage.setItem('active_account_book_name', selected?.name || '');
        window.location.reload();
    };

    return (
        <header className="header glass">
            <div className="header-left">
            </div>

            <div className="header-right">
                <div className="search-bar hidden-sm">
                    <Search size={16} className="search-icon" />
                    <input type="text" placeholder="全局搜索..." />
                    <div className="search-shortcut">
                        <kbd>⌘</kbd><kbd>K</kbd>
                    </div>
                </div>

                <div className="icon-group">
                    <button className="icon-btn">
                        <Bell size={18} />
                        <span className="badge-dot"></span>
                    </button>
                    <div className="header-divider"></div>
                </div>

                <div className="account-book-selector">
                    <Building2 size={16} className="account-icon" />
                    <select
                        value={activeAccountBook}
                        onChange={handleAccountBookChange}
                        className="header-select glass-select"
                        title="当前操作账套"
                    >
                        {user?.account_books?.length > 0 ? (
                            user.account_books.map((b: any) => (
                                <option key={b.id} value={b.id}>{b.name}</option>
                            ))
                        ) : (
                            <option value="">暂无账套权限</option>
                        )}
                    </select>
                </div>

                <div className="header-divider hidden-sm"></div>

                <div
                    className="user-profile-header"
                    role="button"
                    tabIndex={0}
                    title="个人设置"
                    onClick={() => navigate('/account')}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') navigate('/account');
                    }}
                >
                    <div className="user-info-header hidden-sm">
                        <span className="user-name">{user?.real_name || 'Loading...'}</span>
                        <span className="user-role">{user?.org_name || 'System'}</span>
                    </div>
                    <div className="avatar-header">
                        {user?.real_name ? user.real_name[0].toUpperCase() : 'U'}
                    </div>
                    <ChevronDown size={14} className="profile-chevron" />
                </div>

                <button className="logout-btn-header" title="退出登录" onClick={handleLogout}>
                    <LogOut size={16} />
                </button>
            </div>
        </header>
    );
};

export default Header;
