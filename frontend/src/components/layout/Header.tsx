import { useState, useEffect } from 'react';
import { Bell, LogOut, Building2, ChevronDown } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { getMe } from '../../services/api';
import { clearAuthSession, getAuthUser, setAuthUser } from '../../utils/authStorage';
import Select from '../common/Select';
import './Header.css';

type HeaderAccountBook = {
    id: string;
    name: string;
    number?: string | null;
};

type HeaderUser = {
    id: number | string;
    username: string;
    real_name?: string | null;
    org_id?: number | string | null;
    org_name?: string | null;
    role?: string;
    menu_keys?: string[];
    api_keys?: string[];
    account_books?: HeaderAccountBook[];
};

const Header = () => {
    const [user, setUser] = useState<HeaderUser | null>(null);
    const [activeAccountBook, setActiveAccountBook] = useState<string>('');
    const navigate = useNavigate();

    const handleLogout = () => {
        clearAuthSession();
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
                const data = await getMe() as HeaderUser;
                setUser(data);
                const parsedUser = getAuthUser<Partial<HeaderUser>>() || {};
                setAuthUser({
                    ...parsedUser,
                    ...data,
                    role: data.role || parsedUser?.role || 'user',
                    menu_keys: Array.isArray(data.menu_keys) ? data.menu_keys : parsedUser?.menu_keys || [],
                    api_keys: Array.isArray(data.api_keys) ? data.api_keys : parsedUser?.api_keys || [],
                });

                // 将用户上下文信息写入 localStorage 供全局使用
                localStorage.setItem('current_user_id', String(data.id));
                localStorage.setItem('current_username', data.username);
                localStorage.setItem('current_user_realname', data.real_name || data.username);
                localStorage.setItem('current_org_id', String(data.org_id || ''));
                localStorage.setItem('current_org_name', data.org_name || '');

                // Initialize active account book
                if (data.account_books && data.account_books.length > 0) {
                    const stored = localStorage.getItem('active_account_book');
                    const valid = data.account_books.find((b) => b.id === stored);
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

    const handleAccountBookChange = (val: string) => {
        const selected = user?.account_books?.find((b) => String(b.id) === val);
        setActiveAccountBook(val);
        localStorage.setItem('active_account_book', val);
        localStorage.setItem('active_account_book_number', selected?.number || ''); // 存储账簿号
        localStorage.setItem('active_account_book_name', selected?.name || '');
        window.location.reload();
    };

    const accountBookOptions = user?.account_books?.length
        ? user.account_books.map((book) => ({ value: book.id, label: book.name }))
        : [{ value: '', label: '暂无账套权限' }];

    return (
        <header className="header glass">
            <div className="header-left">
            </div>

            <div className="header-right">

                <div className="icon-group">
                    <button className="icon-btn">
                        <Bell size={18} />
                        <span className="badge-dot"></span>
                    </button>
                    <div className="header-divider"></div>
                </div>

                <div className="account-book-selector account-book-selector-custom">
                    <Building2 size={16} className="account-icon" />
                    <Select
                        value={activeAccountBook}
                        onChange={handleAccountBookChange}
                        className="header-select glass-select"
                        options={accountBookOptions}
                    />
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
