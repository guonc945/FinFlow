import { startTransition, useEffect, useMemo, useState } from 'react';
import type { MouseEvent } from 'react';
import { NavLink, useLocation, useNavigate } from 'react-router-dom';
import {
    BarChart3,
    BookOpen,
    Building2,
    CalendarClock,
    Car,
    ChevronDown,
    ChevronRight,
    Database,
    FileJson,
    FileText,
    Home,
    Landmark,
    Layers,
    LayoutDashboard,
    Network,
    Receipt,
    Settings,
    Tags,
    Users,
    Wallet,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import classNames from 'classnames';
import { preloadRoute } from '../../routes/lazyRoutes';
import './Sidebar.css';

interface NavItem {
    path?: string;
    label: string;
    icon: LucideIcon;
    key?: string;
    adminOnly?: boolean;
    static?: boolean;
    children?: NavItem[];
}

const Sidebar = () => {
    const location = useLocation();
    const navigate = useNavigate();
    const [expandedMenus, setExpandedMenus] = useState<Record<string, boolean>>({});

    const warmRoute = (path?: string) => {
        if (!path) return;
        void preloadRoute(path);
    };

    const handleRouteClick = (path: string) => (event: MouseEvent<HTMLAnchorElement>) => {
        const isModifiedClick =
            event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0;
        if (isModifiedClick || location.pathname === path) return;

        event.preventDefault();
        warmRoute(path);
        startTransition(() => navigate(path));
    };

    const toggleMenu = (key: string) => {
        setExpandedMenus((prev) => ({
            ...prev,
            [key]: !prev[key],
        }));
    };

    const userStr = localStorage.getItem('user');
    const user = userStr ? JSON.parse(userStr) : null;
    const isAdmin = user?.role === 'admin';

    const filterNavItems = (items: NavItem[]): NavItem[] =>
        items
            .filter((item) => !item.adminOnly || isAdmin)
            .map((item) => {
                if (!item.children) return item;
                const filteredChildren = filterNavItems(item.children);
                if (filteredChildren.length === 0) return null;
                return {
                    ...item,
                    children: filteredChildren,
                };
            })
            .filter(Boolean) as NavItem[];

    const getItemKey = (item: NavItem, level: number, index: number) => item.key || `menu-${level}-${index}`;

    const collectAncestorKeys = (items: NavItem[], pathname: string, level = 0): string[] => {
        for (let i = 0; i < items.length; i += 1) {
            const item = items[i];
            const itemKey = getItemKey(item, level, i);
            if (item.path === pathname) return [];
            if (item.children) {
                const childAncestors = collectAncestorKeys(item.children, pathname, level + 1);
                if (childAncestors.length > 0 || item.children.some((child) => child.path === pathname)) {
                    return [itemKey, ...childAncestors];
                }
            }
        }
        return [];
    };

    const rawNavItems: NavItem[] = [
        { path: '/', label: '仪表盘', icon: LayoutDashboard },
        {
            key: 'marki-center',
            label: '马克业务',
            icon: FileText,
            children: [
                {
                    key: 'marki-documents',
                    label: '业务单据',
                    icon: Receipt,
                    children: [
                        { path: '/receipt-bills', label: '收款单据', icon: Receipt },
                        { path: '/deposit-records', label: '押金管理', icon: Wallet },
                        { path: '/prepayment-records', label: '预存款管理', icon: Wallet },
                        { path: '/bills', label: '运营账单', icon: Receipt },
                    ],
                },
                {
                    key: 'marki-archives',
                    label: '基础资料',
                    icon: Database,
                    children: [
                        { path: '/projects', label: '园区管理', icon: Building2, adminOnly: true },
                        { path: '/charge-items', label: '收费项目', icon: Wallet, adminOnly: true },
                        { path: '/houses', label: '房屋管理', icon: Home },
                        { path: '/residents', label: '住户管理', icon: Users },
                        { path: '/parks', label: '车位管理', icon: Car },
                    ],
                },
            ],
        },
        {
            key: 'kingdee-center',
            label: '金蝶财务',
            icon: Landmark,
            children: [
                {
                    key: 'kingdee-archives',
                    label: '财务档案',
                    icon: BookOpen,
                    children: [
                        { path: '/account-books', label: '账簿管理', icon: BookOpen },
                        { path: '/accounting-subjects', label: '会计科目', icon: BookOpen },
                        { path: '/auxiliary-data-categories', label: '辅助资料分类', icon: Tags },
                        { path: '/auxiliary-data', label: '辅助资料', icon: Layers },
                        { path: '/customers', label: '客户管理', icon: Users },
                        { path: '/suppliers', label: '供应商管理', icon: Users },
                        { path: '/kd-houses', label: '金蝶房号', icon: Home },
                        { path: '/bank-accounts', label: '银行账户', icon: Landmark },
                    ],
                },
                {
                    key: 'kingdee-vouchers',
                    label: '凭证管理',
                    icon: Layers,
                    children: [
                        { path: '/vouchers/templates', label: '凭证模板', icon: Layers, adminOnly: true },
                        { path: '/vouchers/categories', label: '模板分类', icon: Tags, adminOnly: true },
                    ],
                },
            ],
        },
        {
            key: 'oa-center',
            label: '泛微协同',
            icon: Building2,
            children: [
                { path: '/oa-center', label: '待接入页面', icon: FileText },
            ],
        },
        {
            key: 'integration-center',
            label: '集成中心',
            icon: Network,
            children: [
                { path: '/integrations/reporting', label: '报表设计', icon: BarChart3 },
                { path: '/integrations/sync-schedules', label: '同步计划', icon: CalendarClock, adminOnly: true },
                { path: '/integrations/credentials', label: '凭证配置', icon: Settings, adminOnly: true },
                { path: '/integrations/apis', label: '接口管理', icon: FileJson, adminOnly: true },
            ],
        },
        { path: '/report-center', label: '报表中心', icon: BarChart3 },
        {
            key: 'system-management',
            label: '系统管理',
            icon: Settings,
            adminOnly: true,
            children: [
                { path: '/organizations', label: '组织管理', icon: Network },
                { path: '/users', label: '用户管理', icon: Users },
                { path: '/settings', label: '系统设置', icon: Settings },
                { path: '/account', label: '个人设置', icon: Users },
            ],
        },
    ];

    const navItems = useMemo(() => filterNavItems(rawNavItems), [isAdmin]);
    const activeAncestorSet = useMemo(
        () => new Set(collectAncestorKeys(navItems, location.pathname)),
        [location.pathname, navItems]
    );

    useEffect(() => {
        const ancestors = collectAncestorKeys(navItems, location.pathname);
        if (ancestors.length === 0) return;
        setExpandedMenus((prev) => {
            const next = { ...prev };
            ancestors.forEach((key) => {
                next[key] = true;
            });
            return next;
        });
    }, [location.pathname, navItems]);

    const renderItems = (items: NavItem[], level = 0) =>
        items.map((item, index) => {
            if (item.children) {
                const key = getItemKey(item, level, index);
                const isExpanded = expandedMenus[key];

                return (
                    <div
                        key={key}
                        className={classNames('nav-group', {
                            'nested-group': level > 0,
                            'group-active': activeAncestorSet.has(key),
                        })}
                    >
                        <button
                            className={classNames('nav-item justify-between w-full', {
                                'nested-nav-item': level > 0,
                                'nav-parent-active': activeAncestorSet.has(key),
                                'nav-parent-open': isExpanded,
                            })}
                            onClick={() => toggleMenu(key)}
                        >
                            <div className="flex items-center gap-3">
                                <item.icon className="nav-icon" size={level === 0 ? 20 : 18} />
                                <span className="nav-text">{item.label}</span>
                            </div>
                            {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                        </button>
                        {isExpanded && (
                            <div className={classNames('nav-children', { 'nav-children-active': activeAncestorSet.has(key) })}>
                                {renderItems(item.children, level + 1)}
                            </div>
                        )}
                    </div>
                );
            }

            if (!item.path) {
                return (
                    <div
                        key={item.key || `static-${level}-${index}`}
                        className={classNames('nav-item', 'nav-static', {
                            'nav-child': level > 0,
                        })}
                    >
                        {level === 0 && <item.icon className="nav-icon" size={20} />}
                        <span className={classNames('nav-text', { 'pl-2': level > 0 })}>{item.label}</span>
                    </div>
                );
            }

            return (
                <NavLink
                    key={item.path}
                    to={item.path}
                    onClick={handleRouteClick(item.path)}
                    onMouseEnter={() => warmRoute(item.path)}
                    onFocus={() => warmRoute(item.path)}
                    onPointerDown={() => warmRoute(item.path)}
                    className={({ isActive }) =>
                        classNames('nav-item', {
                            active: isActive,
                            'nav-child': level > 0,
                        })
                    }
                >
                    {level === 0 && <item.icon className="nav-icon" size={20} />}
                    <span className={classNames('nav-text', { 'pl-2': level > 0 })}>{item.label}</span>
                </NavLink>
            );
        });

    return (
        <aside className="sidebar glass">
            <div className="sidebar-header">
                <div className="logo-container">
                    <div className="logo-icon bg-gradient-brand">FF</div>
                    <h1 className="logo-text">FinFlow</h1>
                </div>
            </div>

            <nav className="sidebar-nav custom-scrollbar">{renderItems(navItems)}</nav>
        </aside>
    );
};

export default Sidebar;
