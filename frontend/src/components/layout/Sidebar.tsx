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
    DatabaseZap,
    FileJson,
    FileText,
    Home,
    Landmark,
    Layers,
    LayoutDashboard,
    Network,
    Receipt,
    Settings,
    ShieldCheck,
    Tags,
    Users,
    Wallet,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import classNames from 'classnames';
import { preloadRoute } from '../../routes/lazyRoutes';
import { getMe } from '../../services/api';
import { getAuthUser, setAuthUser } from '../../utils/authStorage';
import './Sidebar.css';

interface NavItem {
    path?: string;
    label: string;
    icon: LucideIcon;
    key?: string;
    permissionKeys?: string[];
    adminOnly?: boolean;
    children?: NavItem[];
}

interface SidebarUser {
    role?: string;
    menu_keys?: unknown[];
    api_keys?: unknown[];
}

const filterNavItems = (
    items: NavItem[],
    options: {
        isAdmin: boolean;
        hasExplicitMenuPermissions: boolean;
        allowedMenuKeySet: Set<string>;
    }
): NavItem[] =>
    items
        .filter((item) => {
            if (item.children) return true;
            if (item.adminOnly && !options.isAdmin) return false;
            if (options.isAdmin) return true;
            if (!item.path) return true;
            if (!options.hasExplicitMenuPermissions) return !item.adminOnly;
            const permissionKeys = item.permissionKeys?.length ? item.permissionKeys : [item.path];
            return permissionKeys.some((key) => options.allowedMenuKeySet.has(key));
        })
        .map((item) => {
            if (!item.children) return item;
            const filteredChildren = filterNavItems(item.children, options);
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

const RAW_NAV_ITEMS: NavItem[] = [
    { path: '/', label: '仪表盘', icon: LayoutDashboard },
    {
        key: 'mark-center',
        label: '马克业务',
        icon: FileText,
        children: [
            {
                key: 'mark-documents',
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
                key: 'mark-archives',
                label: '基础资料',
                icon: Database,
                children: [
                    { path: '/projects', label: '园区管理', icon: Building2 },
                    { path: '/charge-items', label: '收费项目', icon: Wallet },
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
                    { path: '/tax-rates', label: '税率档案', icon: Tags },
                    { path: '/kd-houses', label: '金蝶房号', icon: Home },
                    { path: '/bank-accounts', label: '银行账户', icon: Landmark },
                ],
            },
        ],
    },
    {
        key: 'oa-center',
        label: '泛微协同',
        icon: Building2,
        children: [{ path: '/oa-center', label: '协同入口', icon: FileText }],
    },
    {
        key: 'integration-center',
        label: '集成中心',
        icon: Network,
        children: [
            {
                key: 'integration-connectors',
                label: '接口接入',
                icon: FileJson,
                children: [
                    { path: '/integrations/credentials', label: '接口认证', icon: Settings },
                    { path: '/integrations/apis', label: '接口管理', icon: FileJson },
                ],
            },
            {
                key: 'integration-vouchers',
                label: '财务凭证',
                icon: Layers,
                children: [
                    { path: '/vouchers/templates', label: '凭证模板', icon: Layers },
                    { path: '/vouchers/categories', label: '模板分类', icon: Tags },
                ],
            },
            {
                key: 'integration-schedules',
                label: '计划任务',
                icon: CalendarClock,
                children: [
                    {
                        path: '/integrations/data-sync-schedules',
                        label: '数据同步',
                        icon: CalendarClock,
                        permissionKeys: ['/integrations/data-sync-schedules', '/integrations/sync-schedules'],
                    },
                    {
                        path: '/integrations/voucher-push-schedules',
                        label: '凭证推送',
                        icon: CalendarClock,
                        permissionKeys: ['/integrations/voucher-push-schedules', '/integrations/sync-schedules'],
                    },
                ],
            },
            {
                key: 'integration-data-center',
                label: '数据中心',
                icon: BarChart3,
                children: [
                    {
                        path: '/integrations/data-center/connections',
                        label: '数据连接',
                        icon: Database,
                        permissionKeys: ['/integrations/reporting', '/integrations/data-center', '/integrations/data-center/connections'],
                    },
                    {
                        path: '/integrations/data-center/datasets',
                        label: '数据建模',
                        icon: FileJson,
                        permissionKeys: ['/integrations/reporting', '/integrations/data-center', '/integrations/data-center/datasets'],
                    },
                    {
                        path: '/integrations/data-center/applications',
                        label: '报表设计',
                        icon: BarChart3,
                        permissionKeys: ['/integrations/reporting', '/integrations/data-center', '/integrations/data-center/applications'],
                    },
                    {
                        path: '/integrations/data-center/dictionaries',
                        label: '业务字典',
                        icon: DatabaseZap,
                        permissionKeys: ['/integrations/reporting', '/integrations/data-center', '/integrations/data-center/dictionaries'],
                    },
                    {
                        path: '/integrations/data-center/categories',
                        label: '报表分类',
                        icon: Tags,
                        permissionKeys: ['/integrations/reporting', '/integrations/data-center', '/integrations/data-center/categories'],
                    },
                ],
            },
        ],
    },
    { path: '/report-center', label: '报表中心', icon: BarChart3 },
    {
        key: 'system-management',
        label: '系统管理',
        icon: Settings,
        children: [
            { path: '/organizations', label: '组织管理', icon: Network },
            { path: '/users', label: '用户管理', icon: Users },
            { path: '/menu-permissions', label: '菜单权限', icon: ShieldCheck, adminOnly: true },
            { path: '/settings', label: '系统设置', icon: Settings },
            { path: '/account', label: '个人设置', icon: Users },
        ],
    },
];

const Sidebar = () => {
    const location = useLocation();
    const navigate = useNavigate();
    const [expandedMenus, setExpandedMenus] = useState<Record<string, boolean>>({});
    const [currentUser, setCurrentUser] = useState<SidebarUser | null>(() => {
        return getAuthUser();
    });

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

    useEffect(() => {
        let mounted = true;

        const refreshUserContext = async () => {
            try {
                const me = await getMe();
                if (!mounted) return;

                const parsedUser = getAuthUser<Record<string, unknown>>() || {};
                const nextUser = {
                    ...parsedUser,
                    ...me,
                    role: me.role || parsedUser?.role || 'user',
                    menu_keys: Array.isArray(me.menu_keys) ? me.menu_keys : parsedUser?.menu_keys || [],
                    api_keys: Array.isArray(me.api_keys) ? me.api_keys : parsedUser?.api_keys || [],
                };
                setAuthUser(nextUser);
                setCurrentUser(nextUser);
            } catch (error) {
                console.error('Failed to refresh sidebar user context:', error);
            }
        };

        void refreshUserContext();
        return () => {
            mounted = false;
        };
    }, []);

    const isAdmin = currentUser?.role === 'admin';
    const menuKeys = useMemo(
        () => (
            Array.isArray(currentUser?.menu_keys)
                ? currentUser.menu_keys.filter((item: unknown): item is string => typeof item === 'string')
                : []
        ),
        [currentUser?.menu_keys]
    );
    const hasExplicitMenuPermissions = isAdmin || menuKeys.length > 0;
    const allowedMenuKeySet = useMemo(() => new Set(menuKeys), [menuKeys]);

    const navItems = useMemo(
        () => filterNavItems(RAW_NAV_ITEMS, { allowedMenuKeySet, hasExplicitMenuPermissions, isAdmin }),
        [allowedMenuKeySet, hasExplicitMenuPermissions, isAdmin]
    );

    const activeAncestorSet = useMemo(
        () => new Set(collectAncestorKeys(navItems, location.pathname)),
        [location.pathname, navItems]
    );

    useEffect(() => {
        const ancestors = collectAncestorKeys(navItems, location.pathname);
        if (ancestors.length === 0) return;
        setExpandedMenus((prev) => {
            const needsUpdate = ancestors.some((key) => !prev[key]);
            if (!needsUpdate) {
                return prev;
            }
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
                return null;
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
