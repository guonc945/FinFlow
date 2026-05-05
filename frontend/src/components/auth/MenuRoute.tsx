import type { ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { getAuthToken, getAuthUser } from '../../utils/authStorage';

type MenuRouteProps = {
    menuKey: string;
    fallbackMenuKeys?: string[];
    apiKey?: string;
    children: ReactNode;
};

const MenuRoute = ({ menuKey, fallbackMenuKeys = [], apiKey, children }: MenuRouteProps) => {
    const token = getAuthToken();
    const user = getAuthUser<{ role?: string; menu_keys?: unknown[]; api_keys?: unknown[] }>();

    if (!token) {
        return <Navigate to="/login" replace />;
    }

    if (user?.role === 'admin') {
        return <>{children}</>;
    }

    const menuKeys = Array.isArray(user?.menu_keys) ? user.menu_keys.filter((item: unknown): item is string => typeof item === 'string') : [];
    const apiKeys = Array.isArray(user?.api_keys) ? user.api_keys.filter((item: unknown): item is string => typeof item === 'string') : [];
    const allowedMenuKeys = [menuKey, ...fallbackMenuKeys];
    if (menuKeys.length > 0 && !allowedMenuKeys.some((key) => menuKeys.includes(key))) {
        const fallbackPath = menuKeys[0] || '/';
        return <Navigate to={fallbackPath} replace />;
    }

    if (apiKey && apiKeys.length > 0 && !apiKeys.includes(apiKey)) {
        const fallbackPath = menuKeys[0] || '/';
        return <Navigate to={fallbackPath} replace />;
    }

    return <>{children}</>;
};

export default MenuRoute;
