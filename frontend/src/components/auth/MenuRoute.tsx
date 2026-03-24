import type { ReactNode } from 'react';
import { Navigate } from 'react-router-dom';

type MenuRouteProps = {
    menuKey: string;
    apiKey?: string;
    children: ReactNode;
};

const MenuRoute = ({ menuKey, apiKey, children }: MenuRouteProps) => {
    const token = localStorage.getItem('token');
    const userRaw = localStorage.getItem('user');
    const user = userRaw ? JSON.parse(userRaw) : null;

    if (!token) {
        return <Navigate to="/login" replace />;
    }

    if (user?.role === 'admin') {
        return <>{children}</>;
    }

    const menuKeys = Array.isArray(user?.menu_keys) ? user.menu_keys.filter((item: unknown): item is string => typeof item === 'string') : [];
    const apiKeys = Array.isArray(user?.api_keys) ? user.api_keys.filter((item: unknown): item is string => typeof item === 'string') : [];
    if (menuKeys.length > 0 && !menuKeys.includes(menuKey)) {
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
