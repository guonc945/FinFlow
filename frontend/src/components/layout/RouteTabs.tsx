import React, { startTransition, useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useOutlet } from 'react-router-dom';
import { X } from 'lucide-react';
import classNames from 'classnames';
import { preloadRoute } from '../../routes/lazyRoutes';
import './RouteTabs.css';

interface Tab {
    key: string;
    path: string;
    title: string;
}

const RouteTabs: React.FC<{ getPageTitle: (path: string) => { title: string } }> = ({ getPageTitle }) => {
    const location = useLocation();
    const { pathname, search } = location;
    const navigate = useNavigate();
    const outlet = useOutlet();
    const [tabs, setTabs] = useState<Tab[]>([]);
    const paneCacheRef = useRef<Record<string, React.ReactNode>>({});

    const openTab = (path: string) => {
        if (pathname === path) return;
        void preloadRoute(path);
        startTransition(() => navigate(path));
    };

    useEffect(() => {
        setTabs(prev => {
            const exists = prev.find(t => t.path === pathname);
            if (!exists) {
                const { title } = getPageTitle(pathname);
                return [...prev, {
                    key: pathname,
                    path: pathname,
                    title,
                }];
            }
            return prev;
        });

        if (outlet && !paneCacheRef.current[pathname]) {
            paneCacheRef.current[pathname] = outlet;
        }
    }, [getPageTitle, outlet, pathname, search]);

    const closeTab = (e: React.MouseEvent, key: string) => {
        e.stopPropagation();
        setTabs(prev => {
            const newTabs = prev.filter(t => t.key !== key);
            delete paneCacheRef.current[key];
            if (newTabs.length === 0) {
                startTransition(() => navigate('/'));
                return prev; // Let the next render handle adding the index page if it was cleared
            }
            if (key === pathname) {
                const nextPath = newTabs[newTabs.length - 1].path;
                void preloadRoute(nextPath);
                startTransition(() => navigate(nextPath));
            }
            return newTabs;
        });
    };

    return (
        <div className="route-tabs-container">
            <div className="route-tabs-header">
                {tabs.map(tab => (
                    <div
                        key={tab.key}
                        className={classNames('route-tab', { active: pathname === tab.path })}
                        onClick={() => openTab(tab.path)}
                        onMouseEnter={() => void preloadRoute(tab.path)}
                    >
                        <span className="tab-title">{tab.title}</span>
                        {tab.key !== '/' && (
                            <div className="tab-close-wrapper" onClick={(e) => closeTab(e, tab.key)}>
                                <X size={14} className="tab-close" />
                            </div>
                        )}
                    </div>
                ))}
            </div>
            <main className="page-content route-tabs-content">
                {tabs.map(tab => (
                    <div
                        key={tab.key}
                        className={classNames('route-tab-pane', { 'pane-active': pathname === tab.path })}
                    >
                        {paneCacheRef.current[tab.path]}
                    </div>
                ))}
            </main>
        </div>
    );
};

export default RouteTabs;
