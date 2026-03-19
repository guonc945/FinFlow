import { useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import type { ReactNode } from 'react';
import {
    Activity,
    ChevronRight,
    Clock,
    Coins,
    FunctionSquare,
    Search,
    ShieldCheck,
    User,
    X,
    Zap,
} from 'lucide-react';
import axios from 'axios';
import { API_BASE_URL } from '../../services/apiBase';
import { localizeVariableItem } from './variablePresentation';
import './VariablePicker.css';

interface PickerItem {
    id: number | string;
    key: string;
    description: string;
    category: string;
    insert_text?: string;
    syntax?: string;
    example?: string;
}

interface VariablePickerProps {
    isOpen: boolean;
    onClose: () => void;
    onSelect: (item: any) => void;
    includeFunctions?: boolean;
}

const BUILT_IN_VARS: PickerItem[] = [
    { id: 'b1', key: 'CURRENT_DATE', description: '当前日期，格式为 YYYY-MM-DD', category: 'datetime' },
    { id: 'b2', key: 'CURRENT_DATETIME', description: '当前日期时间，格式为 YYYY-MM-DD HH:mm:ss', category: 'datetime' },
    { id: 'b3', key: 'CURRENT_TIME', description: '当前时间，格式为 HH:mm:ss', category: 'datetime' },
    { id: 'b4', key: 'YESTERDAY', description: '昨天的日期', category: 'datetime' },
    { id: 'b5', key: 'TOMORROW', description: '明天的日期', category: 'datetime' },
    { id: 'b6', key: 'TIMESTAMP', description: '当前 Unix 时间戳', category: 'datetime' },
    { id: 'b7', key: 'YEAR', description: '当前年份', category: 'datetime' },
    { id: 'b8', key: 'MONTH', description: '当前月份', category: 'datetime' },
    { id: 'b9', key: 'DAY', description: '当前日期中的日', category: 'datetime' },
    { id: 'b10', key: 'YEAR_MONTH', description: '当前年月', category: 'datetime' },
    { id: 's1', key: 'SYSTEM_VERSION', description: '系统版本号', category: 'system' },
    { id: 's2', key: 'APP_ENV', description: '应用运行环境', category: 'system' },
    { id: 's3', key: 'BASE_PATH', description: '系统 API 基础路径', category: 'system' },
    { id: 'r1', key: 'UUID', description: '随机生成的 UUID v4', category: 'random' },
    { id: 'r2', key: 'RANDOM_6', description: '随机 6 位数字', category: 'random' },
    { id: 'r3', key: 'NONCE', description: '随机 nonce 字符串', category: 'random' },
    { id: 'f1', key: 'CURRENCY', description: '默认币别代码', category: 'finance' },
    { id: 'f2', key: 'DEFAULT_TAX', description: '默认税率', category: 'finance' },
    { id: 'u1', key: 'CURRENT_ACCOUNT_BOOK_NUMBER', description: '当前账套编号', category: 'user' },
    { id: 'u2', key: 'CURRENT_ACCOUNT_BOOK_NAME', description: '当前账套名称', category: 'user' },
    { id: 'u3', key: 'CURRENT_USER_REALNAME', description: '当前用户姓名', category: 'user' },
    { id: 'u4', key: 'CURRENT_USERNAME', description: '当前用户名', category: 'user' },
    { id: 'u5', key: 'CURRENT_USER_ID', description: '当前用户 ID', category: 'user' },
    { id: 'u6', key: 'CURRENT_ORG_ID', description: '当前组织 ID', category: 'user' },
    { id: 'u7', key: 'CURRENT_ORG_NAME', description: '当前组织名称', category: 'user' },
];

type TabType = 'custom' | 'datetime' | 'system' | 'random' | 'finance' | 'user' | 'functions';

const VariablePicker = ({
    isOpen,
    onClose,
    onSelect,
    includeFunctions = false,
}: VariablePickerProps) => {
    const [dbVariables, setDbVariables] = useState<PickerItem[]>([]);
    const [functions, setFunctions] = useState<PickerItem[]>([]);
    const [search, setSearch] = useState('');
    const [activeTab, setActiveTab] = useState<TabType>('custom');
    const modalRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        const requests = [axios.get(`${API_BASE_URL}/settings/variables`)];
        if (includeFunctions) {
            requests.push(axios.get(`${API_BASE_URL}/settings/functions`));
        }

        Promise.all(requests)
            .then(([varsRes, functionsRes]) => {
                setDbVariables((varsRes.data || []).map(localizeVariableItem));
                setFunctions(includeFunctions ? (functionsRes?.data || []) : []);
            })
            .catch((err) => {
                console.error('Failed to load picker data:', err);
                setFunctions([]);
            });
    }, [includeFunctions, isOpen]);

    useEffect(() => {
        if (!includeFunctions && activeTab === 'functions') {
            setActiveTab('custom');
        }
    }, [activeTab, includeFunctions]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (modalRef.current && !modalRef.current.contains(event.target as Node)) {
                onClose();
            }
        };

        if (isOpen) {
            document.addEventListener('mousedown', handleClickOutside);
        }

        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [isOpen, onClose]);

    useEffect(() => {
        const handleEsc = (event: KeyboardEvent) => {
            if (event.key === 'Escape') {
                onClose();
            }
        };

        if (isOpen) {
            document.addEventListener('keydown', handleEsc);
        }

        return () => document.removeEventListener('keydown', handleEsc);
    }, [isOpen, onClose]);

    useEffect(() => {
        if (!isOpen || typeof document === 'undefined') {
            return;
        }

        const { body } = document;
        const previousOverflow = body.style.overflow;
        body.style.overflow = 'hidden';

        return () => {
            body.style.overflow = previousOverflow;
        };
    }, [isOpen]);

    const tabs = useMemo(() => {
        const baseTabs = [
            { id: 'custom', label: '自定义', icon: <Activity size={14} />, color: 'blue' },
            { id: 'datetime', label: '日期时间', icon: <Clock size={14} />, color: 'amber' },
            { id: 'user', label: '用户', icon: <User size={14} />, color: 'violet' },
            { id: 'system', label: '系统', icon: <ShieldCheck size={14} />, color: 'cyan' },
            { id: 'random', label: '工具', icon: <Zap size={14} />, color: 'rose' },
            { id: 'finance', label: '财务', icon: <Coins size={14} />, color: 'emerald' },
        ] as Array<{ id: TabType; label: string; icon: ReactNode; color: string }>;

        if (includeFunctions) {
            baseTabs.splice(1, 0, { id: 'functions', label: '函数', icon: <FunctionSquare size={14} />, color: 'indigo' });
        }

        return baseTabs;
    }, [includeFunctions]);

    const currentItems = useMemo(() => {
        if (activeTab === 'custom') {
            return dbVariables;
        }
        if (activeTab === 'functions') {
            return functions;
        }
        return BUILT_IN_VARS.filter(item => item.category === activeTab);
    }, [activeTab, dbVariables, functions]);

    const filteredItems = useMemo(() => {
        const keyword = search.trim().toLowerCase();
        if (!keyword) {
            return currentItems;
        }
        return currentItems.filter(item =>
            item.key.toLowerCase().includes(keyword) ||
            item.description.toLowerCase().includes(keyword) ||
            (item.syntax || '').toLowerCase().includes(keyword) ||
            (item.example || '').toLowerCase().includes(keyword)
        );
    }, [currentItems, search]);

    if (!isOpen) {
        return null;
    }

    if (typeof document === 'undefined') {
        return null;
    }

    return createPortal(
        <div className="ff-picker-overlay">
            <div ref={modalRef} className="ff-picker-modal glass-effect">
                <aside className="ff-picker-sidebar">
                    <div className="sidebar-logo">
                        <Activity size={24} className="text-blue-500 animate-pulse" />
                        <span>{includeFunctions ? '变量 / 函数' : '变量'}</span>
                    </div>

                    <div className="sidebar-tabs">
                        {tabs.map(tab => (
                            <button
                                key={tab.id}
                                className={`sidebar-tab-item ${activeTab === tab.id ? 'active' : ''} color-${tab.color}`}
                                onClick={() => setActiveTab(tab.id)}
                            >
                                <span className="tab-icon-box">{tab.icon}</span>
                                <span className="tab-label">{tab.label}</span>
                                {activeTab === tab.id && <div className="active-indicator" />}
                            </button>
                        ))}
                    </div>

                    <div className="sidebar-footer">
                        <div className="version-tag">FINFLOW V1.0</div>
                    </div>
                </aside>

                <main className="ff-picker-main">
                    <header className="ff-picker-header">
                        <div className="header-info">
                            <h3 className="picker-title">{tabs.find(tab => tab.id === activeTab)?.label}</h3>
                            <p className="picker-desc">
                                {activeTab === 'functions'
                                    ? '选择一个内置函数模板并插入到当前编辑内容中。'
                                    : '选择一个变量并插入到当前编辑内容中。'}
                            </p>
                        </div>
                        <button className="close-x-btn" onClick={onClose}>
                            <X size={20} />
                        </button>
                    </header>

                    <div className="ff-picker-search">
                        <div className="search-wrapper">
                            <Search size={18} className="search-glass" />
                            <input
                                autoFocus
                                placeholder={activeTab === 'functions' ? '搜索函数...' : '搜索变量...'}
                                value={search}
                                onChange={(e) => setSearch(e.target.value)}
                            />
                            {search && <X size={14} className="clear-x" onClick={() => setSearch('')} />}
                        </div>
                    </div>

                    <div className="ff-picker-scrollarea">
                        <div className="var-grid">
                            {filteredItems.length > 0 ? (
                                filteredItems.map(item => (
                                    <div
                                        key={item.id}
                                        className={`var-pro-card cat-${item.category}`}
                                        onClick={() => {
                                            onSelect(item);
                                            onClose();
                                        }}
                                    >
                                        <div className="var-pro-content">
                                            <div className="var-pro-key">
                                                <code>{item.key}</code>
                                            </div>
                                            <p className="var-pro-desc">{item.description}</p>
                                            {activeTab === 'functions' && item.syntax && (
                                                <p className="var-pro-desc"><code>{item.syntax}</code></p>
                                            )}
                                            {activeTab === 'functions' && item.example && (
                                                <p className="var-pro-desc">示例：<code>{item.example}</code></p>
                                            )}
                                        </div>
                                        <div className="var-pro-arrow">
                                            <ChevronRight size={16} />
                                        </div>
                                    </div>
                                ))
                            ) : (
                                <div className="no-data-state">
                                    <div className="no-data-icon"><Search size={40} /></div>
                                    <p>未找到匹配项。</p>
                                </div>
                            )}
                        </div>
                    </div>

                    <footer className="ff-picker-footer">
                        <div className="tip-box">
                            <ShieldCheck size={14} className="text-emerald-500" />
                            <span>
                                {activeTab === 'functions'
                                    ? <>函数会以模板形式插入，方便你继续补充或调整参数。</>
                                    : <>变量会以 <b>{`{key}`}</b> 的形式插入，并在运行时解析。</>}
                            </span>
                        </div>
                    </footer>
                </main>
            </div>
        </div>,
        document.body
    );
};

export default VariablePicker;
