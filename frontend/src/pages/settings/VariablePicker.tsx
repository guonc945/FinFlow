import { useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import {
    Search,
    X,
    Clock,
    ShieldCheck,
    Zap,
    Coins,
    Activity,
    User,
    ChevronRight,
    FunctionSquare,
} from 'lucide-react';
import axios from 'axios';
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
    { id: 'b1', key: 'CURRENT_DATE', description: '当前日期 (YYYY-MM-DD)', category: 'datetime' },
    { id: 'b2', key: 'CURRENT_DATETIME', description: '当前日期时间 (YYYY-MM-DD HH:mm:ss)', category: 'datetime' },
    { id: 'b3', key: 'CURRENT_TIME', description: '当前时间 (HH:mm:ss)', category: 'datetime' },
    { id: 'b4', key: 'YESTERDAY', description: '昨天日期 (YYYY-MM-DD)', category: 'datetime' },
    { id: 'b5', key: 'TOMORROW', description: '明天日期 (YYYY-MM-DD)', category: 'datetime' },
    { id: 'b6', key: 'TIMESTAMP', description: 'Unix 时间戳（秒）', category: 'datetime' },
    { id: 'b7', key: 'YEAR', description: '当前年份 (YYYY)', category: 'datetime' },
    { id: 'b8', key: 'MONTH', description: '当前月份 (01-12)', category: 'datetime' },
    { id: 'b9', key: 'DAY', description: '当前日期中的日 (01-31)', category: 'datetime' },
    { id: 'b10', key: 'YEAR_MONTH', description: '当前年月 (YYYY-MM)', category: 'datetime' },
    { id: 's1', key: 'SYSTEM_VERSION', description: '系统版本号', category: 'system' },
    { id: 's2', key: 'APP_ENV', description: '运行环境', category: 'system' },
    { id: 's3', key: 'BASE_PATH', description: '后端服务基础路径', category: 'system' },
    { id: 'r1', key: 'UUID', description: '随机 UUID v4', category: 'random' },
    { id: 'r2', key: 'RANDOM_6', description: '6 位随机数字', category: 'random' },
    { id: 'r3', key: 'NONCE', description: '16 位随机字符串', category: 'random' },
    { id: 'f1', key: 'CURRENCY', description: '默认币种代码', category: 'finance' },
    { id: 'f2', key: 'DEFAULT_TAX', description: '默认税率', category: 'finance' },
    { id: 'u1', key: 'CURRENT_ACCOUNT_BOOK_NUMBER', description: '当前账簿编码', category: 'user' },
    { id: 'u2', key: 'CURRENT_ACCOUNT_BOOK_NAME', description: '当前账簿名称', category: 'user' },
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

        const requests = [axios.get(`${import.meta.env.VITE_API_BASE_URL}/settings/variables`)];
        if (includeFunctions) {
            requests.push(axios.get(`${import.meta.env.VITE_API_BASE_URL}/settings/functions`));
        }

        Promise.all(requests)
            .then(([varsRes, functionsRes]) => {
                setDbVariables(varsRes.data || []);
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

    const tabs = useMemo(() => {
        const baseTabs = [
            { id: 'custom', label: '自定义', icon: <Activity size={14} />, color: 'blue' },
            { id: 'datetime', label: '时间', icon: <Clock size={14} />, color: 'amber' },
            { id: 'user', label: '用户', icon: <User size={14} />, color: 'violet' },
            { id: 'system', label: '系统', icon: <ShieldCheck size={14} />, color: 'cyan' },
            { id: 'random', label: '工具', icon: <Zap size={14} />, color: 'rose' },
            { id: 'finance', label: '财务', icon: <Coins size={14} />, color: 'emerald' },
        ] as Array<{ id: TabType; label: string; icon: ReactNode; color: string }>;

        if (includeFunctions) {
            baseTabs.splice(2, 0, { id: 'functions', label: '内置函数', icon: <FunctionSquare size={14} />, color: 'indigo' });
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

    return (
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
                            <h3 className="picker-title">
                                {tabs.find(tab => tab.id === activeTab)?.label}资源库
                            </h3>
                            <p className="picker-desc">
                                {activeTab === 'functions'
                                    ? '选择一个内置函数并插入到当前输入框'
                                    : (includeFunctions
                                        ? '搜索并插入变量，或切换到“内置函数”页签插入日期和格式处理函数'
                                        : '搜索并插入变量到当前输入框')}
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
                                placeholder={activeTab === 'functions' ? '搜索函数名、语法、示例...' : '搜索变量名、描述...'}
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
                                                <p className="var-pro-desc">例如：<code>{item.example}</code></p>
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
                                    <p>没有找到匹配项</p>
                                </div>
                            )}
                        </div>
                    </div>

                    <footer className="ff-picker-footer">
                        <div className="tip-box">
                            <ShieldCheck size={14} className="text-emerald-500" />
                            <span>
                                {activeTab === 'functions'
                                    ? <>函数会按示例语法直接插入，你可以再替换其中字段。</>
                                    : <>变量插入格式为 <b>{`{key}`}</b>{includeFunctions ? '，也可以切换到“内置函数”页签使用其它能力。' : '，运行时会自动解析。'}</>}
                            </span>
                        </div>
                    </footer>
                </main>
            </div>
        </div>
    );
};

export default VariablePicker;
