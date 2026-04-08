import { useEffect, useState } from 'react';
import { Search, Landmark, X, ChevronLeft, ChevronRight, Loader2 } from 'lucide-react';
import { getBankAccounts } from '../../services/api';
import type { BankAccountBrief } from '../../types';
import './KingdeeProjectSelector.css';

interface BankAccountItem {
    id: string;
    name: string;
    bankaccountnumber: string;
    acctname?: string;
    bank_name?: string;
    accttype?: string;
    acctstatus?: string;
}

interface BankAccountSelectorProps {
    value?: string;
    onSelect: (account: BankAccountBrief | null) => void;
    label?: string;
    placeholder?: string;
}

const QUICK_PICK_LIMIT = 8;

const formatAccountLabel = (account: BankAccountItem) => {
    if (!account.name && !account.bankaccountnumber) return '';
    return `${account.name || ''}${account.bankaccountnumber ? ` (${account.bankaccountnumber})` : ''}`.trim();
};

const acctTypeLabel = (type?: string) => {
    if (type === 'in_out') return '收支户';
    if (type === 'in') return '收入户';
    if (type === 'out') return '支出户';
    return type || '-';
};

const BankAccountSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择银行账户...'
}: BankAccountSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [inputValue, setInputValue] = useState(value || '');
    const [isInputFocused, setIsInputFocused] = useState(false);

    const [quickSearchTerm, setQuickSearchTerm] = useState('');
    const [quickMatches, setQuickMatches] = useState<BankAccountItem[]>([]);
    const [isQuickLoading, setIsQuickLoading] = useState(false);
    const [highlightedIndex, setHighlightedIndex] = useState(0);

    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [accounts, setAccounts] = useState<BankAccountItem[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    const [page, setPage] = useState(1);
    const [total, setTotal] = useState(0);
    const pageSize = 50;

    useEffect(() => {
        setInputValue(value || '');
    }, [value]);

    useEffect(() => {
        const timer = window.setTimeout(() => {
            setDebouncedSearch(searchTerm);
            setPage(1);
        }, 300);
        return () => window.clearTimeout(timer);
    }, [searchTerm]);

    useEffect(() => {
        const timer = window.setTimeout(() => {
            setQuickSearchTerm(inputValue.trim());
        }, 250);
        return () => window.clearTimeout(timer);
    }, [inputValue]);

    useEffect(() => {
        if (isModalOpen) {
            void fetchAccounts();
        }
    }, [isModalOpen, debouncedSearch, page]);

    useEffect(() => {
        if (!isInputFocused) {
            setQuickMatches([]);
            setIsQuickLoading(false);
            setHighlightedIndex(0);
            return;
        }

        if (!quickSearchTerm) {
            setQuickMatches([]);
            setIsQuickLoading(false);
            setHighlightedIndex(0);
            return;
        }

        let cancelled = false;
        const fetchQuickMatches = async () => {
            setIsQuickLoading(true);
            try {
                const res = await getBankAccounts({
                    search: quickSearchTerm,
                    limit: QUICK_PICK_LIMIT
                });
                if (cancelled) return;
                setQuickMatches(Array.isArray(res?.items) ? res.items : []);
                setHighlightedIndex(0);
            } catch (error) {
                if (!cancelled) {
                    console.error('Failed to fetch quick bank account matches:', error);
                    setQuickMatches([]);
                }
            } finally {
                if (!cancelled) setIsQuickLoading(false);
            }
        };

        void fetchQuickMatches();
        return () => {
            cancelled = true;
        };
    }, [isInputFocused, quickSearchTerm]);

    const fetchAccounts = async () => {
        setIsLoading(true);
        try {
            const res = await getBankAccounts({
                search: debouncedSearch || undefined,
                skip: (page - 1) * pageSize,
                limit: pageSize
            });
            setAccounts(Array.isArray(res?.items) ? res.items : []);
            setTotal(res?.total || 0);
        } catch (error) {
            console.error('获取银行账户列表失败:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSelect = (account: BankAccountItem) => {
        const brief: BankAccountBrief = {
            id: account.id,
            name: account.name,
            bankaccountnumber: account.bankaccountnumber,
            bank_name: account.bank_name
        };
        setInputValue(formatAccountLabel(account));
        setQuickMatches([]);
        setHighlightedIndex(0);
        onSelect(brief);
        setIsModalOpen(false);
    };

    const handleClear = () => {
        setInputValue('');
        setQuickMatches([]);
        setHighlightedIndex(0);
        onSelect(null);
    };

    const commitManualInput = async () => {
        const text = inputValue.trim().toLowerCase();
        if (!text) {
            onSelect(null);
            return;
        }

        const matchLocal =
            quickMatches.find((item) => {
                const fullLabel = formatAccountLabel(item).toLowerCase();
                return (
                    (item.name || '').toLowerCase() === text ||
                    (item.bankaccountnumber || '').toLowerCase() === text ||
                    fullLabel === text
                );
            }) || null;
        if (matchLocal) {
            handleSelect(matchLocal);
            return;
        }

        try {
            const res = await getBankAccounts({ search: text, limit: 20 });
            const items = Array.isArray(res?.items) ? res.items : [];
            const exact = items.find((item: BankAccountItem) => {
                const fullLabel = formatAccountLabel(item).toLowerCase();
                return (
                    (item.name || '').toLowerCase() === text ||
                    (item.bankaccountnumber || '').toLowerCase() === text ||
                    fullLabel === text
                );
            });
            if (exact) {
                handleSelect(exact);
                return;
            }
        } catch (error) {
            console.error('Manual bank account search failed', error);
        }

        setInputValue(value || '');
    };

    const totalPages = Math.ceil(total / pageSize);

    return (
        <div className="project-selector-container">
            {label && <label className="selector-label">{label}</label>}
            <div className="selector-input-wrapper">
                <input
                    type="text"
                    className="selector-input"
                    placeholder={placeholder}
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onFocus={() => setIsInputFocused(true)}
                    onBlur={() => {
                        window.setTimeout(() => {
                            setIsInputFocused(false);
                            void commitManualInput();
                        }, 120);
                    }}
                    onKeyDown={(e) => {
                        if (e.key === 'ArrowDown' && quickMatches.length > 0) {
                            e.preventDefault();
                            setHighlightedIndex((prev) => Math.min(prev + 1, quickMatches.length - 1));
                        }
                        if (e.key === 'ArrowUp' && quickMatches.length > 0) {
                            e.preventDefault();
                            setHighlightedIndex((prev) => Math.max(prev - 1, 0));
                        }
                        if (e.key === 'Enter') {
                            e.preventDefault();
                            if (quickMatches[highlightedIndex]) {
                                handleSelect(quickMatches[highlightedIndex]);
                            } else {
                                void commitManualInput();
                            }
                        }
                        if (e.key === 'Escape') {
                            setQuickMatches([]);
                            setInputValue(value || '');
                        }
                    }}
                />
                <div className="selector-actions">
                    {inputValue && (
                        <button type="button" className="clear-btn" onClick={handleClear} title="清除绑定">
                            <X size={14} />
                        </button>
                    )}
                    <button type="button" className="search-trigger-btn" onClick={() => setIsModalOpen(true)}>
                        <Landmark size={14} />
                    </button>
                </div>

                {isInputFocused && (quickSearchTerm || quickMatches.length > 0) && (
                    <div className="suggestions-dropdown quick-selector-dropdown">
                        {isQuickLoading ? (
                            <div className="quick-selector-empty">
                                <Loader2 className="animate-spin" size={14} />
                                <span>正在匹配银行账户...</span>
                            </div>
                        ) : quickMatches.length > 0 ? (
                            quickMatches.map((account, index) => (
                                <button
                                    key={account.id}
                                    type="button"
                                    className={`suggestion-item quick-selector-item ${highlightedIndex === index ? 'highlighted' : ''}`}
                                    onMouseDown={(e) => e.preventDefault()}
                                    onClick={() => handleSelect(account)}
                                >
                                    <div className="quick-selector-main">
                                        <span className="quick-selector-name">{account.name || '-'}</span>
                                    </div>
                                    <div className="quick-selector-meta">
                                        {(account.bank_name || '银行账户') + (account.bankaccountnumber ? ` · ${account.bankaccountnumber}` : '')}
                                    </div>
                                </button>
                            ))
                        ) : (
                            <div className="quick-selector-empty">没有找到匹配的银行账户</div>
                        )}
                    </div>
                )}
            </div>

            {isModalOpen && (
                <div className="project-modal-overlay" onClick={() => setIsModalOpen(false)}>
                    <div className="project-modal-content animate-scale-in" onClick={e => e.stopPropagation()} style={{ maxWidth: '720px' }}>
                        <div className="modal-header border-b border-slate-100/50 pb-4">
                            <div className="flex items-center gap-2">
                                <Landmark size={20} className="text-primary" />
                                <h3 className="text-lg font-bold">选择银行账户</h3>
                            </div>
                            <button className="modal-close-btn" onClick={() => setIsModalOpen(false)}>
                                <X size={20} />
                            </button>
                        </div>

                        <div className="project-modal-body p-4 flex flex-col gap-4">
                            <div className="search-bar-wrapper">
                                <Search size={16} className="search-icon" />
                                <input
                                    type="text"
                                    placeholder="搜索账户名称、银行账号、开户行..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="project-list-wrapper custom-scrollbar">
                                <table className="project-table">
                                    <thead>
                                        <tr>
                                            <th>账户简称</th>
                                            <th>银行账号</th>
                                            <th>开户行</th>
                                            <th>账户性质</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading && accounts.length === 0 ? (
                                            <tr>
                                                <td colSpan={4}>
                                                    <div className="flex justify-center py-8">
                                                        <Loader2 className="animate-spin text-slate-300" size={24} />
                                                    </div>
                                                </td>
                                            </tr>
                                        ) : accounts.length > 0 ? (
                                            accounts.map(acct => (
                                                <tr key={acct.id} onClick={() => handleSelect(acct)} className="project-row">
                                                    <td className="text-slate-700 font-medium">{acct.name || '-'}</td>
                                                    <td className="font-mono text-primary font-medium" style={{ fontSize: '0.8rem' }}>{acct.bankaccountnumber || '-'}</td>
                                                    <td className="text-sm text-slate-500">{acct.bank_name || '-'}</td>
                                                    <td>
                                                        <span style={{
                                                            fontSize: '0.7rem',
                                                            fontWeight: 600,
                                                            padding: '0.15rem 0.4rem',
                                                            borderRadius: '4px',
                                                            background: acct.accttype === 'in' ? '#dcfce7' : acct.accttype === 'out' ? '#fef3c7' : '#dbeafe',
                                                            color: acct.accttype === 'in' ? '#15803d' : acct.accttype === 'out' ? '#b45309' : '#1d4ed8'
                                                        }}>
                                                            {acctTypeLabel(acct.accttype)}
                                                        </span>
                                                    </td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={4}>
                                                    <div className="text-center py-8 text-slate-400">暂无银行账户数据</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>

                            <div className="modal-footer pt-3 border-t border-slate-100 flex justify-between items-center">
                                <div className="text-xs text-slate-500">
                                    共找到 <span className="font-bold text-slate-700">{total}</span> 个银行账户
                                </div>
                                <div className="pagination flex items-center gap-2">
                                    <button
                                        disabled={page <= 1 || isLoading}
                                        onClick={() => setPage(p => p - 1)}
                                        className="btn-icon"
                                    >
                                        <ChevronLeft size={16} />
                                    </button>
                                    <span className="text-sm px-2">{page} / {totalPages || 1}</span>
                                    <button
                                        disabled={page >= totalPages || isLoading}
                                        onClick={() => setPage(p => p + 1)}
                                        className="btn-icon"
                                    >
                                        <ChevronRight size={16} />
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default BankAccountSelector;
