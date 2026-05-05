import { useEffect, useState, useCallback } from 'react';
import { Search, Book, X, ChevronLeft, ChevronRight, Loader2 } from 'lucide-react';
import { getAccountBooks } from '../../services/api';
import type { KingdeeAccountBookBrief } from '../../types';
import './KingdeeProjectSelector.css';

interface AccountBookItem {
    id: string;
    number: string;
    name: string;
}

interface KingdeeAccountBookSelectorProps {
    value?: string;
    onSelect: (book: KingdeeAccountBookBrief | null) => void;
    label?: string;
    placeholder?: string;
}

const QUICK_PICK_LIMIT = 8;

const formatBookLabel = (book: AccountBookItem) => [book.number, book.name].filter(Boolean).join(' ').trim();

const KingdeeAccountBookSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择金蝶核算账簿...'
}: KingdeeAccountBookSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [inputValue, setInputValue] = useState(value || '');
    const [isInputFocused, setIsInputFocused] = useState(false);

    const [quickSearchTerm, setQuickSearchTerm] = useState('');
    const [quickMatches, setQuickMatches] = useState<AccountBookItem[]>([]);
    const [isQuickLoading, setIsQuickLoading] = useState(false);
    const [highlightedIndex, setHighlightedIndex] = useState(0);

    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [books, setBooks] = useState<AccountBookItem[]>([]);
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
                const res = await getAccountBooks({
                    search: quickSearchTerm,
                    limit: QUICK_PICK_LIMIT
                });
                if (cancelled) return;
                setQuickMatches(Array.isArray(res?.items) ? res.items : []);
                setHighlightedIndex(0);
            } catch (error) {
                if (!cancelled) {
                    console.error('Failed to fetch quick account book matches:', error);
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

    const fetchBooks = useCallback(async () => {
        setIsLoading(true);
        try {
            const res = await getAccountBooks({
                search: debouncedSearch || undefined,
                skip: (page - 1) * pageSize,
                limit: pageSize
            });
            setBooks(Array.isArray(res?.items) ? res.items : []);
            setTotal(res?.total || 0);
        } catch (error) {
            console.error('获取核算账簿列表失败:', error);
        } finally {
            setIsLoading(false);
        }
    }, [debouncedSearch, page, pageSize]);

    useEffect(() => {
        if (isModalOpen) {
            void fetchBooks();
        }
    }, [fetchBooks, isModalOpen]);

    const handleSelect = (book: AccountBookItem) => {
        const brief: KingdeeAccountBookBrief = {
            id: book.id,
            number: book.number,
            name: book.name
        };
        setInputValue(formatBookLabel(book));
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

        const localExact =
            quickMatches.find((item) => {
                const labelText = formatBookLabel(item).toLowerCase();
                return item.number.toLowerCase() === text || item.name.toLowerCase() === text || labelText === text;
            }) || null;
        if (localExact) {
            handleSelect(localExact);
            return;
        }

        try {
            const res = await getAccountBooks({ search: text, limit: 20 });
            const items = Array.isArray(res?.items) ? res.items : [];
            const exact = items.find((item: AccountBookItem) => {
                const labelText = formatBookLabel(item).toLowerCase();
                return item.number.toLowerCase() === text || item.name.toLowerCase() === text || labelText === text;
            });
            if (exact) {
                handleSelect(exact);
                return;
            }
        } catch (error) {
            console.error('Manual account book search failed', error);
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
                        <Book size={14} />
                    </button>
                </div>

                {isInputFocused && (quickSearchTerm || quickMatches.length > 0) && (
                    <div className="suggestions-dropdown quick-selector-dropdown">
                        {isQuickLoading ? (
                            <div className="quick-selector-empty">
                                <Loader2 className="animate-spin" size={14} />
                                <span>正在匹配核算账簿...</span>
                            </div>
                        ) : quickMatches.length > 0 ? (
                            quickMatches.map((book, index) => (
                                <button
                                    key={book.id}
                                    type="button"
                                    className={`suggestion-item quick-selector-item ${highlightedIndex === index ? 'highlighted' : ''}`}
                                    onMouseDown={(e) => e.preventDefault()}
                                    onClick={() => handleSelect(book)}
                                >
                                    <div className="quick-selector-main">
                                        <span className="quick-selector-code">{book.number || '-'}</span>
                                        <span className="quick-selector-name">{book.name || '-'}</span>
                                    </div>
                                </button>
                            ))
                        ) : (
                            <div className="quick-selector-empty">没有找到匹配的核算账簿</div>
                        )}
                    </div>
                )}
            </div>

            {isModalOpen && (
                <div className="project-modal-overlay" onClick={() => setIsModalOpen(false)}>
                    <div className="project-modal-content animate-scale-in" onClick={e => e.stopPropagation()} style={{ maxWidth: '600px' }}>
                        <div className="modal-header border-b border-slate-100/50 pb-4">
                            <div className="flex items-center gap-2">
                                <Book size={20} className="text-primary" />
                                <h3 className="text-lg font-bold">选择金蝶核算账簿</h3>
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
                                    placeholder="搜索账簿名称、编码..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="project-list-wrapper custom-scrollbar">
                                <table className="project-table">
                                    <thead>
                                        <tr>
                                            <th>编码</th>
                                            <th>账簿名称</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading && books.length === 0 ? (
                                            <tr>
                                                <td colSpan={2}>
                                                    <div className="flex justify-center py-8">
                                                        <Loader2 className="animate-spin text-slate-300" size={24} />
                                                    </div>
                                                </td>
                                            </tr>
                                        ) : books.length > 0 ? (
                                            books.map(book => (
                                                <tr key={book.id} onClick={() => handleSelect(book)} className="project-row">
                                                    <td className="font-mono text-primary font-medium" style={{ fontSize: '0.8rem' }}>{book.number || '-'}</td>
                                                    <td className="text-slate-700 font-medium">{book.name || '-'}</td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={2}>
                                                    <div className="text-center py-8 text-slate-400">暂无核算账簿数据</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>

                            <div className="modal-footer pt-3 border-t border-slate-100 flex justify-between items-center">
                                <div className="text-xs text-slate-500">
                                    共找到 <span className="font-bold text-slate-700">{total}</span> 个账簿
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

export default KingdeeAccountBookSelector;
