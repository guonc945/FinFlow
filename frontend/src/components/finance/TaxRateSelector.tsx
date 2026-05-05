import { useEffect, useState, useRef } from 'react';
import { ChevronLeft, ChevronRight, Loader2, Percent, Search, X } from 'lucide-react';
import { getTaxRates } from '../../services/api';
import type { TaxRateBrief } from '../../types';
import './KingdeeProjectSelector.css';

interface TaxRateSelectorProps {
    value?: string;
    onSelect: (taxRate: TaxRateBrief | null) => void;
    label?: string;
    placeholder?: string;
}

const TaxRateSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择金蝶税率档案...',
}: TaxRateSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [inputValue, setInputValue] = useState(value || '');
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [items, setItems] = useState<TaxRateBrief[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [page, setPage] = useState(1);
    const [total, setTotal] = useState(0);
    const pageSize = 50;
    
    // 输入框搜索建议相关状态
    const [showSuggestions, setShowSuggestions] = useState(false);
    const [suggestions, setSuggestions] = useState<TaxRateBrief[]>([]);
    const [highlightedIndex, setHighlightedIndex] = useState(-1);
    const [dropdownPosition, setDropdownPosition] = useState<{ top: number; left: number; width: number } | null>(null);
    const inputRef = useRef<HTMLInputElement>(null);
    const suggestionsRef = useRef<HTMLDivElement>(null);
    const wrapperRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        setInputValue(value || '');
    }, [value]);

    // 点击外部关闭搜索建议
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (
                inputRef.current &&
                !inputRef.current.contains(event.target as Node) &&
                suggestionsRef.current &&
                !suggestionsRef.current.contains(event.target as Node)
            ) {
                setShowSuggestions(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    // 计算下拉列表位置
    useEffect(() => {
        if (showSuggestions && wrapperRef.current) {
            const rect = wrapperRef.current.getBoundingClientRect();
            setDropdownPosition({
                top: rect.bottom + 6,
                left: rect.left,
                width: rect.width,
            });
        }
    }, [showSuggestions]);

    // 窗口滚动或大小改变时更新位置
    useEffect(() => {
        const updatePosition = () => {
            if (showSuggestions && wrapperRef.current) {
                const rect = wrapperRef.current.getBoundingClientRect();
                setDropdownPosition({
                    top: rect.bottom + 6,
                    left: rect.left,
                    width: rect.width,
                });
            }
        };

        window.addEventListener('scroll', updatePosition, true);
        window.addEventListener('resize', updatePosition);
        return () => {
            window.removeEventListener('scroll', updatePosition, true);
            window.removeEventListener('resize', updatePosition);
        };
    }, [showSuggestions]);

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearch(searchTerm);
            setPage(1);
        }, 400);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    // 输入框实时搜索建议
    useEffect(() => {
        const fetchSuggestions = async () => {
            if (!inputValue.trim()) {
                setSuggestions([]);
                setShowSuggestions(false);
                return;
            }
            
            try {
                const res = await getTaxRates({ search: inputValue.trim(), limit: 10 });
                const filteredItems = (res.items || []).filter(
                    item => 
                        item.number?.toLowerCase().includes(inputValue.toLowerCase()) ||
                        item.name?.toLowerCase().includes(inputValue.toLowerCase()) ||
                        `${item.number} ${item.name}`.toLowerCase().includes(inputValue.toLowerCase())
                );
                setSuggestions(filteredItems);
                setShowSuggestions(filteredItems.length > 0);
                setHighlightedIndex(-1);
            } catch (error) {
                console.error('Fetch suggestions failed:', error);
                setSuggestions([]);
            }
        };
        
        const timer = setTimeout(fetchSuggestions, 300);
        return () => clearTimeout(timer);
    }, [inputValue]);

    useEffect(() => {
        if (!isModalOpen) return;
        const fetchData = async () => {
            setIsLoading(true);
            try {
                const res = await getTaxRates({
                    search: debouncedSearch || undefined,
                    skip: (page - 1) * pageSize,
                    limit: pageSize,
                });
                setItems(res.items || []);
                setTotal(res.total || 0);
            } catch (error) {
                console.error('Failed to fetch tax rates:', error);
            } finally {
                setIsLoading(false);
            }
        };
        fetchData();
    }, [debouncedSearch, isModalOpen, page]);

    const handleSelect = (taxRate: TaxRateBrief) => {
        setInputValue(`${taxRate.number} ${taxRate.name}`);
        onSelect(taxRate);
        setIsModalOpen(false);
        setShowSuggestions(false);
    };

    const handleClear = () => {
        setInputValue('');
        onSelect(null);
        setShowSuggestions(false);
    };

    const handleSuggestionSelect = (taxRate: TaxRateBrief) => {
        handleSelect(taxRate);
    };

    const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
        if (!showSuggestions || suggestions.length === 0) return;

        if (e.key === 'ArrowDown') {
            e.preventDefault();
            setHighlightedIndex(prev => (prev < suggestions.length - 1 ? prev + 1 : 0));
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            setHighlightedIndex(prev => (prev > 0 ? prev - 1 : suggestions.length - 1));
        } else if (e.key === 'Enter') {
            e.preventDefault();
            if (highlightedIndex >= 0 && highlightedIndex < suggestions.length) {
                handleSuggestionSelect(suggestions[highlightedIndex]);
            }
        } else if (e.key === 'Escape') {
            setShowSuggestions(false);
        }
    };

    const totalPages = Math.ceil(total / pageSize) || 1;

    return (
        <div className="project-selector-container">
            {label && <label className="selector-label">{label}</label>}
            <div ref={wrapperRef} className="selector-input-wrapper" style={{ position: 'relative' }}>
                <input
                    ref={inputRef}
                    type="text"
                    className="selector-input"
                    placeholder={placeholder}
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onKeyDown={handleKeyDown}
                    onFocus={() => inputValue && suggestions.length > 0 && setShowSuggestions(true)}
                />
                <div className="selector-actions">
                    {inputValue && (
                        <button type="button" className="clear-btn" onClick={handleClear} title="清除税率映射">
                            <X size={14} />
                        </button>
                    )}
                    <button type="button" className="search-trigger-btn" onClick={() => setIsModalOpen(true)}>
                        <Search size={14} />
                        </button>
                </div>
            </div>

            {/* 搜索建议下拉列表 - 使用 fixed 定位避免随父容器滚动 */}
            {showSuggestions && dropdownPosition && (
                <div
                    ref={suggestionsRef}
                    className="suggestions-dropdown"
                    style={{
                        position: 'fixed',
                        top: dropdownPosition.top,
                        left: dropdownPosition.left,
                        width: dropdownPosition.width,
                        zIndex: 99999,
                        backgroundColor: 'white',
                        borderRadius: '8px',
                        boxShadow: '0 10px 25px -5px rgba(0, 0, 0, 0.2), 0 4px 10px -2px rgba(0, 0, 0, 0.1)',
                        border: '1px solid #e2e8f0',
                        maxHeight: '240px',
                        overflowY: 'auto',
                    }}
                >
                        {suggestions.map((item, index) => (
                            <div
                                key={item.id}
                                className={`suggestion-item ${index === highlightedIndex ? 'highlighted' : ''}`}
                                onClick={() => handleSuggestionSelect(item)}
                                onMouseEnter={() => setHighlightedIndex(index)}
                                style={{
                                    padding: '10px 12px',
                                    cursor: 'pointer',
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center',
                                    backgroundColor: index === highlightedIndex ? '#f1f5f9' : 'transparent',
                                    borderBottom: index < suggestions.length - 1 ? '1px solid #f1f5f9' : 'none',
                                }}
                            >
                                <div style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                        <span
                                            className="font-mono"
                                            style={{
                                                color: '#3b82f6',
                                                fontWeight: 600,
                                                fontSize: '13px',
                                            }}
                                        >
                                            {item.number}
                                        </span>
                                        <span
                                            style={{
                                                color: '#1e293b',
                                                fontWeight: 500,
                                                fontSize: '13px',
                                            }}
                                        >
                                            {item.name}
                                        </span>
                                    </div>
                                    {item.enable_title && (
                                        <span
                                            style={{
                                                fontSize: '11px',
                                                color: '#64748b',
                                                marginTop: '2px',
                                            }}
                                        >
                                            {item.enable_title}
                                        </span>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>
            )}

            {isModalOpen && (
                <div className="project-modal-overlay" onClick={() => setIsModalOpen(false)}>
                    <div className="project-modal-content animate-scale-in" onClick={e => e.stopPropagation()}>
                        <div className="modal-header border-b border-slate-100/50 pb-4">
                            <div className="flex items-center gap-2">
                                <Percent size={20} className="text-primary" />
                                <h3 className="text-lg font-bold">选择金蝶税率档案</h3>
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
                                    placeholder="搜索税率编码或名称..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="project-list-wrapper custom-scrollbar">
                                <table className="project-table">
                                    <thead>
                                        <tr>
                                            <th>税率编码</th>
                                            <th>税率名称</th>
                                            <th>状态</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading && items.length === 0 ? (
                                            <tr>
                                                <td colSpan={3}>
                                                    <div className="flex justify-center py-8">
                                                        <Loader2 className="animate-spin text-slate-300" size={24} />
                                                    </div>
                                                </td>
                                            </tr>
                                        ) : items.length > 0 ? (
                                            items.map(item => (
                                                <tr key={item.id} onClick={() => handleSelect(item)} className="project-row">
                                                    <td className="font-mono text-primary font-medium">{item.number || '-'}</td>
                                                    <td className="text-slate-700 font-medium">{item.name || '-'}</td>
                                                    <td className="text-sm text-slate-500">{item.enable_title || item.status || '-'}</td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={3}>
                                                    <div className="text-center py-8 text-slate-400">没有找到匹配的金蝶税率档案</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>

                            <div className="modal-footer pt-3 border-t border-slate-100 flex justify-between items-center">
                                <div className="text-xs text-slate-500">
                                    共找到 <span className="font-bold text-slate-700">{total}</span> 条税率档案
                                </div>
                                <div className="pagination flex items-center gap-2">
                                    <button
                                        disabled={page <= 1 || isLoading}
                                        onClick={() => setPage(p => p - 1)}
                                        className="btn-icon"
                                    >
                                        <ChevronLeft size={16} />
                                    </button>
                                    <span className="text-sm px-2">{page} / {totalPages}</span>
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

export default TaxRateSelector;
