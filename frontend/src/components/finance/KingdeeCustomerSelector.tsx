import { useEffect, useRef, useState } from 'react';
import { ChevronLeft, ChevronRight, Loader2, Search, User, X } from 'lucide-react';
import axios from 'axios';
import type { Customer } from '../../types';
import { API_BASE_URL } from '../../services/apiBase';
import './KingdeeCustomerSelector.css';

interface KingdeeCustomerSelectorProps {
    value?: string; // Display string or ID
    onSelect: (customer: Customer | null) => void;
    label?: string;
    placeholder?: string;
    defaultSearch?: string;
    autoOpen?: boolean;
}

const KingdeeCustomerSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择金蝶客户...',
    defaultSearch = '',
    autoOpen = false
}: KingdeeCustomerSelectorProps) => {
    const [isDropdownOpen, setIsDropdownOpen] = useState(autoOpen);
    const [inputValue, setInputValue] = useState(value || '');
    const containerRef = useRef<HTMLDivElement>(null);

    // Search & Data
    const [searchTerm, setSearchTerm] = useState(defaultSearch);
    const [debouncedSearch, setDebouncedSearch] = useState(defaultSearch);
    const [customers, setCustomers] = useState<Customer[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    // Pagination
    const [page, setPage] = useState(1);
    const [total, setTotal] = useState(0);
    const pageSize = 50;

    useEffect(() => {
        setInputValue(value || '');
    }, [value]);

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearch(searchTerm);
            setPage(1);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    useEffect(() => {
        if (!isDropdownOpen) return;
        fetchCustomers();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isDropdownOpen, debouncedSearch, page]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setTimeout(() => setIsDropdownOpen(false), 150);
            }
        };

        if (isDropdownOpen) {
            document.addEventListener('mousedown', handleClickOutside);
        }
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [isDropdownOpen]);

    const fetchCustomers = async () => {
        setIsLoading(true);
        try {
            const res = await axios.get(`${API_BASE_URL}/finance/customers`, {
                params: {
                    search: debouncedSearch,
                    skip: (page - 1) * pageSize,
                    limit: pageSize
                }
            });
            setCustomers(res.data.items || []);
            setTotal(res.data.total || 0);
        } catch (error) {
            console.error('Failed to fetch Kingdee customers:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSelect = (customer: Customer) => {
        setInputValue(`${customer.number} ${customer.name}`.trim());
        onSelect(customer);
        setIsDropdownOpen(false);
    };

    const handleClear = () => {
        setInputValue('');
        onSelect(null);
    };

    const handleManualSearch = async (text: string) => {
        if (!text) return;
        setIsLoading(true);
        try {
            const res = await axios.get(`${API_BASE_URL}/finance/customers`, {
                params: {
                    search: text,
                    limit: 10
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                const match = res.data.items.find((c: Customer) => c.number === text || c.name === text);
                if (match) handleSelect(match);
            }
        } catch (err) {
            console.error('Manual customer search failed', err);
        } finally {
            setIsLoading(false);
        }
    };

    const totalPages = Math.ceil(total / pageSize);

    return (
        <div className="kd-customer-selector-container kd-customer-selector" ref={containerRef}>
            {label && <label className="kd-customer-selector-label">{label}</label>}
            <div className="kd-customer-input-wrapper">
                <input
                    type="text"
                    className="kd-customer-input"
                    placeholder={placeholder}
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onBlur={() => handleManualSearch(inputValue)}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter') handleManualSearch(inputValue);
                    }}
                />
                <div className="kd-customer-actions">
                    {inputValue && (
                        <button type="button" className="kd-customer-clear" onClick={handleClear} title="清除映射" aria-label="清除映射">
                            <X size={14} />
                        </button>
                    )}
                    <button
                        type="button"
                        className="kd-customer-open"
                        onClick={() => setIsDropdownOpen(!isDropdownOpen)}
                        title="打开选择列表"
                        aria-label="打开选择列表"
                    >
                        <Search size={14} />
                    </button>
                </div>
            </div>

            <div className="kd-customer-dropdown-anchor">
                {isDropdownOpen && (
                    <div className="kd-customer-dropdown" role="listbox" aria-label="金蝶客户选择列表">
                        <div className="kd-customer-dropdown-header">
                            <div className="kd-customer-dropdown-icon" aria-hidden="true">
                                <User size={14} />
                            </div>
                            <div className="kd-customer-dropdown-title">选择金蝶客户</div>
                            <button
                                type="button"
                                className="kd-customer-dropdown-close"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setIsDropdownOpen(false);
                                }}
                                aria-label="关闭"
                                title="关闭"
                            >
                                <X size={14} />
                            </button>
                        </div>

                        <div className="kd-customer-dropdown-body">
                            <div className="kd-customer-search">
                                <Search size={14} className="kd-customer-search-icon" aria-hidden="true" />
                                <input
                                    type="text"
                                    className="kd-customer-search-input"
                                    placeholder="搜索客户编码、名称..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="kd-customer-table-wrapper">
                                <table className="kd-customer-table">
                                    <colgroup>
                                        <col style={{ width: '28%' }} />
                                        <col style={{ width: '52%' }} />
                                        <col style={{ width: '20%' }} />
                                    </colgroup>
                                    <thead className="kd-customer-thead">
                                        <tr>
                                            <th>编码</th>
                                            <th>名称</th>
                                            <th>状态</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading && customers.length === 0 ? (
                                            <tr>
                                                <td colSpan={3} className="kd-customer-loading-cell">
                                                    <Loader2 className="kd-customer-loading-icon" size={18} />
                                                    <span>加载中...</span>
                                                </td>
                                            </tr>
                                        ) : customers.length > 0 ? (
                                            customers.map((c) => (
                                                <tr key={c.id} onClick={() => handleSelect(c)} className="kd-customer-row">
                                                    <td className="kd-customer-cell kd-customer-cell-mono">{c.number || '-'}</td>
                                                    <td className="kd-customer-cell">{c.name || '-'}</td>
                                                    <td className="kd-customer-cell kd-customer-cell-muted kd-customer-cell-nowrap">{c.status || '-'}</td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={3} className="kd-customer-empty">
                                                    <Search size={18} aria-hidden="true" />
                                                    <div className="kd-customer-empty-title">未能找到匹配的客户</div>
                                                    <div className="kd-customer-empty-subtitle">请尝试修改关键词后重新搜索</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        <div className="kd-customer-dropdown-footer">
                            <div className="kd-customer-match-info">匹配到 {total} 条金蝶数据</div>
                            <div className="kd-customer-pagination">
                                <button
                                    type="button"
                                    className="kd-customer-page-btn"
                                    disabled={page <= 1 || isLoading}
                                    onClick={() => setPage((p) => p - 1)}
                                    aria-label="上一页"
                                    title="上一页"
                                >
                                    <ChevronLeft size={14} />
                                </button>
                                <div className="kd-customer-page-indicator">
                                    {page} / {totalPages || 1}
                                </div>
                                <button
                                    type="button"
                                    className="kd-customer-page-btn"
                                    disabled={page >= totalPages || isLoading}
                                    onClick={() => setPage((p) => p + 1)}
                                    aria-label="下一页"
                                    title="下一页"
                                >
                                    <ChevronRight size={14} />
                                </button>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};

export default KingdeeCustomerSelector;
