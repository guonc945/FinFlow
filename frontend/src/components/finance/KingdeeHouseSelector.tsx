import { useState, useEffect, useRef } from 'react';
import { Search, MapPin, X, ChevronLeft, ChevronRight, Loader2 } from 'lucide-react';
import axios from 'axios';
import type { KingdeeHouse } from '../../types';
import './KingdeeHouseSelector.css';

interface KingdeeHouseSelectorProps {
    value?: string; // Display string or ID
    onSelect: (house: KingdeeHouse | null) => void;
    label?: string;
    placeholder?: string;
    defaultSearch?: string;
    autoOpen?: boolean;
}

const KingdeeHouseSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择金蝶系统房号...',
    defaultSearch = '',
    autoOpen = false
}: KingdeeHouseSelectorProps) => {
    const [isDropdownOpen, setIsDropdownOpen] = useState(autoOpen);
    const [inputValue, setInputValue] = useState(value || '');
    const containerRef = useRef<HTMLDivElement>(null);

    // Search & Data
    const [searchTerm, setSearchTerm] = useState(defaultSearch);
    const [debouncedSearch, setDebouncedSearch] = useState(defaultSearch);
    const [houses, setHouses] = useState<KingdeeHouse[]>([]);
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
            setPage(1); // Reset page on new search
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    useEffect(() => {
        if (isDropdownOpen) {
            fetchHouses();
        }
    }, [isDropdownOpen, debouncedSearch, page]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                // Add a small delay so that if the user clicked an external button (like "Cancel"),
                // the click event has time to register before the dropdown unmounts and shifts the layout.
                setTimeout(() => {
                    setIsDropdownOpen(false);
                }, 150);
            }
        };
        if (isDropdownOpen) {
            document.addEventListener('mousedown', handleClickOutside);
        }
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [isDropdownOpen]);

    const fetchHouses = async () => {
        setIsLoading(true);
        try {
            const res = await axios.get(`${import.meta.env.VITE_API_BASE_URL}/finance/kd-houses`, {
                params: {
                    search: debouncedSearch,
                    skip: (page - 1) * pageSize,
                    limit: pageSize
                }
            });
            setHouses(res.data.items || []);
            setTotal(res.data.total || 0);
        } catch (error) {
            console.error('Failed to fetch Kingdee houses:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSelect = (house: KingdeeHouse) => {
        const displayNumber = house.wtw8_number || house.number || '';
        setInputValue(`${displayNumber} ${house.name}`.trim());
        onSelect(house);
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
            const res = await axios.get(`${import.meta.env.VITE_API_BASE_URL}/finance/kd-houses`, {
                params: {
                    search: text,
                    limit: 10
                }
            });
            if (res.data && Array.isArray(res.data.items)) {
                const match = res.data.items.find((h: KingdeeHouse) => h.wtw8_number === text || h.name === text || h.number === text);
                if (match) {
                    handleSelect(match);
                }
            }
        } catch (err) {
            console.error("Manual search failed", err);
        } finally {
            setIsLoading(false);
        }
    };

    const totalPages = Math.ceil(total / pageSize);

    return (
        <div className="house-selector-container kd-house-selector" ref={containerRef}>
            {label && <label className="selector-label">{label}</label>}
            <div className="selector-input-wrapper">
                <input
                    type="text"
                    className="selector-input"
                    placeholder={placeholder}
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onBlur={() => handleManualSearch(inputValue)}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter') handleManualSearch(inputValue);
                    }}
                />
                <div className="selector-actions">
                    {inputValue && (
                        <button type="button" className="clear-btn" onClick={handleClear} title="清除映射">
                            <X size={14} />
                        </button>
                    )}
                    <button type="button" className="search-trigger-btn" onClick={() => setIsDropdownOpen(!isDropdownOpen)}>
                        <Search size={14} />
                    </button>
                </div>
            </div>

            <div className="kd-house-dropdown-anchor">
                {isDropdownOpen && (
                    <div className="kd-house-dropdown" role="listbox" aria-label="金蝶房号选择列表">
                        <div className="kd-house-dropdown-header">
                            <div className="kd-house-dropdown-icon" aria-hidden="true">
                                <MapPin size={14} />
                            </div>
                            <div className="kd-house-dropdown-title">选择金蝶房号</div>
                            <button
                                type="button"
                                className="kd-house-dropdown-close"
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

                        <div className="kd-house-dropdown-body">
                            <div className="kd-house-search">
                                <Search size={14} className="kd-house-search-icon" aria-hidden="true" />
                                <input
                                    type="text"
                                    className="kd-house-search-input"
                                    placeholder="搜索房号、系统内码、房屋名称..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="kd-house-table-wrapper">
                                <table className="kd-house-table">
                                    <colgroup>
                                        <col style={{ width: '30%' }} />
                                        <col style={{ width: '30%' }} />
                                        <col style={{ width: '25%' }} />
                                        <col style={{ width: '15%' }} />
                                    </colgroup>
                                    <thead className="kd-house-thead">
                                        <tr>
                                            <th>房号 (wtw8)</th>
                                            <th>系统内码</th>
                                            <th>房屋名称</th>
                                            <th>投资权属</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading && houses.length === 0 ? (
                                            <tr>
                                                <td colSpan={4} className="kd-house-loading-cell">
                                                    <Loader2 className="kd-house-loading-icon" size={18} />
                                                    <span>加载中...</span>
                                                </td>
                                            </tr>
                                        ) : houses.length > 0 ? (
                                            houses.map((h) => (
                                                <tr
                                                    key={h.id}
                                                    onClick={() => handleSelect(h)}
                                                    className="kd-house-row"
                                                >
                                                    <td className="kd-house-cell kd-house-cell-mono">{h.wtw8_number || '-'}</td>
                                                    <td className="kd-house-cell kd-house-cell-mono kd-house-cell-muted">{h.number || '-'}</td>
                                                    <td className="kd-house-cell">{h.name}</td>
                                                    <td className="kd-house-cell kd-house-cell-nowrap">
                                                        <span className="kd-house-tag">{h.tzqslx || '-'}</span>
                                                    </td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={4} className="kd-house-empty">
                                                    <Search size={18} aria-hidden="true" />
                                                    <div className="kd-house-empty-title">未能找到匹配的房号</div>
                                                    <div className="kd-house-empty-subtitle">请尝试修改关键词后重新搜索</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>

                        <div className="kd-house-dropdown-footer">
                            <div className="kd-house-match-info">匹配到 {total} 条金蝶数据</div>
                            <div className="kd-house-pagination">
                                <button
                                    type="button"
                                    className="kd-house-page-btn"
                                    disabled={page <= 1 || isLoading}
                                    onClick={() => setPage((p) => p - 1)}
                                    aria-label="上一页"
                                    title="上一页"
                                >
                                    <ChevronLeft size={14} />
                                </button>
                                <div className="kd-house-page-indicator">
                                    {page} / {totalPages || 1}
                                </div>
                                <button
                                    type="button"
                                    className="kd-house-page-btn"
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

export default KingdeeHouseSelector;
