import { useEffect, useState } from 'react';
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

    useEffect(() => {
        setInputValue(value || '');
    }, [value]);

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearch(searchTerm);
            setPage(1);
        }, 400);
        return () => clearTimeout(timer);
    }, [searchTerm]);

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
    };

    const handleClear = () => {
        setInputValue('');
        onSelect(null);
    };

    const handleManualSearch = async (text: string) => {
        if (!text) return;
        setIsLoading(true);
        try {
            const res = await getTaxRates({ search: text, limit: 10 });
            const match = (res.items || []).find(
                item => item.number === text || item.name === text || `${item.number} ${item.name}` === text,
            );
            if (match) {
                handleSelect(match);
            }
        } catch (error) {
            console.error('Manual tax rate search failed:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const totalPages = Math.ceil(total / pageSize) || 1;

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
                    onBlur={() => handleManualSearch(inputValue)}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter') handleManualSearch(inputValue);
                    }}
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
