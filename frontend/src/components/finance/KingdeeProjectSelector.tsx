import { useState, useEffect } from 'react';
import { Search, MapPin, X, ChevronLeft, ChevronRight, Loader2 } from 'lucide-react';
import { getKingdeeProjects } from '../../services/api';
import type { KingdeeProject } from '../../types';
import './KingdeeProjectSelector.css';

interface KingdeeProjectSelectorProps {
    value?: string; // Display string or ID
    onSelect: (project: KingdeeProject | null) => void;
    label?: string;
    placeholder?: string;
}

const KingdeeProjectSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择金蝶系统管理项目...'
}: KingdeeProjectSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [inputValue, setInputValue] = useState(value || '');

    // Search & Data
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [projects, setProjects] = useState<KingdeeProject[]>([]);
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
        if (isModalOpen) {
            fetchProjects();
        }
    }, [isModalOpen, debouncedSearch, page]);

    const fetchProjects = async () => {
        setIsLoading(true);
        try {
            const res = await getKingdeeProjects({
                search: debouncedSearch,
                skip: (page - 1) * pageSize,
                limit: pageSize
            });
            setProjects(res.items || []);
            setTotal(res.total || 0);
        } catch (error) {
            console.error('Failed to fetch Kingdee projects:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSelect = (project: KingdeeProject) => {
        setInputValue(`${project.number} ${project.name}`);
        onSelect(project);
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
            const res = await getKingdeeProjects({
                search: text,
                limit: 10
            });
            if (res && Array.isArray(res.items)) {
                const match = res.items.find((p: KingdeeProject) => p.number === text || p.name === text);
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
                        <button type="button" className="clear-btn" onClick={handleClear} title="清除映射">
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
                                <MapPin size={20} className="text-primary" />
                                <h3 className="text-lg font-bold">选择金蝶系统管理项目</h3>
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
                                    placeholder="搜索管理项目名称、系统号..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="project-list-wrapper custom-scrollbar">
                                <table className="project-table">
                                    <thead>
                                        <tr>
                                            <th>系统内码</th>
                                            <th>辅助资料名称</th>
                                            <th>所属类别</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading && projects.length === 0 ? (
                                            <tr>
                                                <td colSpan={3}>
                                                    <div className="flex justify-center py-8">
                                                        <Loader2 className="animate-spin text-slate-300" size={24} />
                                                    </div>
                                                </td>
                                            </tr>
                                        ) : projects.length > 0 ? (
                                            projects.map(p => (
                                                <tr key={p.id} onClick={() => handleSelect(p)} className="project-row">
                                                    <td className="font-mono text-primary font-medium">{p.number || '-'}</td>
                                                    <td className="text-slate-700 font-medium">{p.name}</td>
                                                    <td className="text-sm text-slate-500">{p.group_name || '管理项目'}</td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={3}>
                                                    <div className="text-center py-8 text-slate-400">没有查找到对应的金蝶管理项目</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>

                            <div className="modal-footer pt-3 border-t border-slate-100 flex justify-between items-center">
                                <div className="text-xs text-slate-500">
                                    共找到 <span className="font-bold text-slate-700">{total}</span> 个辅助资料
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

export default KingdeeProjectSelector;
