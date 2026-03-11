import { useState, useEffect } from 'react';
import { Search, Book, X, ChevronLeft, ChevronRight, Loader2 } from 'lucide-react';
import { getAccountBooks } from '../../services/api';
import type { KingdeeAccountBookBrief } from '../../types';
import './KingdeeProjectSelector.css'; // 复用相同样式

interface AccountBookItem {
    id: string;
    number: string;
    name: string;
}

interface KingdeeAccountBookSelectorProps {
    value?: string; // 显示文本
    onSelect: (book: KingdeeAccountBookBrief | null) => void;
    label?: string;
    placeholder?: string;
}

const KingdeeAccountBookSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择金蝶核算账簿...'
}: KingdeeAccountBookSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [inputValue, setInputValue] = useState(value || '');

    // 搜索 & 数据
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [books, setBooks] = useState<AccountBookItem[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    // 分页
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
        if (isModalOpen) {
            fetchBooks();
        }
    }, [isModalOpen, debouncedSearch, page]);

    const fetchBooks = async () => {
        setIsLoading(true);
        try {
            const res = await getAccountBooks({
                search: debouncedSearch || undefined,
                skip: (page - 1) * pageSize,
                limit: pageSize
            });
            setBooks(res.items || []);
            setTotal(res.total || 0);
        } catch (error) {
            console.error('获取核算账簿列表失败:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSelect = (book: AccountBookItem) => {
        const brief: KingdeeAccountBookBrief = {
            id: book.id,
            number: book.number,
            name: book.name
        };
        setInputValue(`${book.number} ${book.name}`);
        onSelect(brief);
        setIsModalOpen(false);
    };

    const handleClear = () => {
        setInputValue('');
        onSelect(null);
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
                    readOnly
                    onClick={() => setIsModalOpen(true)}
                    style={{ cursor: 'pointer' }}
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
