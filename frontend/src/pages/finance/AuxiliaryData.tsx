import { useState, useEffect, useMemo, useRef } from 'react';
import {
    RefreshCw, Layers,
    Search, AlertTriangle, X, Filter
    , ChevronDown, ChevronUp
} from 'lucide-react';
import axios from 'axios';
import { useToast, ToastContainer } from '../../components/Toast';
import './AccountingSubjects.css';
import '../bills/Bills.css'; // Reusing the same styling for consistency

interface AuxiliaryData {
    id: string;
    number: string;
    name: string;
    issyspreset: boolean;
    ctrlstrategy: string;
    enable: string;
    group_name?: string;
    parent_name?: string;
    createorg_name?: string;
}

const CTRL_STRATEGY_MAP: Record<string, string> = {
    '7': '私有',
    '5': '全局共享'
};

interface CategoryOption {
    number: string;
    name: string;
}

const AuxiliaryDataPage = () => {
    const [auxData, setAuxData] = useState<AuxiliaryData[]>([]);
    const [categories, setCategories] = useState<CategoryOption[]>([]);
    const [selectedCategories, setSelectedCategories] = useState<string[]>([]);

    const [total, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(100);
    const [loading, setLoading] = useState(false);
    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);
    const { toasts, showToast, removeToast } = useToast();

    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [categorySearch, setCategorySearch] = useState('');
    const dropdownRef = useRef<HTMLDivElement>(null);

    const filteredCategoriesList = useMemo(() => {
        return categories.filter(c =>
            c.name.toLowerCase().includes(categorySearch.toLowerCase()) ||
            c.number.toLowerCase().includes(categorySearch.toLowerCase())
        );
    }, [categories, categorySearch]);

    const toggleCategory = (number: string) => {
        setSelectedCategories(prev =>
            prev.includes(number) ? prev.filter(x => x !== number) : [...prev, number]
        );
    };

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsDropdownOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, []);

    useEffect(() => {
        const fetchAllCategories = async () => {
            try {
                const res = await axios.get(`${import.meta.env.VITE_API_BASE_URL}/finance/auxiliary-data-categories`, {
                    params: { skip: 0, limit: 1000 }
                });
                if (res.data && Array.isArray(res.data.items)) {
                    setCategories(res.data.items.map((c: any) => ({ number: c.number, name: c.name })));
                }
            } catch (err) {
                console.error("Failed to load categories", err);
            }
        };
        fetchAllCategories();
    }, []);

    useEffect(() => {
        fetchAuxData();
    }, [currentPage, pageSize]);

    // Re-fetch when search or categories changes, reset to page 1
    useEffect(() => {
        const timer = setTimeout(() => {
            setCurrentPage(1);
            fetchAuxData();
        }, 300);
        return () => clearTimeout(timer);
    }, [searchTerm, selectedCategories]);

    const fetchAuxData = async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await axios.get(`${import.meta.env.VITE_API_BASE_URL}/finance/auxiliary-data`, {
                params: {
                    skip: skip,
                    limit: pageSize,
                    search: searchTerm || undefined,
                    categories: selectedCategories.length > 0 ? selectedCategories.join(',') : undefined
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                setAuxData(res.data.items);
                setTotal(res.data.total);
            } else {
                setAuxData([]);
                setTotal(0);
            }
        } catch (err) {
            console.error(err);
            showToast('error', '加载失败', '无法获取辅助资料列表');
        } finally {
            setLoading(false);
        }
    };

    const handleSync = async () => {
        setIsConfirmModalOpen(false);
        setLoading(true);
        try {
            await axios.post(`${import.meta.env.VITE_API_BASE_URL}/finance/auxiliary-data/sync`, {
                categories: selectedCategories.length > 0 ? selectedCategories : undefined
            });
            showToast('success', '同步任务已提交', '系统正在后台处理数据同步，请在 1-2 分钟后尝试刷新列表。');
            setTimeout(fetchAuxData, 3000);
        } catch (err: any) {
            showToast('error', '同步异常', err.response?.data?.error || err.response?.data?.detail || err.message || '由于网络或授权原因，同步请求未能成功发送');
        } finally {
            setLoading(false);
        }
    };

    const totalPages = Math.ceil(total / pageSize);

    return (
        <div className="page-container fade-in">
            <div className={`bills-filter-section ${isFilterCollapsed ? 'collapsed' : ''}`}>
                <div className="filter-header-row">
                    <div className="flex items-center gap-2">
                        <Filter size={16} className="text-primary" />
                        <h4 className="text-sm font-semibold">辅助核算资料同步与筛选</h4>
                    </div>
                    <button className="collapse-toggle" onClick={() => setIsFilterCollapsed(!isFilterCollapsed)}>
                        {isFilterCollapsed ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                    </button>
                </div>

                {!isFilterCollapsed && (
                    <div className="filter-content-wrapper fade-in">
                        <div className="selection-row">
                            <div className="selection-group" ref={dropdownRef}>
                                <div className={`custom-select-trigger ${isDropdownOpen ? 'active' : ''}`} onClick={() => setIsDropdownOpen(!isDropdownOpen)}>
                                    <div className="trigger-content">
                                        <Filter size={14} />
                                        <span className={selectedCategories.length === 0 ? 'placeholder' : ''}>
                                            {selectedCategories.length === 0 ? '选择需要同步的分类...' : `已选 ${selectedCategories.length} 个分类`}
                                        </span>
                                    </div>
                                    <ChevronDown size={14} className={`arrow ${isDropdownOpen ? 'rotate' : ''}`} />
                                </div>
                                {isDropdownOpen && (
                                    <div className="custom-dropdown card-shadow slide-up">
                                        <div className="p-2 border-b border-gray-100 flex items-center gap-2">
                                            <Search size={14} className="text-tertiary" />
                                            <input autoFocus type="text" placeholder="搜索分类..." className="dropdown-search" value={categorySearch} onChange={(e) => setCategorySearch(e.target.value)} onClick={(e) => e.stopPropagation()} />
                                        </div>
                                        <div className="p-1 flex justify-between bg-gray-50/50">
                                            <button className="btn-text text-xs" onClick={() => setSelectedCategories(categories.map(c => c.number))}>全选</button>
                                            <button className="btn-text text-xs" onClick={() => setSelectedCategories([])}>清空</button>
                                        </div>
                                        <div className="dropdown-list custom-scrollbar">
                                            {filteredCategoriesList.map(c => (
                                                <div key={c.number} className={`dropdown-item ${selectedCategories.includes(c.number) ? 'selected' : ''}`} onClick={(e) => { e.stopPropagation(); toggleCategory(c.number); }}>
                                                    <div className="checkbox">{selectedCategories.includes(c.number) && <div className="check-dot"></div>}</div>
                                                    <div className="item-info"><span className="name">{c.name}</span> <span className="text-xs text-secondary ml-2">{c.number}</span></div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                            <div className="chips-container">
                                {selectedCategories.slice(0, 3).map(id => (
                                    <div key={id} className="selected-chip">
                                        <span>{categories.find(c => c.number === id)?.name || id}</span>
                                        <button onClick={(e) => { e.stopPropagation(); toggleCategory(id); }}><X size={10} /></button>
                                    </div>
                                ))}
                                {selectedCategories.length > 3 && <span className="text-xs text-secondary">+{selectedCategories.length - 3}</span>}
                            </div>
                            <button className={`btn-primary ${selectedCategories.length === 0 ? 'disabled' : ''}`} onClick={handleSync} disabled={selectedCategories.length === 0 || loading}>
                                <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
                                {loading ? '同步中...' : '同步指定分类'}
                            </button>
                        </div>

                        <div className="action-row">
                            <div className="search-group flex-1">
                                <Search size={14} className="search-icon" />
                                <input
                                    type="text"
                                    placeholder="搜索辅助资料编码或名称..."
                                    value={searchTerm}
                                    onChange={e => setSearchTerm(e.target.value)}
                                />
                            </div>
                            <div className="divider-v"></div>
                            <button className="btn-outline" onClick={() => { setCurrentPage(1); fetchAuxData(); }}>
                                <RefreshCw size={14} className={loading ? "animate-spin" : ""} /> 刷新列表
                            </button>
                        </div>
                    </div>
                )}
            </div>

            <div className="table-area-wrapper">
                <div className="table-container custom-scrollbar">
                    <table className="data-table min-w-max w-full text-left border-collapse">
                        <thead className="table-header bg-slate-50/80 backdrop-blur-sm border-b border-slate-200">
                            <tr>
                                <th className="w-16 py-4 px-6 font-semibold text-slate-600 text-center">序号</th>
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600">编码</th>
                                <th className="w-48 py-4 px-6 font-semibold text-slate-600">名称</th>
                                <th className="w-40 py-4 px-6 font-semibold text-slate-600">所属类别</th>
                                <th className="w-48 py-4 px-6 font-semibold text-slate-600">上级名称</th>
                                <th className="w-40 py-4 px-6 font-semibold text-slate-600">创建组织</th>
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600 text-center">系统预置</th>
                                <th className="w-32 py-4 px-6 font-semibold text-slate-600 text-center">控制策略</th>
                                <th className="w-24 py-4 px-6 font-semibold text-slate-600 text-center">使用状态</th>
                            </tr>
                        </thead>
                        <tbody className="table-body">
                            {auxData.length > 0 ? (
                                auxData.map((item, index) => (
                                    <tr key={item.id} className="table-row hover:bg-slate-50/50 transition-colors border-b border-slate-50 group">
                                        <td className="table-cell py-3 px-6 text-center font-medium text-slate-400">
                                            {(currentPage - 1) * pageSize + index + 1}
                                        </td>
                                        <td className="table-cell py-3 px-6 font-mono text-slate-500 text-sm">
                                            {item.number}
                                        </td>
                                        <td className="table-cell py-3 px-6 font-medium text-slate-800">
                                            {item.name}
                                        </td>
                                        <td className="table-cell py-3 px-6 text-slate-600">
                                            <span className="bg-slate-100 text-slate-600 px-2.5 py-1 rounded-md text-xs font-medium border border-slate-200">
                                                {item.group_name || '无类别'}
                                            </span>
                                        </td>
                                        <td className="table-cell py-3 px-6 text-slate-600 text-sm">
                                            {item.parent_name || '-'}
                                        </td>
                                        <td className="table-cell py-3 px-6 text-slate-600 text-sm">
                                            {item.createorg_name || '-'}
                                        </td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            {item.issyspreset ?
                                                <span className="text-xs font-medium text-amber-600 bg-amber-50 px-2.5 py-1 rounded-full border border-amber-200">预置</span> :
                                                <span className="text-xs font-medium text-slate-500 bg-slate-50 px-2.5 py-1 rounded-full border border-slate-200">自定义</span>
                                            }
                                        </td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            <span className={`px-2.5 py-1 rounded-md text-xs font-medium ${item.ctrlstrategy === '5' ? 'bg-sky-50 text-sky-600 border border-sky-200' : 'bg-fuchsia-50 text-fuchsia-600 border border-fuchsia-200'}`}>
                                                {CTRL_STRATEGY_MAP[item.ctrlstrategy] || item.ctrlstrategy || '未知'}
                                            </span>
                                        </td>
                                        <td className="table-cell py-3 px-6 text-center">
                                            {item.enable === '1' ?
                                                <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-50 text-emerald-700 border border-emerald-200">
                                                    <span className="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>启用
                                                </span> :
                                                <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-rose-50 text-rose-700 border border-rose-200">
                                                    <span className="w-1.5 h-1.5 rounded-full bg-rose-500"></span>禁用
                                                </span>
                                            }
                                        </td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan={9} className="empty-state py-16 text-center text-slate-500 bg-slate-50/30">
                                        {loading ? (
                                            <div className="flex flex-col items-center justify-center gap-3">
                                                <RefreshCw size={24} className="animate-spin text-indigo-400" />
                                                <span>数据加载中...</span>
                                            </div>
                                        ) : (
                                            <div className="flex flex-col items-center justify-center gap-3">
                                                <Layers size={32} className="text-slate-300" />
                                                <span>暂无辅助资料数据，请尝试一键同步</span>
                                            </div>
                                        )}
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>

                <div className="pagination-footer p-4 bg-white/60 border-t border-slate-100 flex items-center justify-between text-sm">
                    <div className="pagination-info text-slate-600">
                        共 <span className="text-indigo-600 font-bold mx-1">{total}</span> 条记录
                        <span className="mx-3 text-slate-300">|</span>
                        当前第 {currentPage} 页 / 共 {totalPages || 1} 页
                    </div>
                    <div className="pagination-controls flex items-center gap-4">
                        <div className="page-size-selector flex items-center gap-2 text-slate-600">
                            <span>每页显示:</span>
                            <select
                                value={pageSize}
                                onChange={e => {
                                    setPageSize(Number(e.target.value));
                                    setCurrentPage(1);
                                }}
                                className="page-size-select bg-white border border-slate-200 rounded px-2 py-1 outline-none focus:ring-1 focus:ring-indigo-500"
                            >
                                <option value={20}>20</option>
                                <option value={50}>50</option>
                                <option value={100}>100</option>
                                <option value={500}>500</option>
                            </select>
                        </div>
                        <div className="page-buttons flex items-center gap-2">
                            <button
                                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                                disabled={currentPage === 1 || loading}
                                className="page-btn px-3 py-1.5 rounded border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-indigo-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                            >
                                上一页
                            </button>
                            <div className="current-page-display bg-indigo-50 text-indigo-700 w-8 h-8 rounded flex items-center justify-center font-medium border border-indigo-100">
                                {currentPage}
                            </div>
                            <button
                                onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                                disabled={currentPage >= totalPages || loading}
                                className="page-btn px-3 py-1.5 rounded border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-indigo-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                            >
                                下一页
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            <ToastContainer toasts={toasts} removeToast={removeToast} />

            {/* Confirm Modal */}
            {isConfirmModalOpen && (
                <div className="modal-overlay fixed inset-0 bg-slate-900/40 backdrop-blur-sm z-50 flex items-center justify-center p-4">
                    <div className="modal-content bg-white w-full max-w-md rounded-2xl shadow-xl overflow-hidden animate-fade-in border border-slate-100">
                        <div className="modal-header p-6 pb-0 flex items-start justify-between">
                            <div className="confirm-icon-wrapper w-12 h-12 rounded-full bg-amber-50 flex items-center justify-center text-amber-500 shrink-0">
                                <AlertTriangle size={24} />
                            </div>
                            <button className="modal-close text-slate-400 hover:text-slate-600 transition-colors p-1" onClick={() => setIsConfirmModalOpen(false)}>
                                <X size={20} />
                            </button>
                        </div>
                        <div className="modal-body p-6 pt-4">
                            <h2 className="modal-title font-bold text-lg text-slate-800 mb-2">同步辅助资料</h2>
                            <p className="modal-desc text-slate-600 text-sm leading-relaxed">
                                {selectedCategories.length > 0
                                    ? `确定要从金蝶星空系统拉取所选的 ${selectedCategories.length} 个辅助资料分类下的数据吗？这将会请求接口并在后台更新所选类别的数据。`
                                    : '确定要从金蝶星空系统拉取通用的辅助资料信息吗？这将会请求接口并在后台更新全量数据。'}
                            </p>
                        </div>
                        <div className="modal-footer p-4 bg-slate-50 border-t border-slate-100 flex items-center justify-end gap-3 rounded-b-2xl">
                            <button className="btn-cancel px-4 py-2 font-medium text-slate-600 bg-white border border-slate-200 rounded-lg hover:bg-slate-50 transition-colors" onClick={() => setIsConfirmModalOpen(false)}>取消返回</button>
                            <button className="btn-confirm px-4 py-2 font-medium text-white bg-indigo-600 rounded-lg hover:bg-indigo-700 shadow-sm shadow-indigo-200 transition-colors" onClick={handleSync}>确定同步</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default AuxiliaryDataPage;
