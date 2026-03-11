import React, { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import {
    Search, X, Check, Loader2, FolderTree, BookOpen
} from 'lucide-react';
import axios from 'axios';
import type { AccountingSubject } from '../../types';
import './AccountingSubjectSelector.css';

interface AccountingSubjectSelectorProps {
    value?: string; // 可以是 ID 也可以是名称(取决于具体实现，这里取 ID)
    onSelect: (subject: AccountingSubject | null) => void;
    placeholder?: string;
    label?: string;
    manualInput?: boolean;
}

const ACCOUNT_TYPE_MAP: Record<string, string> = {
    '0': '资产',
    '1': '负债',
    '2': '权益',
    '3': '成本',
    '4': '损益',
    '5': '表外',
    '6': '共同',
    '7': '其它',
    'A': '预算收入',
    'B': '预算支出',
    'C': '预算结余'
};

const AccountingSubjectSelector: React.FC<AccountingSubjectSelectorProps> = ({
    value,
    onSelect,
    placeholder = "请选择会计科目...",
    label,
    manualInput = true
}) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [loading, setLoading] = useState(false);
    const [subjects, setSubjects] = useState<AccountingSubject[]>([]);
    const [, setTotal] = useState(0);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize] = useState(100);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedType, setSelectedType] = useState<string>('all');
    const [inputValue, setInputValue] = useState('');

    // 如果已有 value (ID)，尝试获取其显示名称
    useEffect(() => {
        if (value && !inputValue) {
            // 这种模式下通常需要单独的 API 根据 ID 获取科目名称，
            // 或者父组件直接传入整个对象。
            // 这里暂时假设 input 显示的是 ID 或之后更新。
            setInputValue(value);
        }
    }, [value]);

    useEffect(() => {
        if (isModalOpen) {
            fetchSubjects();
        }
    }, [isModalOpen, currentPage, selectedType]);

    // Search debouncing
    useEffect(() => {
        const timer = setTimeout(() => {
            if (isModalOpen) {
                setCurrentPage(1);
                fetchSubjects();
            }
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    const fetchSubjects = async () => {
        setLoading(true);
        try {
            const skip = (currentPage - 1) * pageSize;
            const res = await axios.get(`${import.meta.env.VITE_API_BASE_URL}/finance/accounting-subjects`, {
                params: {
                    skip: skip,
                    limit: pageSize,
                    search: searchTerm || undefined,
                    account_type: selectedType === 'all' ? undefined : selectedType
                }
            });

            if (res.data && Array.isArray(res.data.items)) {
                setSubjects(res.data.items);
                setTotal(res.data.total);
            }
        } catch (err) {
            console.error("Failed to fetch subjects in selector", err);
        } finally {
            setLoading(false);
        }
    };

    const handleSearchManual = async (text: string) => {
        if (!text) return;
        setLoading(true);
        try {
            // Try to find an exact match by number
            const res = await axios.get(`${import.meta.env.VITE_API_BASE_URL}/finance/accounting-subjects`, {
                params: {
                    search: text,
                    limit: 10
                }
            });
            if (res.data && Array.isArray(res.data.items)) {
                // If we found any, look for exact number match
                const match = res.data.items.find((s: AccountingSubject) => s.number === text || s.name === text);
                if (match && match.is_leaf) {
                    handleSelect(match);
                }
                // If no exact match found, we leave it as is, or maybe open modal
            }
        } catch (err) {
            console.error("Manual search failed", err);
        } finally {
            setLoading(false);
        }
    };

    const handleSelect = (subject: AccountingSubject) => {
        setInputValue(`${subject.number} ${subject.name}`);
        onSelect(subject);
        setIsModalOpen(false);
    };

    const handleClear = () => {
        setInputValue('');
        onSelect(null);
    };

    return (
        <div className="subject-selector-container">
            {label && <label className="selector-label">{label}</label>}
            <div className="selector-input-wrapper">
                <input
                    type="text"
                    className="selector-input"
                    placeholder={placeholder}
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onBlur={() => handleSearchManual(inputValue)}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter') handleSearchManual(inputValue);
                    }}
                    readOnly={!manualInput}
                />
                <div className="selector-actions">
                    {inputValue && (
                        <button className="clear-btn" onClick={handleClear}>
                            <X size={14} />
                        </button>
                    )}
                    <button className="search-trigger-btn" onClick={() => setIsModalOpen(true)}>
                        <Search size={16} />
                    </button>
                </div>
            </div>

            {isModalOpen && createPortal(
                <div className="selector-modal-overlay" onClick={() => setIsModalOpen(false)}>
                    <div className="selector-modal-content animate-scale-in" onClick={e => e.stopPropagation()}>
                        <div className="modal-header">
                            <div className="flex items-center gap-2">
                                <BookOpen size={20} className="text-primary" />
                                <h3 className="text-lg font-bold">选择会计科目</h3>
                            </div>
                            <button className="close-btn" onClick={() => setIsModalOpen(false)}>
                                <X size={20} />
                            </button>
                        </div>

                        <div className="modal-body subject-selector-body">
                            {/* Left Nav */}
                            <div className="subject-type-nav">
                                <div className="nav-title">科目分类</div>
                                <div className="nav-items custom-scrollbar">
                                    <div
                                        className={`nav-item ${selectedType === 'all' ? 'active' : ''}`}
                                        onClick={() => setSelectedType('all')}
                                    >
                                        全部
                                    </div>
                                    {Object.entries(ACCOUNT_TYPE_MAP).map(([key, name]) => (
                                        <div
                                            key={key}
                                            className={`nav-item ${selectedType === key ? 'active' : ''}`}
                                            onClick={() => setSelectedType(key)}
                                        >
                                            {name}
                                        </div>
                                    ))}
                                </div>
                            </div>

                            {/* Main List */}
                            <div className="subject-list-container">
                                <div className="subject-search-bar">
                                    <Search size={16} className="search-icon" />
                                    <input
                                        type="text"
                                        placeholder="搜索编码或名称..."
                                        value={searchTerm}
                                        onChange={e => setSearchTerm(e.target.value)}
                                        autoFocus
                                    />
                                </div>

                                <div className="subject-list-content custom-scrollbar">
                                    {loading ? (
                                        <div className="loading-state">
                                            <Loader2 className="animate-spin text-primary" size={32} />
                                            <span>正在加载会计科目...</span>
                                        </div>
                                    ) : subjects.length > 0 ? (
                                        <table className="selector-table">
                                            <thead>
                                                <tr>
                                                    <th>编码</th>
                                                    <th>名称</th>
                                                    <th>明细</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {subjects.map(s => (
                                                    <tr
                                                        key={s.id}
                                                        className={`selector-row ${!s.is_leaf ? 'not-leaf' : ''}`}
                                                        onClick={() => s.is_leaf && handleSelect(s)}
                                                    >
                                                        <td className="font-mono text-sm">{s.number}</td>
                                                        <td>
                                                            <div className="flex flex-col">
                                                                <span className="font-medium">{s.name}</span>
                                                                <span className="text-xs text-slate-400">{s.fullname}</span>
                                                            </div>
                                                        </td>
                                                        <td>
                                                            {s.is_leaf ? <Check size={14} className="text-green-500" /> : <FolderTree size={14} className="text-blue-400" />}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    ) : (
                                        <div className="empty-state">未找到匹配的科目</div>
                                    )}
                                </div>

                                <div className="selector-footer">
                                    <div className="pagination">
                                        <button
                                            disabled={currentPage === 1}
                                            onClick={() => setCurrentPage(p => p - 1)}
                                        >上一页</button>
                                        <span>第 {currentPage} 页</span>
                                        <button
                                            disabled={subjects.length < pageSize}
                                            onClick={() => setCurrentPage(p => p + 1)}
                                        >下一页</button>
                                    </div>
                                    <div className="hint text-xs text-slate-400">
                                        提示：只能选择明细科目 (叶子节点)
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>, document.body
            )}
        </div>
    );
};

export default AccountingSubjectSelector;
