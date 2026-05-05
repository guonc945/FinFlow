import { useState, useMemo } from 'react';
import { Search, ChevronRight, ChevronDown, X } from 'lucide-react';
import type { AccountingSubject } from '../../types';
import './SubjectPickerModal.css';

interface SubjectPickerModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSelect: (subject: AccountingSubject) => void;
    subjects: AccountingSubject[];
}

const SubjectPickerModal = ({ isOpen, onClose, onSelect, subjects }: SubjectPickerModalProps) => {
    const [searchTerm, setSearchTerm] = useState('');
    const [expandedKeys, setExpandedKeys] = useState<Set<string>>(new Set());
    const [selectedCategory, setSelectedCategory] = useState<string>('ALL');

    const ACCOUNT_TYPE_MAP: Record<string, string> = {
        '0': '资产',
        '1': '负债',
        '2': '权益',
        '3': '成本',
        '4': '损益',
        '5': '表外',
        '6': '共同',
        '7': '其它',
        // 'A': '预算收入',
        // 'B': '预算支出',
        // 'C': '预算结余'
    };

    const categories = [
        { key: 'ALL', label: '全部' },
        ...Object.entries(ACCOUNT_TYPE_MAP).sort().map(([key, label]) => ({ key, label }))
    ];

    const sortedSubjects = useMemo(() => {
        return [...subjects].sort((a, b) => {
            const valA = a.long_number || a.number;
            const valB = b.long_number || b.number;
            return valA.localeCompare(valB);
        });
    }, [subjects]);

    const filteredSubjects = useMemo(() => {
        if (searchTerm) {
            const lowerTerm = searchTerm.toLowerCase();
            return sortedSubjects.filter(s =>
                s.number.toLowerCase().includes(lowerTerm) ||
                s.name.toLowerCase().includes(lowerTerm) ||
                (s.fullname && s.fullname.toLowerCase().includes(lowerTerm))
            );
        }

        // Category filtering (only applies when not searching)
        let visibleSubjects = sortedSubjects;
        if (selectedCategory !== 'ALL') {
            visibleSubjects = sortedSubjects.filter(s => s.account_type_number === selectedCategory);
            // In category view, we might want to show flat list or tree? 
            // If tree, we need to ensure parents are included or just show matching nodes.
            // For simplicity in category filter, let's show flat list of matching nodes first, 
            // or we try to maintain tree structure but that's complex if parents have different categories.
            // Let's assume user wants to see all 'Assets', regardless of tree. 
            // BUT, if it's a tree view, we usually want to see hierarchy. 
            // However, parents often share the same category.
            // Let's stick to the existing tree logic but filtering the *candidates* first?

            // Actually, usually in accounting software, clicking the category filters the root nodes 
            // or filters all nodes. If we filter all nodes, we break the tree.
            // Let's try listing only matching nodes but keep the tree structure if possible. 
            // If we filter strict by category, we might break parent-child chains if a parent 
            // somehow doesn't have the category (unlikely).

            // Simple approach: Filter strictly by category, then apply tree expansion logic 
            // ONLY if the parent is also in the filtered list.

            // Better approach for Sidebar + Tree:
            // Sidebar is usually a high-level filter.
            // Let's filter the List to only include items of that category.
            // And since we are "picking" a leaf usually, the tree is helper.
            // If category is selected, let's flatten or show headers?
            // Let's keep it simple: Filter the list. If parent is missing, it shows as root?
            // Or just check full chain?

            // Let's try: Filter by category. Then apply tree logic on the *filtered* list.
            // This might mean some items become roots if their parents are filtered out.
            return visibleSubjects;
        }

        // Tree mode logic
        const isParentExpanded = (subject: AccountingSubject): boolean => {
            if (subject.level === 1) return true;

            const index = sortedSubjects.findIndex(s => s.id === subject.id);
            if (index <= 0) return true; // Should not happen if data is consistent

            // Look backward for parent
            for (let i = index - 1; i >= 0; i--) {
                const potentialParent = sortedSubjects[i];
                if (potentialParent.level === subject.level - 1) {
                    // Found parent
                    // Parent must be expanded AND visible itself
                    return expandedKeys.has(potentialParent.id) && isParentExpanded(potentialParent);
                }
            }
            return true; // Fallback
        };

        return sortedSubjects.filter(s => isParentExpanded(s));
        return sortedSubjects.filter(s => isParentExpanded(s));
    }, [sortedSubjects, searchTerm, expandedKeys, selectedCategory]);

    const handleToggleExpand = (id: string, e: React.MouseEvent) => {
        e.stopPropagation();
        const newExpandedKeys = new Set(expandedKeys);
        if (newExpandedKeys.has(id)) {
            newExpandedKeys.delete(id);
        } else {
            newExpandedKeys.add(id);
        }
        setExpandedKeys(newExpandedKeys);
    };

    if (!isOpen) return null;

    return (
        <div className="subject-picker-overlay">
            <div className="subject-picker-modal">
                <div className="picker-header">
                    <h3>选择会计科目</h3>
                    <button onClick={onClose} className="picker-close">
                        <X size={20} />
                    </button>
                </div>
                <div className="picker-body">
                    <div className="picker-sidebar">
                        {categories.map(cat => (
                            <button
                                key={cat.key}
                                className={`sidebar-item ${selectedCategory === cat.key ? 'active' : ''}`}
                                onClick={() => setSelectedCategory(cat.key)}
                            >
                                {cat.label}
                            </button>
                        ))}
                    </div>
                    <div className="picker-main">
                        <div className="picker-search">
                            <Search size={16} className="picker-search-icon" />
                            <input
                                type="text"
                                placeholder="搜索科目编码或名称..."
                                value={searchTerm}
                                onChange={e => setSearchTerm(e.target.value)}
                                autoFocus
                            />
                        </div>
                        <div className="picker-table-container">
                            <table className="picker-table">
                                <thead>
                                    <tr>
                                        <th style={{ width: '8%', textAlign: 'center' }}>序号</th>
                                        <th style={{ width: '25%' }}>科目编码</th>
                                        <th style={{ width: '55%' }}>科目名称</th>
                                        <th style={{ width: '12%', textAlign: 'center' }}>操作</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {filteredSubjects.length > 0 ? (
                                        filteredSubjects.map((subject, index) => (
                                            <tr key={subject.id}>
                                                <td style={{ textAlign: 'center', color: '#94a3b8' }}>{index + 1}</td>
                                                <td>
                                                    <div className="subject-cell" style={{
                                                        paddingLeft: searchTerm ? 0 : (subject.level - 1) * 20
                                                    }}>
                                                        {(!subject.is_leaf && !searchTerm) && (
                                                            <button
                                                                className="picker-expand-btn"
                                                                onClick={(e) => handleToggleExpand(subject.id, e)}
                                                            >
                                                                {expandedKeys.has(subject.id) ?
                                                                    <ChevronDown size={14} /> :
                                                                    <ChevronRight size={14} />
                                                                }
                                                            </button>
                                                        )}
                                                        {(subject.is_leaf && !searchTerm) && <div className="picker-placeholder" />}
                                                        <span className="font-mono font-medium">{subject.number}</span>
                                                    </div>
                                                </td>
                                                <td>
                                                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                                                        <span>{subject.name}</span>
                                                        <span style={{ fontSize: '0.75rem', color: '#94a3b8' }}>{subject.fullname}</span>
                                                    </div>
                                                </td>
                                                <td style={{ textAlign: 'center' }}>
                                                    {subject.is_leaf ? (
                                                        <button
                                                            className="select-btn"
                                                            onClick={() => onSelect(subject)}
                                                        >
                                                            选择
                                                        </button>
                                                    ) : (
                                                        <span className="folder-badge">非末级</span>
                                                    )}
                                                </td>
                                            </tr>
                                        ))
                                    ) : (
                                        <tr>
                                            <td colSpan={3} style={{ textAlign: 'center', padding: '2rem', color: '#94a3b8' }}>
                                                未找到匹配的科目
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default SubjectPickerModal;
