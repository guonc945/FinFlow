import { useCallback, useEffect, useMemo, useState } from 'react';
import { Search, MapPin, X, Loader2, ChevronRight, FolderTree } from 'lucide-react';
import { getKingdeeProjects } from '../../services/api';
import type { KingdeeProject } from '../../types';
import './KingdeeProjectSelector.css';

interface KingdeeProjectSelectorProps {
    value?: string;
    onSelect: (project: KingdeeProject | null) => void;
    label?: string;
    placeholder?: string;
}

interface TreeNode extends KingdeeProject {
    children: TreeNode[];
}

const QUICK_PICK_LIMIT = 8;
const MODAL_FETCH_PAGE_SIZE = 500;
const MODAL_FETCH_MAX = 5000;

const formatProjectLabel = (project: Pick<KingdeeProject, 'number' | 'name'>) =>
    [project.number, project.name].filter(Boolean).join(' ').trim();

const buildProjectNumberMap = (items: KingdeeProject[]) => {
    const numberMap = new Map<string, KingdeeProject>();
    items.forEach((item) => {
        if (item.number) {
            numberMap.set(item.number, item);
        }
    });
    return numberMap;
};

const getProjectPathSegments = (project: KingdeeProject, numberMap: Map<string, KingdeeProject>) => {
    const segments: string[] = [];
    const visited = new Set<string>();
    let current: KingdeeProject | undefined = project;

    while (current && current.number && !visited.has(current.number)) {
        visited.add(current.number);
        if (current.name) {
            segments.unshift(current.name);
        }
        const parentNumber = (current.parent_number || '').trim();
        current = parentNumber ? numberMap.get(parentNumber) : undefined;
    }

    if (segments.length === 0 && project.name) {
        segments.push(project.name);
    }

    return segments;
};

const getProjectFullPath = (project: KingdeeProject, numberMap: Map<string, KingdeeProject>) => {
    const pathText = getProjectPathSegments(project, numberMap).join(' / ');
    return [project.number, pathText].filter(Boolean).join(' ').trim();
};

const buildSearchText = (project: KingdeeProject, numberMap: Map<string, KingdeeProject>) =>
    [
        project.number,
        project.name,
        project.group_name,
        project.parent_name,
        project.parent_number,
        formatProjectLabel(project),
        getProjectFullPath(project, numberMap),
        getProjectPathSegments(project, numberMap).join(' '),
    ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();

const buildTree = (items: KingdeeProject[]) => {
    const nodeMap = new Map<string, TreeNode>();
    items.forEach((item) => {
        nodeMap.set(item.id, { ...item, children: [] });
    });

    const numberMap = new Map<string, TreeNode>();
    nodeMap.forEach((node) => {
        if (node.number) {
            numberMap.set(node.number, node);
        }
    });

    const roots: TreeNode[] = [];
    nodeMap.forEach((node) => {
        const parentNumber = (node.parent_number || '').trim();
        const parent = parentNumber ? numberMap.get(parentNumber) : undefined;
        if (parent && parent.id !== node.id) {
            parent.children.push(node);
        } else {
            roots.push(node);
        }
    });

    const sortNodes = (nodes: TreeNode[]) => {
        nodes.sort((a, b) => {
            const byNumber = (a.number || '').localeCompare(b.number || '', 'zh-CN');
            if (byNumber !== 0) return byNumber;
            return (a.name || '').localeCompare(b.name || '', 'zh-CN');
        });
        nodes.forEach((node) => sortNodes(node.children));
    };

    sortNodes(roots);
    return roots;
};

const filterTree = (
    nodes: TreeNode[],
    keyword: string,
    numberMap: Map<string, KingdeeProject>
): TreeNode[] => {
    if (!keyword) return nodes;

    return nodes
        .map((node) => {
            const children = filterTree(node.children, keyword, numberMap);
            const matches = buildSearchText(node, numberMap).includes(keyword);
            if (!matches && children.length === 0) return null;
            return { ...node, children };
        })
        .filter((node): node is TreeNode => Boolean(node));
};

const flattenTree = (nodes: TreeNode[], expandedIds: Set<string>, depth: number = 0) => {
    const rows: Array<TreeNode & { depth: number }> = [];
    nodes.forEach((node) => {
        rows.push({ ...node, depth });
        if (node.children.length > 0 && expandedIds.has(node.id)) {
            rows.push(...flattenTree(node.children, expandedIds, depth + 1));
        }
    });
    return rows;
};

const findExactProject = (
    items: KingdeeProject[],
    text: string,
    numberMap: Map<string, KingdeeProject>
) => {
    const normalized = text.trim().toLowerCase();
    if (!normalized) return null;
    return (
        items.find((item) => {
            const fullLabel = getProjectFullPath(item, numberMap).toLowerCase();
            const pathOnly = getProjectPathSegments(item, numberMap).join(' / ').toLowerCase();
            return (
                (item.number || '').toLowerCase() === normalized ||
                (item.name || '').toLowerCase() === normalized ||
                formatProjectLabel(item).toLowerCase() === normalized ||
                fullLabel === normalized ||
                pathOnly === normalized
            );
        }) || null
    );
};

const KingdeeProjectSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择金蝶系统管理项目...'
}: KingdeeProjectSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [inputValue, setInputValue] = useState(value || '');
    const [isInputFocused, setIsInputFocused] = useState(false);

    const [quickSearchTerm, setQuickSearchTerm] = useState('');
    const [quickMatches, setQuickMatches] = useState<KingdeeProject[]>([]);
    const [isQuickLoading, setIsQuickLoading] = useState(false);
    const [highlightedIndex, setHighlightedIndex] = useState(0);

    const [searchTerm, setSearchTerm] = useState('');
    const [allProjects, setAllProjects] = useState<KingdeeProject[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [hasLoadedAll, setHasLoadedAll] = useState(false);
    const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());

    useEffect(() => {
        setInputValue(value || '');
    }, [value]);

    useEffect(() => {
        const timer = window.setTimeout(() => {
            setQuickSearchTerm(inputValue.trim());
        }, 250);
        return () => window.clearTimeout(timer);
    }, [inputValue]);

    useEffect(() => {
        if ((isModalOpen || isInputFocused) && !hasLoadedAll) {
            void fetchAllProjects();
        }
    }, [isModalOpen, isInputFocused, hasLoadedAll]);

    const fetchAllProjects = async () => {
        setIsLoading(true);
        try {
            const collected: KingdeeProject[] = [];
            let skip = 0;
            let total = Number.POSITIVE_INFINITY;

            while (skip < total && skip < MODAL_FETCH_MAX) {
                const res = await getKingdeeProjects({
                    skip,
                    limit: MODAL_FETCH_PAGE_SIZE
                });
                const items = Array.isArray(res?.items) ? res.items : [];
                total = typeof res?.total === 'number' ? res.total : items.length;
                collected.push(...items);
                if (items.length < MODAL_FETCH_PAGE_SIZE) break;
                skip += MODAL_FETCH_PAGE_SIZE;
            }

            setAllProjects(collected);
            setHasLoadedAll(true);

            const nextExpandedIds = new Set<string>();
            const treeRoots = buildTree(collected);
            treeRoots.forEach(function expandNode(node) {
                if (node.children.length > 0) {
                    nextExpandedIds.add(node.id);
                    node.children.forEach(expandNode);
                }
            });
            setExpandedIds(nextExpandedIds);
        } catch (error) {
            console.error('Failed to fetch Kingdee projects:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const numberMap = useMemo(() => buildProjectNumberMap(allProjects), [allProjects]);
    const decorateProject = useCallback(
        (project: KingdeeProject): KingdeeProject => ({
            ...project,
            full_path: getProjectFullPath(project, numberMap),
        }),
        [numberMap]
    );

    const handleSelect = (project: KingdeeProject) => {
        const decoratedProject = decorateProject(project);
        setInputValue(decoratedProject.full_path || formatProjectLabel(project));
        setQuickMatches([]);
        setHighlightedIndex(0);
        onSelect(decoratedProject);
        setIsModalOpen(false);
    };

    const handleClear = () => {
        setInputValue('');
        setQuickMatches([]);
        setHighlightedIndex(0);
        onSelect(null);
    };

    const commitManualInput = async () => {
        const text = inputValue.trim();
        if (!text) {
            onSelect(null);
            return;
        }

        const localExact = findExactProject(allProjects, text, numberMap);
        if (localExact) {
            handleSelect(localExact);
            return;
        }

        try {
            const res = await getKingdeeProjects({
                search: text,
                limit: 50
            });
            const items = Array.isArray(res?.items) ? res.items : [];
            const mergedItems = [...allProjects];
            items.forEach((item: KingdeeProject) => {
                if (!mergedItems.some((existing) => existing.id === item.id)) {
                    mergedItems.push(item);
                }
            });
            const mergedMap = buildProjectNumberMap(mergedItems);
            const exact = findExactProject(mergedItems, text, mergedMap);
            if (exact) {
                setAllProjects(mergedItems);
                handleSelect(exact);
                return;
            }
        } catch (error) {
            console.error('Manual Kingdee project search failed', error);
        }

        setInputValue(value || '');
    };

    const handleInputBlur = () => {
        window.setTimeout(() => {
            setIsInputFocused(false);
            void commitManualInput();
        }, 120);
    };

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

        if (!hasLoadedAll) {
            setIsQuickLoading(true);
            return;
        }

        const keyword = quickSearchTerm.toLowerCase();
        const matches = allProjects
            .filter((project) => buildSearchText(project, numberMap).includes(keyword))
            .slice(0, QUICK_PICK_LIMIT)
            .map(decorateProject);

        setQuickMatches(matches);
        setHighlightedIndex(0);
        setIsQuickLoading(false);
    }, [isInputFocused, quickSearchTerm, hasLoadedAll, allProjects, numberMap, decorateProject]);

    const roots = useMemo(() => buildTree(allProjects), [allProjects]);
    const filteredRoots = useMemo(
        () => filterTree(roots, searchTerm.trim().toLowerCase(), numberMap),
        [roots, searchTerm, numberMap]
    );
    const visibleRows = useMemo(
        () => flattenTree(filteredRoots, expandedIds),
        [filteredRoots, expandedIds]
    );

    const toggleExpanded = (projectId: string) => {
        setExpandedIds((prev) => {
            const next = new Set(prev);
            if (next.has(projectId)) {
                next.delete(projectId);
            } else {
                next.add(projectId);
            }
            return next;
        });
    };

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
                    onBlur={handleInputBlur}
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
                        <button type="button" className="clear-btn" onClick={handleClear} title="清除映射">
                            <X size={14} />
                        </button>
                    )}
                    <button type="button" className="search-trigger-btn" onClick={() => setIsModalOpen(true)}>
                        <Search size={14} />
                    </button>
                </div>

                {isInputFocused && (quickSearchTerm || quickMatches.length > 0) && (
                    <div className="suggestions-dropdown quick-selector-dropdown">
                        {isQuickLoading ? (
                            <div className="quick-selector-empty">
                                <Loader2 className="animate-spin" size={14} />
                                <span>正在匹配管理项目...</span>
                            </div>
                        ) : quickMatches.length > 0 ? (
                            quickMatches.map((project, index) => (
                                <button
                                    key={project.id}
                                    type="button"
                                    className={`suggestion-item quick-selector-item ${highlightedIndex === index ? 'highlighted' : ''}`}
                                    onMouseDown={(e) => e.preventDefault()}
                                    onClick={() => handleSelect(project)}
                                >
                                    <div className="quick-selector-main">
                                        <span className="quick-selector-code">{project.number || '-'}</span>
                                        <span className="quick-selector-name">{project.full_path || project.name}</span>
                                    </div>
                                    <div className="quick-selector-meta">
                                        {getProjectPathSegments(project, numberMap).join(' / ') || project.group_name || '管理项目'}
                                    </div>
                                </button>
                            ))
                        ) : (
                            <div className="quick-selector-empty">没有找到匹配的管理项目</div>
                        )}
                    </div>
                )}
            </div>

            {isModalOpen && (
                <div className="project-modal-overlay" onClick={() => setIsModalOpen(false)}>
                    <div className="project-modal-content animate-scale-in" onClick={(e) => e.stopPropagation()}>
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
                                    placeholder="搜索管理项目名称、编码、上级项目..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="project-list-wrapper custom-scrollbar">
                                <table className="project-table project-tree-table">
                                    <thead>
                                        <tr>
                                            <th>系统内码</th>
                                            <th>管理项目层级</th>
                                            <th>所属类别</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading ? (
                                            <tr>
                                                <td colSpan={3}>
                                                    <div className="flex justify-center py-8">
                                                        <Loader2 className="animate-spin text-slate-300" size={24} />
                                                    </div>
                                                </td>
                                            </tr>
                                        ) : visibleRows.length > 0 ? (
                                            visibleRows.map((project) => {
                                                const hasChildren = project.children.length > 0;
                                                const isExpanded = expandedIds.has(project.id);
                                                return (
                                                    <tr key={project.id} onClick={() => handleSelect(project)} className="project-row">
                                                        <td className="font-mono text-primary font-medium">{project.number || '-'}</td>
                                                        <td>
                                                            <div
                                                                className="project-tree-cell"
                                                                style={{ paddingLeft: `${project.depth * 20}px` }}
                                                            >
                                                                <button
                                                                    type="button"
                                                                    className={`tree-toggle-btn ${hasChildren ? '' : 'empty'}`}
                                                                    onClick={(event) => {
                                                                        event.stopPropagation();
                                                                        if (hasChildren) {
                                                                            toggleExpanded(project.id);
                                                                        }
                                                                    }}
                                                                    aria-label={hasChildren ? (isExpanded ? '折叠子级' : '展开子级') : '无子级'}
                                                                >
                                                                    {hasChildren ? (
                                                                        <ChevronRight
                                                                            size={14}
                                                                            className={`tree-toggle-icon ${isExpanded ? 'expanded' : ''}`}
                                                                        />
                                                                    ) : (
                                                                        <FolderTree size={12} className="tree-leaf-icon" />
                                                                    )}
                                                                </button>
                                                                <div className="project-tree-content">
                                                                    <span className="project-tree-name">{project.name}</span>
                                                                    <span className="project-tree-meta">
                                                                        {getProjectPathSegments(project, numberMap).join(' / ')}
                                                                    </span>
                                                                </div>
                                                            </div>
                                                        </td>
                                                        <td className="text-sm text-slate-500">{project.group_name || '管理项目'}</td>
                                                    </tr>
                                                );
                                            })
                                        ) : (
                                            <tr>
                                                <td colSpan={3}>
                                                    <div className="text-center py-8 text-slate-400">没有找到对应的金蝶管理项目</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>

                            <div className="modal-footer pt-3 border-t border-slate-100 flex justify-between items-center">
                                <div className="text-xs text-slate-500">
                                    当前显示 <span className="font-bold text-slate-700">{visibleRows.length}</span> /{' '}
                                    <span className="font-bold text-slate-700">{allProjects.length}</span> 个管理项目
                                </div>
                                <div className="text-xs text-slate-500">
                                    完整路径按级次展开显示，可直接点击任一节点完成映射
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
