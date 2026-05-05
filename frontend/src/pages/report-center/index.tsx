import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
    BarChart3,
    ChevronDown,
    ChevronLeft,
    ChevronRight,
    ChevronUp,
    Columns,
    Download,
    FileSpreadsheet,
    Filter,
    FolderOpen,
    LayoutGrid,
    List,
    Play,
    RefreshCw,
    Search,
    Table2,
    X,
} from 'lucide-react';
import {
    BarChart,
    Bar,
    LineChart,
    Line,
    PieChart,
    Pie,
    Cell,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from 'recharts';
import {
    getBusinessDictionaryItems,
    getReportingReports,
    getReportingReportCategoriesTree,
    runReportingReport,
    getDataDictionaryItems,
} from '../../services/api';
import type {
    DataDictionaryItem,
    QueryResult,
    Report,
    ReportCategory,
    ReportConfig,
    TableStyleConfig,
} from '../data-center/types';
import {
    buildChartData,
    buildCsvContent,
    buildFilterDefaults,
    formatValueByColumnConfig,
    getColumnStyleForValue,
    normalizeFilter,
    parseJson,
    sanitizeReportColumns,
} from '../data-center/utils';
import '../data-center/DataCenter.css';

const CHART_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];

type ReportDictionaryItemLike = {
    code?: string | number | null;
    label?: string | number | null;
    value?: string | number | null;
    path?: string | null;
};

type ApiErrorLike = {
    response?: {
        data?: {
            detail?: string;
        };
    };
    message?: string;
};

const toReportDictionaryItems = (items: ReportDictionaryItemLike[]): DataDictionaryItem[] =>
    (items || []).map((item) => ({
        key: String(item.code || ''),
        label: String(item.label || ''),
        value: item.value == null ? null : String(item.value),
        path: item.path == null ? null : String(item.path),
        raw: null,
    }));

function getReportTypeLabel(type: string) {
    if (type === 'summary') return '汇总应用';
    if (type === 'table') return '明细应用';
    return type;
}

export default function ReportCenterPage() {
    const navigate = useNavigate();
    const { id: routeId } = useParams<{ id: string }>();

    // 是否为全屏独立查看模式（从新标签页打开，不包含侧边栏）
    const isStandalone = window.location.pathname.startsWith('/report-viewer');

    const [reports, setReports] = useState<Report[]>([]);
    const [categories, setCategories] = useState<ReportCategory[]>([]);
    const [expandedCategoryIds, setExpandedCategoryIds] = useState<Set<number>>(new Set());
    const [selectedCategoryId, setSelectedCategoryId] = useState<number | null>(null);
    const [loading, setLoading] = useState(true);
    const [searchText, setSearchText] = useState('');
    const [viewMode, setViewMode] = useState<'card' | 'list'>('card');

    const loadReports = useCallback(async () => {
        setLoading(true);
        try {
            const [data, treeData] = await Promise.all([
                getReportingReports(),
                getReportingReportCategoriesTree(),
            ]);
            setReports(data || []);
            setCategories(treeData || []);
            // auto-expand all
            const allIds = new Set<number>();
            const collect = (items: ReportCategory[]) => {
                for (const item of items) {
                    allIds.add(item.id);
                    if (item.children?.length) collect(item.children);
                }
            };
            collect(treeData || []);
            setExpandedCategoryIds(allIds);
        } catch {
            setReports([]);
            setCategories([]);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        void loadReports();
    }, [loadReports]);

    // 查找当前路由对应的 report
    const currentReport = useMemo(() => {
        if (!routeId) return null;
        return reports.find((r) => r.id === Number(routeId)) || null;
    }, [routeId, reports]);

    // 独立模式下设置浏览器标签页标题
    useLayoutEffect(() => {
        if (isStandalone && currentReport?.name) {
            document.title = `${currentReport.name} - FinFlow`;
        }
    }, [isStandalone, currentReport?.name]);

    const activeReports = useMemo(
        () => reports.filter((r) => r.is_active),
        [reports]
    );

    const filteredReports = useMemo(() => {
        let result = activeReports;
        if (selectedCategoryId !== null) {
            result = result.filter((item) => item.category_id === selectedCategoryId);
        }
        const keyword = searchText.trim().toLowerCase();
        if (keyword) {
            result = result.filter((item) =>
                [item.name, item.dataset_name, item.description, item.category_name]
                    .filter(Boolean)
                    .some((field) => String(field).toLowerCase().includes(keyword))
            );
        }
        return result;
    }, [activeReports, selectedCategoryId, searchText]);

    if (routeId) {
        if (currentReport) {
            return (
                <ReportViewer
                    report={currentReport}
                    onBack={isStandalone ? () => window.close() : () => navigate('/report-center')}
                    isStandalone={isStandalone}
                />
            );
        }
        // 独立模式下，数据加载中时显示占位
        if (isStandalone) {
            return (
                <div className="page-container fade-in report-viewer-page standalone-viewer">
                    <div className="report-viewer-header">
                        <div className="report-viewer-title">
                            <h2>加载中...</h2>
                        </div>
                    </div>
                    <div className="report-viewer-table-wrapper">
                        <div className="empty-box">正在加载数据应用...</div>
                    </div>
                </div>
            );
        }
    }

    const toggleCategoryExpand = (id: number) => {
        setExpandedCategoryIds((prev) => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    const renderCategoryTreeItem = (item: ReportCategory, depth: number) => {
        const hasChildren = item.children && item.children.length > 0;
        const isExpanded = expandedCategoryIds.has(item.id);
        const isSelected = selectedCategoryId === item.id;
        const reportCount = activeReports.filter((r) => r.category_id === item.id).length;

        return (
            <div key={item.id}>
                <div
                    className={`report-center-nav-item${isSelected ? ' active' : ''}`}
                    style={{ paddingLeft: `${depth * 16 + 8}px` }}
                    onClick={() => setSelectedCategoryId(item.id)}
                >
                    <span
                        className="report-center-nav-toggle"
                        onClick={(e) => {
                            e.stopPropagation();
                            if (hasChildren) toggleCategoryExpand(item.id);
                        }}
                    >
                        {hasChildren ? (isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />) : <span style={{ width: 12, display: 'inline-block' }} />}
                    </span>
                    <FolderOpen size={14} />
                    <span className="report-center-nav-name">{item.name}</span>
                    <span className="report-center-nav-count">{reportCount}</span>
                </div>
                {hasChildren && isExpanded && item.children!.map((child) => renderCategoryTreeItem(child, depth + 1))}
            </div>
        );
    };

    return (
        <div className="page-container fade-in report-center-page">
            <div className="report-center-main">
                {/* 左侧分类导航树 */}
                <div className="report-center-sidebar">
                    <div className="report-center-sidebar-head">
                        <strong>应用分类</strong>
                    </div>
                    {loading ? (
                        <div className="empty-box small">加载中...</div>
                    ) : categories.length ? (
                        <div className="report-center-nav-tree">
                            <div
                                className={`report-center-nav-item${selectedCategoryId === null ? ' active' : ''}`}
                                onClick={() => setSelectedCategoryId(null)}
                            >
                                <span style={{ width: 12, display: 'inline-block' }} />
                                <FolderOpen size={14} />
                                <span className="report-center-nav-name">全部分类</span>
                                <span className="report-center-nav-count">{activeReports.length}</span>
                            </div>
                            {categories.map((item) => renderCategoryTreeItem(item, 0))}
                        </div>
                    ) : (
                        <div className="empty-box small">暂无分类</div>
                    )}
                </div>

                {/* 右侧内容 */}
                <div className="report-center-content">
                    <div className="report-center-panel">
                        <div className="report-center-toolbar">
                            <div className="report-center-stats">
                                <span className="dataset-stat-chip">
                                    {selectedCategoryId !== null
                                        ? `${categories.find((c) => c.id === selectedCategoryId)?.name || '选中分类'} · ${filteredReports.length}`
                                        : `全部 · ${activeReports.length}`}
                                </span>
                            </div>
                            <div className="dataset-toolbar-actions">
                                <div className="view-mode-toggle">
                                    <button
                                        type="button"
                                        className={viewMode === 'card' ? 'active' : ''}
                                        onClick={() => setViewMode('card')}
                                        title="卡片视图"
                                    >
                                        <LayoutGrid size={14} />
                                    </button>
                                    <button
                                        type="button"
                                        className={viewMode === 'list' ? 'active' : ''}
                                        onClick={() => setViewMode('list')}
                                        title="列表视图"
                                    >
                                        <List size={14} />
                                    </button>
                                </div>
                                <label className="workspace-search-field" style={{ minWidth: 220, padding: '0.5rem 0.7rem' }}>
                                    <Search size={14} />
                                    <input
                                        value={searchText}
                                        onChange={(e) => setSearchText(e.target.value)}
                                        placeholder="搜索应用名称或描述"
                                    />
                                </label>
                                <button className="btn-outline" type="button" onClick={() => void loadReports()} style={{ padding: '0.45rem 0.7rem', fontSize: '0.78rem' }}>
                                    <RefreshCw size={13} />
                                    刷新
                                </button>
                            </div>
                        </div>

                        {loading ? (
                            <div className="empty-box">加载中...</div>
                        ) : filteredReports.length ? (
                            viewMode === 'card' ? (
                                <div className="resource-list dataset-card-grid">
                                    {filteredReports.map((item) => (
                                        <div
                                            key={item.id}
                                            className="report-center-card"
                                            onClick={() => window.open(`/report-viewer/${item.id}`, '_blank')}
                                        >
                                            <div className="report-center-card-accent" />
                                            <div className="report-center-card-glow" />
                                            <div className="report-center-card-body">
                                                <div className="report-center-card-topline">
                                                    <span className="report-center-card-kicker">数据应用</span>
                                                    <span className="report-center-card-status">启用</span>
                                                </div>
                                                <div className="report-center-card-row1">
                                                    <div className="report-center-card-icon" aria-hidden="true">
                                                        <FileSpreadsheet size={16} />
                                                    </div>
                                                    <div className="report-center-card-heading">
                                                        <strong className="report-center-card-name">{item.name}</strong>
                                                        <span className="report-center-card-subtitle">
                                                            {item.dataset_name || '暂未绑定数据集'}
                                                        </span>
                                                    </div>
                                                </div>
                                                <div className="report-center-card-row2">
                                                    <span className="report-center-card-type">{getReportTypeLabel(item.report_type)}</span>
                                                    {item.category_name && (
                                                        <span className="report-center-card-cat">
                                                            <FolderOpen size={10} />{item.category_name}
                                                        </span>
                                                    )}
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="report-center-list">
                                    <div className="report-center-list-header">
                                        <span className="list-col-name">应用名称</span>
                                        <span className="list-col-status">状态</span>
                                        <span className="list-col-type">类型</span>
                                        <span className="list-col-dataset">数据集</span>
                                        <span className="list-col-category">分类</span>
                                        <span className="list-col-desc">说明</span>
                                        <span className="list-col-action">操作</span>
                                    </div>
                                    {filteredReports.map((item) => (
                                        <div
                                            key={item.id}
                                            className="report-center-list-row"
                                            onClick={() => window.open(`/report-viewer/${item.id}`, '_blank')}
                                        >
                                            <span className="list-col-name">
                                                <FileSpreadsheet size={14} style={{ flexShrink: 0, color: '#3b82f6' }} />
                                                <strong>{item.name}</strong>
                                            </span>
                                            <span className="list-col-status">
                                                <span className="connection-status-chip active" style={{ fontSize: '0.65rem', padding: '0.1rem 0.4rem' }}>启用</span>
                                            </span>
                                            <span className="list-col-type">{getReportTypeLabel(item.report_type)}</span>
                                            <span className="list-col-dataset">{item.dataset_name || '-'}</span>
                                            <span className="list-col-category">{item.category_name || '-'}</span>
                                            <span className="list-col-desc" title={item.description || ''}>{item.description || '未填写应用说明'}</span>
                                            <span className="list-col-action">
                                                <button
                                                    className="btn-outline"
                                                    style={{ padding: '0.3rem 0.55rem', fontSize: '0.75rem' }}
                                                    onClick={(e) => { e.stopPropagation(); window.open(`/report-viewer/${item.id}`, '_blank'); }}
                                                >
                                                    <Play size={12} />
                                                    查看
                                                </button>
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            )
                        ) : (
                            <div className="empty-box">
                                {searchText.trim() ? '没有匹配当前搜索条件的数据应用。' : selectedCategoryId !== null ? '当前分类下暂无数据应用。' : '暂无已发布的数据应用。'}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

// ── 列偏好持久化 ──
type ColumnPreferences = {
    columnOrder: string[];   // 列 key 顺序
    hiddenKeys: string[];    // 用户手动隐藏的列 key
};

const COLUMN_PREFS_PREFIX = 'finflow_report_cols_';

const loadColumnPreferences = (reportId: number): ColumnPreferences | null => {
    try {
        const raw = localStorage.getItem(`${COLUMN_PREFS_PREFIX}${reportId}`);
        if (!raw) return null;
        return JSON.parse(raw) as ColumnPreferences;
    } catch {
        return null;
    }
};

const saveColumnPreferences = (reportId: number, prefs: ColumnPreferences) => {
    try {
        localStorage.setItem(`${COLUMN_PREFS_PREFIX}${reportId}`, JSON.stringify(prefs));
    } catch { /* ignore */ }
};

function ReportViewer({ report, onBack, isStandalone = false }: { report: Report; onBack?: () => void; isStandalone?: boolean }) {
    // 设置浏览器标签页标题（useLayoutEffect 同步执行，确保在绘制前生效）
    useLayoutEffect(() => {
        if (report.name) {
            document.title = `${report.name} - FinFlow`;
        }
    }, [report.name]);

    const config = useMemo(() => parseJson<ReportConfig>(report.config_json, {}), [report.config_json]);
    const filters = useMemo(() => (config.filters || []).map(normalizeFilter), [config.filters]);
    const columns = useMemo(() => sanitizeReportColumns(config.columns || []), [config.columns]);
    const tableStyle = useMemo<TableStyleConfig>(() => config.table_style || {}, [config.table_style]);

    const paginationEnabled = tableStyle.pagination_enabled ?? false;
    const pageSize = tableStyle.page_size || 20;
    const pageSizeOptions = tableStyle.page_size_options || [10, 20, 50, 100];

    const [currentPage, setCurrentPage] = useState(1);
    const [currentPageSize, setCurrentPageSize] = useState(pageSize);

    const [runtimeFilters, setRuntimeFilters] = useState<Record<string, string>>(() => buildFilterDefaults(filters));
    const [result, setResult] = useState<QueryResult | null>(null);
    const [running, setRunning] = useState(false);
    const [filtersVisible, setFiltersVisible] = useState(false);
    const [dictionaryItemsById, setDictionaryItemsById] = useState<Record<number, DataDictionaryItem[]>>({});

    // ── 列偏好状态 ──
    const [columnOrder, setColumnOrder] = useState<string[]>(() => loadColumnPreferences(report.id)?.columnOrder || []);
    const [hiddenKeys, setHiddenKeys] = useState<string[]>(() => loadColumnPreferences(report.id)?.hiddenKeys || []);
    const [columnConfigOpen, setColumnConfigOpen] = useState(false);

    // ── 列拖拽状态 ──
    const [draggingColKey, setDraggingColKey] = useState<string | null>(null);
    const [dragOverColKey, setDragOverColKey] = useState<string | null>(null);
    const [dragOverPosition, setDragOverPosition] = useState<'left' | 'right' | null>(null);
    const dragGhostRef = useRef<HTMLDivElement | null>(null);

    const requiredDictionaryIds = useMemo(() => {
        const ids = new Set<number>();
        columns.forEach((col) => {
            if (typeof col.dictionary_id === 'number') ids.add(col.dictionary_id);
        });
        filters.forEach((f) => {
            if (typeof f.dictionary_id === 'number') ids.add(f.dictionary_id);
        });
        return Array.from(ids);
    }, [columns, filters]);

    useEffect(() => {
        if (!requiredDictionaryIds.length) return;
        const missing = requiredDictionaryIds.filter((id) => !dictionaryItemsById[id]);
        if (!missing.length) return;
        void Promise.all(
            missing.map(async (id) => {
                if (id < 0) {
                    const items = await getBusinessDictionaryItems(Math.abs(id), false);
                    return [id, toReportDictionaryItems(items || [])] as const;
                }
                const res = await getDataDictionaryItems(id, 500);
                return [id, res.items || []] as const;
            })
        )
            .then((results) => {
                setDictionaryItemsById((prev) => {
                    const next = { ...prev };
                    results.forEach(([id, items]) => { next[id] = items; });
                    return next;
                });
            })
            .catch(() => {});
    }, [requiredDictionaryIds, dictionaryItemsById]);

    const runReport = useCallback(async () => {
        setRunning(true);
        try {
            const params = Object.fromEntries(
                Object.entries(runtimeFilters).filter(([, v]) => v !== '' && v !== null && v !== undefined)
            );
            const res = await runReportingReport(report.id, {
                params,
            });
            setResult({
                columns: res.columns || [],
                rows: res.rows || [],
                numeric_summary: res.numeric_summary || {},
                row_count: res.row_count || 0,
                limit: res.limit || 0,
            });
        } catch (error: unknown) {
            const apiError = error as ApiErrorLike;
            alert(apiError.response?.data?.detail || apiError.message || '运行失败');
        } finally {
            setRunning(false);
        }
    }, [report.id, runtimeFilters]);

    useEffect(() => {
        void runReport();
    }, [runReport]);

    // 持久化列偏好
    useEffect(() => {
        saveColumnPreferences(report.id, { columnOrder, hiddenKeys });
    }, [report.id, columnOrder, hiddenKeys]);

    // ── 列拖拽处理 ──
    const handleColumnDragStart = (e: React.DragEvent, colKey: string) => {
        setDraggingColKey(colKey);
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', colKey);
        // 自定义拖拽幽灵
        const ghost = document.createElement('div');
        ghost.className = 'col-drag-ghost';
        ghost.textContent = resolvedColumns.find((c) => c.config.key === colKey)?.config.label || colKey;
        document.body.appendChild(ghost);
        dragGhostRef.current = ghost;
        e.dataTransfer.setDragImage(ghost, 0, 0);
        requestAnimationFrame(() => ghost.remove());
    };

    const handleColumnDragOver = (e: React.DragEvent, colKey: string) => {
        if (!draggingColKey || draggingColKey === colKey) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
        const midX = rect.left + rect.width / 2;
        setDragOverColKey(colKey);
        setDragOverPosition(e.clientX < midX ? 'left' : 'right');
    };

    const handleColumnDragLeave = (e: React.DragEvent) => {
        const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
        if (e.clientX < rect.left || e.clientX > rect.right || e.clientY < rect.top || e.clientY > rect.bottom) {
            setDragOverColKey(null);
            setDragOverPosition(null);
        }
    };

    const handleColumnDrop = (e: React.DragEvent, targetKey: string) => {
        e.preventDefault();
        if (!draggingColKey || draggingColKey === targetKey) return;

        const currentKeys = resolvedColumns.map((c) => c.config.key);
        const sourceIdx = currentKeys.indexOf(draggingColKey);
        const targetIdx = currentKeys.indexOf(targetKey);
        if (sourceIdx === -1 || targetIdx === -1) return;

        const newOrder = [...currentKeys];
        newOrder.splice(sourceIdx, 1);
        const insertIdx = newOrder.indexOf(targetKey) + (dragOverPosition === 'right' ? 1 : 0);
        newOrder.splice(insertIdx, 0, draggingColKey);
        setColumnOrder(newOrder);

        // 清理
        setDraggingColKey(null);
        setDragOverColKey(null);
        setDragOverPosition(null);
    };

    const handleColumnDragEnd = () => {
        setDraggingColKey(null);
        setDragOverColKey(null);
        setDragOverPosition(null);
        if (dragGhostRef.current) {
            dragGhostRef.current.remove();
            dragGhostRef.current = null;
        }
    };

    const toggleColumnVisibility = (colKey: string) => {
        setHiddenKeys((prev) => prev.includes(colKey) ? prev.filter((k) => k !== colKey) : [...prev, colKey]);
    };

    const resetColumnPreferences = () => {
        setColumnOrder([]);
        setHiddenKeys([]);
    };

    const visibleColumns = useMemo(
        () => columns.filter((col) => col.visible !== false && !hiddenKeys.includes(col.key)),
        [columns, hiddenKeys]
    );

    const resolvedColumns = useMemo(() => {
        const source = result?.columns || [];
        const sourceMap = new Map(source.map((c) => [c.name, c]));
        let ordered = visibleColumns.map((col) => {
            const sourceCol = sourceMap.get(col.key);
            return { config: col, source: sourceCol };
        }).filter((item) => item.source);

        // 应用用户自定义列顺序
        if (columnOrder.length > 0) {
            const orderMap = new Map(columnOrder.map((key, i) => [key, i]));
            ordered = [...ordered].sort((a, b) => {
                const oa = orderMap.get(a.config.key) ?? Infinity;
                const ob = orderMap.get(b.config.key) ?? Infinity;
                return oa - ob;
            });
        }

        return ordered;
    }, [visibleColumns, result, columnOrder]);

    const chartData = useMemo(
        () => buildChartData(result?.rows || [], config.chart),
        [result?.rows, config.chart]
    );

    const aggregateSummaries = useMemo(() => {
        const rows = result?.rows || [];
        if (!rows.length) return [];
        return columns
            .filter((col) => col.visible && col.aggregate && col.aggregate !== 'none')
            .map((col) => {
                const values = rows.map((row) => row[col.key]);
                let value: string = '-';
                const nums = values.map((v) => {
                    const n = typeof v === 'number' ? v : Number(v);
                    return Number.isFinite(n) ? n : null;
                }).filter((n): n is number => n !== null);

                if (col.aggregate === 'count') {
                    value = String(values.filter((v) => v !== null && v !== undefined && v !== '').length);
                } else if (col.aggregate === 'count_distinct') {
                    value = String(new Set(values.filter((v) => v !== null && v !== undefined && v !== '').map(String)).size);
                } else if (nums.length) {
                    if (col.aggregate === 'sum') value = nums.reduce((a, b) => a + b, 0).toFixed(2);
                    else if (col.aggregate === 'avg') value = (nums.reduce((a, b) => a + b, 0) / nums.length).toFixed(2);
                    else if (col.aggregate === 'min') value = Math.min(...nums).toFixed(2);
                    else if (col.aggregate === 'max') value = Math.max(...nums).toFixed(2);
                }
                return { key: col.key, label: col.label || col.key, method: col.aggregate!, value };
            });
    }, [columns, result]);

    const pagedRows = useMemo(() => {
        const allRows = result?.rows || [];
        if (!paginationEnabled) return allRows;
        const start = (currentPage - 1) * currentPageSize;
        return allRows.slice(start, start + currentPageSize);
    }, [result?.rows, paginationEnabled, currentPage, currentPageSize]);

    const totalPages = useMemo(() => {
        if (!paginationEnabled || !result?.rows?.length) return 1;
        return Math.max(1, Math.ceil(result.rows.length / currentPageSize));
    }, [paginationEnabled, result?.rows, currentPageSize]);

    const handleExportCsv = () => {
        if (!result?.rows?.length) { alert('当前没有可导出的数据'); return; }
        const headers = resolvedColumns.map((item) => item.config.label || item.config.key);
        const keys = resolvedColumns.map((item) => item.config.key);
        const exportRows = result.rows.map((row) =>
            Object.fromEntries(
                keys.map((key, i) => [
                    headers[i],
                    formatValueByColumnConfig(row[key], {
                        configuredType: resolvedColumns[i].config.type,
                        columnType: resolvedColumns[i].source?.type,
                        sample: resolvedColumns[i].source?.sample,
                        dictionaryItems: typeof resolvedColumns[i].config.dictionary_id === 'number'
                            ? dictionaryItemsById[resolvedColumns[i].config.dictionary_id] || [] : [],
                        dictionaryDisplay: resolvedColumns[i].config.dictionary_display,
                    }),
                ])
            )
        );
        const csvContent = buildCsvContent(headers, exportRows);
        const filenameBase = (report.name || 'report').replace(/[\\/:*?"<>|]+/g, '_').replace(/\s+/g, '_');
        const blob = new Blob([`\ufeff${csvContent}`], { type: 'text/csv;charset=utf-8;' });
        const downloadUrl = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = downloadUrl;
        link.download = `${filenameBase}_${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.setTimeout(() => URL.revokeObjectURL(downloadUrl), 1000);
    };

    return (
        <div className={`page-container fade-in report-viewer-page${isStandalone ? ' standalone-viewer' : ''}`}>
            <div className="report-viewer-header">
                {!isStandalone && onBack && (
                    <button className="btn-outline" onClick={onBack}>
                        <ChevronLeft size={14} />
                        返回列表
                    </button>
                )}
                <div className="report-viewer-title">
                    <h2>{report.name}</h2>
                    {report.description && <span className="resource-meta">{report.description}</span>}
                </div>
                <div className="report-viewer-actions">
                    {filters.length > 0 && (
                        <button
                            className={`btn-outline${filtersVisible ? ' active' : ''}`}
                            onClick={() => setFiltersVisible((v) => !v)}
                        >
                            <Filter size={14} />
                            筛选
                        </button>
                    )}
                    <button className="btn-outline" onClick={handleExportCsv} disabled={!result?.rows?.length}>
                        <Download size={14} />
                        导出 CSV
                    </button>
                    {resolvedColumns.length > 0 && (
                        <button
                            className={`btn-outline${columnConfigOpen ? ' active' : ''}`}
                            onClick={() => setColumnConfigOpen((v) => !v)}
                        >
                            <Columns size={14} />
                            列配置
                        </button>
                    )}
                    <button className="btn-primary" onClick={() => void runReport()} disabled={running}>
                        <RefreshCw size={14} className={running ? 'spin' : ''} />
                        {running ? '运行中' : '刷新数据'}
                    </button>
                </div>
            </div>

            {filters.length > 0 && filtersVisible && (
                <div className="report-viewer-filters">
                    <div className="report-viewer-filters-body">
                        {filters.map((filter) => {
                            const dictItems = typeof filter.dictionary_id === 'number'
                                ? dictionaryItemsById[filter.dictionary_id] || [] : [];
                            const filterStyle = filter.width ? { width: filter.width, minWidth: undefined } : undefined;
                            return (
                                <label key={filter.key} className="report-viewer-filter-item" style={filterStyle}>
                                    <span>{filter.label || filter.key}</span>
                                    {filter.type === 'select' && (filter.options?.length || dictItems.length) ? (
                                        <select
                                            value={runtimeFilters[filter.key] || ''}
                                            onChange={(e) => setRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}
                                        >
                                            <option value="">全部</option>
                                            {dictItems.length
                                                ? dictItems.map((item) => (
                                                    <option key={item.key} value={item.key}>{item.path || item.label}</option>
                                                ))
                                                : (filter.options || []).map((opt) => (
                                                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                                                ))}
                                        </select>
                                    ) : filter.type === 'date' ? (
                                        <input
                                            type="date"
                                            value={runtimeFilters[filter.key] || ''}
                                            onChange={(e) => setRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}
                                        />
                                    ) : (
                                        <input
                                            type={filter.type === 'number' ? 'number' : 'text'}
                                            value={runtimeFilters[filter.key] || ''}
                                            onChange={(e) => setRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}
                                            placeholder={filter.placeholder || `输入${filter.label || filter.key}`}
                                        />
                                    )}
                                </label>
                            );
                        })}
                    </div>
                    <div className="report-viewer-filters-actions">
                        <button className="btn-primary" onClick={() => void runReport()} disabled={running}>
                            <Play size={14} />
                            查询
                        </button>
                        <button className="btn-outline" onClick={() => setFiltersVisible(false)}>
                            <ChevronUp size={14} />
                            收起
                        </button>
                    </div>
                </div>
            )}

            {columnConfigOpen && resolvedColumns.length > 0 && (
                <div className="column-config-drawer">
                    <div className="column-config-drawer-head">
                        <strong>列显示与顺序</strong>
                        <span className="resource-meta">拖拽调整顺序，勾选控制显示</span>
                        <button className="ghost-btn" type="button" onClick={() => setColumnConfigOpen(false)}>
                            <X size={16} />
                        </button>
                    </div>
                    <div className="column-config-drawer-list">
                        {columns.map((col) => {
                            const isVisible = col.visible !== false && !hiddenKeys.includes(col.key);
                            return (
                                <label key={col.key} className={`column-config-drawer-item ${!isVisible ? 'is-hidden' : ''}`}>
                                    <input
                                        type="checkbox"
                                        checked={isVisible}
                                        onChange={() => toggleColumnVisibility(col.key)}
                                        disabled={col.visible === false}
                                    />
                                    <span className="column-config-drawer-name">{col.label || col.key}</span>
                                    {col.visible === false && <span className="resource-meta" style={{ fontSize: '0.7rem' }}>默认隐藏</span>}
                                </label>
                            );
                        })}
                    </div>
                    <div className="column-config-drawer-footer">
                        <button className="btn-outline btn-sm" onClick={resetColumnPreferences}>
                            恢复默认
                        </button>
                    </div>
                </div>
            )}

            {config.chart?.enabled && chartData.length > 0 && (
                <div className="report-viewer-chart">
                    <div className="chart-header">
                        <span className="chart-badge">
                            {config.chart.chart_type === 'bar' ? <BarChart3 size={14} /> :
                             config.chart.chart_type === 'pie' ? <FileSpreadsheet size={14} /> :
                             <Table2 size={14} />}
                            图表
                        </span>
                    </div>
                    <div className="chart-stage">
                        <ResponsiveContainer width="100%" height={320}>
                            {config.chart.chart_type === 'bar' ? (
                                <BarChart data={chartData}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                                    <YAxis tick={{ fontSize: 12 }} />
                                    <Tooltip />
                                    <Legend />
                                    <Bar dataKey="value" name="数值" radius={[4, 4, 0, 0]}>
                                        {chartData.map((_, index) => (
                                            <Cell key={index} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                        ))}
                                    </Bar>
                                </BarChart>
                            ) : config.chart.chart_type === 'line' ? (
                                <LineChart data={chartData}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                                    <YAxis tick={{ fontSize: 12 }} />
                                    <Tooltip />
                                    <Legend />
                                    <Line type="monotone" dataKey="value" name="数值" stroke="#3b82f6" strokeWidth={2} />
                                </LineChart>
                            ) : (
                                <PieChart>
                                    <Pie
                                        data={chartData}
                                        dataKey="value"
                                        nameKey="name"
                                        cx="50%"
                                        cy="50%"
                                        outerRadius={120}
                                        label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
                                    >
                                        {chartData.map((_, index) => (
                                            <Cell key={index} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                        ))}
                                    </Pie>
                                    <Tooltip />
                                    <Legend />
                                </PieChart>
                            )}
                        </ResponsiveContainer>
                    </div>
                </div>
            )}

            <div
                className="report-viewer-table-wrapper"
                style={{
                    borderRadius: tableStyle.border_radius || undefined,
                    border: tableStyle.border_style === 'none' ? 'none' : tableStyle.border_style ? `${tableStyle.border_style} 1px ${tableStyle.border_color || 'rgba(226, 232, 240, 0.6)'}` : undefined,
                } as React.CSSProperties}
            >
                {result?.rows?.length ? (
                    <table
                        className={`report-viewer-table${tableStyle.striped === false ? ' no-stripes' : ''}`}
                        style={{
                            fontSize: tableStyle.font_size || undefined,
                        } as React.CSSProperties}
                    >
                        <thead>
                            <tr>
                                {tableStyle.show_row_number && (
                                    <th style={{ width: '48px', textAlign: 'center', background: tableStyle.header_background || undefined, color: tableStyle.header_color || undefined, fontSize: tableStyle.header_font_size || undefined, fontWeight: tableStyle.header_font_weight ? Number(tableStyle.header_font_weight) : undefined }}>#</th>
                                )}
                                {resolvedColumns.map((item) => (
                                    <th
                                        key={item.config.key}
                                        draggable
                                        onDragStart={(e) => handleColumnDragStart(e, item.config.key)}
                                        onDragOver={(e) => handleColumnDragOver(e, item.config.key)}
                                        onDragLeave={handleColumnDragLeave}
                                        onDrop={(e) => handleColumnDrop(e, item.config.key)}
                                        onDragEnd={handleColumnDragEnd}
                                        className={`viewer-th${draggingColKey === item.config.key ? ' is-dragging' : ''}${dragOverColKey === item.config.key ? ` is-drag-over drag-${dragOverPosition}` : ''}`}
                                        style={{
                                            width: item.config.width || undefined,
                                            background: tableStyle.header_background || undefined,
                                            color: tableStyle.header_color || undefined,
                                            fontSize: tableStyle.header_font_size || undefined,
                                            fontWeight: tableStyle.header_font_weight ? Number(tableStyle.header_font_weight) : undefined,
                                            cursor: 'grab',
                                            userSelect: 'none',
                                        } as React.CSSProperties}
                                    >
                                        {item.config.label || item.config.key}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {pagedRows.map((row, rowIndex) => (
                                <tr key={rowIndex} style={tableStyle.row_height ? { height: tableStyle.row_height } : undefined}>
                                    {tableStyle.show_row_number && (
                                        <td style={{ textAlign: 'center', color: '#94a3b8', fontSize: '0.78rem', background: tableStyle.body_background || undefined }}>
                                            {(currentPage - 1) * currentPageSize + rowIndex + 1}
                                        </td>
                                    )}
                                    {resolvedColumns.map((item) => {
                                        const value = row[item.config.key];
                                        const dictItems = typeof item.config.dictionary_id === 'number'
                                            ? dictionaryItemsById[item.config.dictionary_id] || [] : [];
                                        const text = formatValueByColumnConfig(value, {
                                            configuredType: item.config.type,
                                            columnType: item.source?.type,
                                            sample: item.source?.sample,
                                            dictionaryItems: dictItems,
                                            dictionaryDisplay: item.config.dictionary_display,
                                        });
                                        const style = getColumnStyleForValue(value, item.config.style_rules, row);
                                        return (
                                            <td key={item.config.key} style={{
                                                background: tableStyle.body_background || undefined,
                                                color: tableStyle.body_color || undefined,
                                                fontSize: tableStyle.body_font_size || undefined,
                                            } as React.CSSProperties}>
                                                <span title={text} style={style || undefined}>{text}</span>
                                            </td>
                                        );
                                    })}
                                </tr>
                            ))}
                        </tbody>
                        {aggregateSummaries.length > 0 && (
                            <tfoot>
                                <tr>
                                    {tableStyle.show_row_number && (
                                        <td style={{ background: tableStyle.footer_background || undefined, color: tableStyle.footer_color || undefined }}></td>
                                    )}
                                    {resolvedColumns.map((item) => {
                                        const summary = aggregateSummaries.find((s) => s.key === item.config.key);
                                        return (
                                            <td key={item.config.key} style={{
                                                background: tableStyle.footer_background || undefined,
                                                color: tableStyle.footer_color || undefined,
                                            } as React.CSSProperties}>
                                                {summary ? <span className="aggregate-footer-value">{summary.value}</span> : null}
                                            </td>
                                        );
                                    })}
                                </tr>
                            </tfoot>
                        )}
                    </table>
                ) : running ? (
                    <div className="empty-box">正在加载数据...</div>
                ) : result ? (
                    <div className="empty-box">{tableStyle.empty_text || '当前查询没有返回数据。'}</div>
                ) : null}
            </div>

            {result && paginationEnabled && result.rows.length > 0 && (
                <div className="report-viewer-pagination">
                    <div className="pagination-info">
                        第 {(currentPage - 1) * currentPageSize + 1}–{Math.min(currentPage * currentPageSize, result.rows.length)} 行，共 {result.rows.length} 行
                    </div>
                    <div className="pagination-controls">
                        <select
                            className="pagination-page-size"
                            value={currentPageSize}
                            onChange={(e) => {
                                setCurrentPageSize(Number(e.target.value));
                                setCurrentPage(1);
                            }}
                        >
                            {pageSizeOptions.map((size) => (
                                <option key={size} value={size}>{size} 行/页</option>
                            ))}
                        </select>
                        <button
                            className="pagination-btn"
                            disabled={currentPage <= 1}
                            onClick={() => setCurrentPage(1)}
                            title="首页"
                        >
                            ⟪
                        </button>
                        <button
                            className="pagination-btn"
                            disabled={currentPage <= 1}
                            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                            title="上一页"
                        >
                            ‹
                        </button>
                        <span className="pagination-page-num">
                            {currentPage} / {totalPages}
                        </span>
                        <button
                            className="pagination-btn"
                            disabled={currentPage >= totalPages}
                            onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
                            title="下一页"
                        >
                            ›
                        </button>
                        <button
                            className="pagination-btn"
                            disabled={currentPage >= totalPages}
                            onClick={() => setCurrentPage(totalPages)}
                            title="末页"
                        >
                            ⟫
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}
