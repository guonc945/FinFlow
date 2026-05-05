import { ChevronDown, ChevronRight, FolderOpen, Pencil, Plus, RefreshCw, Search, Trash2 } from 'lucide-react';
import type { ReportCategory, Report } from '../types';

type ReportsSectionProps = {
    reports: Report[];
    totalReports: number;
    searchText: string;
    onSearchChange: (value: string) => void;
    onCreate: () => void;
    onEdit: (report: Report) => void;
    onReloadReports: () => void;
    onDelete: (reportId: number) => void | Promise<void>;
    categoryTree?: ReportCategory[];
    expandedCategoryIds?: Set<number>;
    selectedCategoryId?: number | null;
    onToggleCategoryExpand?: (id: number) => void;
    onSelectCategory?: (id: number | null) => void;
};

const getReportTypeLabel = (type: string) => {
    const typeMap: Record<string, string> = {
        table: '数据表格',
        chart: '图表',
        card: '卡片',
        pivot: '透视表',
    };
    return typeMap[type] || type;
};

export default function ReportsSection({
    reports,
    totalReports,
    searchText,
    onSearchChange,
    onCreate,
    onEdit,
    onReloadReports,
    onDelete,
    categoryTree,
    expandedCategoryIds,
    selectedCategoryId,
    onToggleCategoryExpand,
    onSelectCategory,
}: ReportsSectionProps) {
    const activeCount = reports.filter((item) => item.is_active).length;
    const hasCategoryNav = categoryTree && categoryTree.length > 0;

    const renderCategoryTreeItem = (item: ReportCategory, depth: number) => {
        const hasChildren = item.children && item.children.length > 0;
        const isExpanded = expandedCategoryIds?.has(item.id);
        const isSelected = selectedCategoryId === item.id;
        const reportCount = reports.filter((r) => r.category_id === item.id).length;

        return (
            <div key={item.id}>
                <div
                    className={`report-center-nav-item${isSelected ? ' active' : ''}`}
                    style={{ paddingLeft: `${depth * 16 + 8}px` }}
                    onClick={() => onSelectCategory?.(item.id)}
                >
                    <span
                        className="report-center-nav-toggle"
                        onClick={(e) => {
                            e.stopPropagation();
                            if (hasChildren) onToggleCategoryExpand?.(item.id);
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

    const mainContent = (
        <div className="report-center-panel">
            <div className="report-center-toolbar">
                <div className="report-center-stats">
                    <span className="dataset-stat-chip">
                        全部 · {totalReports}
                    </span>
                    <span className="dataset-stat-chip active">
                        启用中 · {activeCount}
                    </span>
                </div>
                <div className="dataset-toolbar-actions">
                    <label className="workspace-search-field" style={{ minWidth: 200, padding: '0.45rem 0.65rem' }}>
                        <Search size={14} />
                        <input
                            value={searchText}
                            onChange={(e) => onSearchChange(e.target.value)}
                            placeholder="搜索名称、数据集或描述"
                        />
                    </label>
                    <button className="btn-primary" onClick={onCreate} style={{ padding: '0.45rem 0.7rem', fontSize: '0.78rem' }}>
                        <Plus size={13} />
                        新建
                    </button>
                    <button className="btn-outline" type="button" onClick={onReloadReports} style={{ padding: '0.45rem 0.7rem', fontSize: '0.78rem' }}>
                        <RefreshCw size={13} />
                        刷新
                    </button>
                </div>
            </div>

            <div className="resource-list dataset-card-grid">
                {reports.map((item) => (
                    <div key={item.id} className="resource-card dataset-compact-card report-center-card">
                        <div className="dataset-card-body">
                            <div className="dataset-card-title">
                                <strong>{item.name}</strong>
                                <span className={`connection-status-chip ${item.is_active ? 'active' : 'inactive'}`}>
                                    {item.is_active ? '启用' : '停用'}
                                </span>
                            </div>
                            <div className="resource-meta">{item.dataset_name || '未绑定数据集'} · {getReportTypeLabel(item.report_type)}</div>
                            {item.category_name && (
                                <div className="resource-meta" style={{ color: '#3b82f6' }}>
                                    <FolderOpen size={11} style={{ display: 'inline', verticalAlign: 'middle', marginRight: 2 }} />
                                    {item.category_name}
                                </div>
                            )}
                            {item.description ? (
                                <div className="resource-meta">{item.description}</div>
                            ) : (
                                <div className="resource-meta" style={{ color: '#94a3b8' }}>未填写报表说明</div>
                            )}
                        </div>

                        <div className="resource-actions dataset-card-actions">
                            <button onClick={() => onEdit(item)}>
                                <Pencil size={13} />
                                设计
                            </button>
                            <button className="danger" onClick={() => void onDelete(item.id)}>
                                <Trash2 size={13} />
                                删除
                            </button>
                        </div>
                    </div>
                ))}
                {!reports.length && (
                    <div className="empty-box">
                        {searchText.trim() ? '没有匹配当前搜索条件的报表。' : selectedCategoryId !== null ? '当前分类下暂无报表。' : '还没有报表，先新建一个开始设计。'}
                    </div>
                )}
            </div>
        </div>
    );

    // 有分类导航时，渲染侧边栏 + 内容布局
    if (hasCategoryNav) {
        return (
            <div className="report-center-main">
                <div className="report-center-sidebar">
                    <div className="report-center-sidebar-head">
                        <strong>报表分类</strong>
                    </div>
                    <div className="report-center-nav-tree">
                        <div
                            className={`report-center-nav-item${selectedCategoryId === null ? ' active' : ''}`}
                            onClick={() => onSelectCategory?.(null)}
                        >
                            <span style={{ width: 12, display: 'inline-block' }} />
                            <FolderOpen size={14} />
                            <span className="report-center-nav-name">全部分类</span>
                            <span className="report-center-nav-count">{totalReports}</span>
                        </div>
                        {categoryTree.map((item) => renderCategoryTreeItem(item, 0))}
                    </div>
                </div>
                <div className="report-center-content">
                    {mainContent}
                </div>
            </div>
        );
    }

    // 无分类导航时，保持原有布局
    return (
        <div className="card glass reporting-panel report-workspace-panel">
            <div className="section-head">
                <div>
                    <h3>报表设计</h3>
                    <div className="resource-meta">将数据集封装成报表，支持筛选器、图表配置和数据导出。</div>
                </div>
                <button className="btn-primary" onClick={onCreate}>
                    <Plus size={14} />
                    新建报表
                </button>
            </div>

            <div className="dataset-toolbar">
                <div className="dataset-toolbar-stats">
                    <span className="dataset-stat-chip">
                        全部 {totalReports}
                    </span>
                    <span className="dataset-stat-chip active">
                        启用中 {activeCount}
                    </span>
                </div>
                <div className="dataset-toolbar-actions">
                    <label className="workspace-search-field">
                        <Search size={15} />
                        <input
                            value={searchText}
                            onChange={(e) => onSearchChange(e.target.value)}
                            placeholder="搜索名称、数据集或描述"
                        />
                    </label>
                    <button className="btn-outline" type="button" onClick={onReloadReports}>
                        <RefreshCw size={14} />
                        刷新列表
                    </button>
                </div>
            </div>

            <div className="resource-list dataset-card-grid">
                {reports.map((item) => (
                    <div key={item.id} className="resource-card dataset-compact-card">
                        <div className="dataset-card-body">
                            <div className="dataset-card-title">
                                <strong>{item.name}</strong>
                                <span className={`connection-status-chip ${item.is_active ? 'active' : 'inactive'}`}>
                                    {item.is_active ? '启用' : '停用'}
                                </span>
                            </div>
                            <div className="resource-meta">{item.dataset_name || '未绑定数据集'} / {getReportTypeLabel(item.report_type)}</div>
                            {item.description ? (
                                <div className="resource-meta">{item.description}</div>
                            ) : (
                                <div className="resource-meta" style={{ color: '#94a3b8' }}>未填写报表说明</div>
                            )}
                        </div>

                        <div className="resource-actions dataset-card-actions">
                            <button onClick={() => onEdit(item)}>
                                <Pencil size={14} />
                                设计
                            </button>
                            <button className="danger" onClick={() => void onDelete(item.id)}>
                                <Trash2 size={14} />
                                删除
                            </button>
                        </div>
                    </div>
                ))}
                {!reports.length && (
                    <div className="empty-box">
                        {searchText.trim() ? '没有匹配当前搜索条件的报表。' : '还没有报表，先新建一个开始设计。'}
                    </div>
                )}
            </div>
        </div>
    );
}
