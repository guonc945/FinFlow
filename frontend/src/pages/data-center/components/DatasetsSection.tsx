import { useMemo, useState } from 'react';
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Database, FileJson, Pencil, Plus, RefreshCw, Search, Trash2 } from 'lucide-react';
import Select from '../../../components/common/Select';
import type { Dataset } from '../types';

type DatasetsSectionProps = {
    datasets: Dataset[];
    totalDatasets: number;
    searchText: string;
    onSearchChange: (value: string) => void;
    onCreate: () => void;
    onEdit: (dataset: Dataset) => void;
    onReloadDatasets: () => void;
    onDelete: (datasetId: number) => void | Promise<void>;
};

const getColumnCount = (value?: string | null) => {
    if (!value) return 0;
    try {
        const parsed = JSON.parse(value);
        return Array.isArray(parsed) ? parsed.length : 0;
    } catch {
        return 0;
    }
};

export default function DatasetsSection({
    datasets,
    totalDatasets,
    searchText,
    onSearchChange,
    onCreate,
    onEdit,
    onReloadDatasets,
    onDelete,
}: DatasetsSectionProps) {
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(12);

    const totalPages = Math.max(1, Math.ceil(datasets.length / pageSize));

    const pagedDatasets = useMemo(() => {
        const start = (page - 1) * pageSize;
        return datasets.slice(start, start + pageSize);
    }, [datasets, page, pageSize]);

    const activeCount = datasets.filter((item) => item.is_active).length;
    const validatedCount = datasets.filter((item) => !!item.last_validated_at).length;

    const handlePageChange = (newPage: number) => {
        setPage(Math.max(1, Math.min(newPage, totalPages)));
    };

    const handlePageSizeChange = (newSize: number) => {
        setPageSize(newSize);
        setPage(1);
    };

    const handleSearchChange = (value: string) => {
        onSearchChange(value);
        setPage(1);
    };

    return (
        <div className="card glass reporting-panel dataset-workspace-panel">
            <div className="section-head">
                <div>
                    <h3>数据集建模工作区</h3>
                    <div className="resource-meta">集中管理 SQL 数据模型、参数模板、校验结果与预览入口。</div>
                </div>
                <button className="btn-primary" onClick={onCreate}>
                    <Plus size={14} />
                    新建数据集
                </button>
            </div>

            <div className="dataset-toolbar">
                <div className="dataset-toolbar-stats">
                    <span className="dataset-stat-chip">
                        <Database size={14} />
                        全部 {totalDatasets}
                    </span>
                    <span className="dataset-stat-chip active">
                        启用中 {activeCount}
                    </span>
                    <span className="dataset-stat-chip warm">
                        已校验 {validatedCount}
                    </span>
                </div>
                <div className="dataset-toolbar-actions">
                    <label className="workspace-search-field">
                        <Search size={15} />
                        <input
                            value={searchText}
                            onChange={(e) => handleSearchChange(e.target.value)}
                            placeholder="搜索名称、连接、描述或 SQL"
                        />
                    </label>
                    <button className="btn-outline" type="button" onClick={onReloadDatasets}>
                        <RefreshCw size={14} />
                        刷新列表
                    </button>
                </div>
            </div>

            <div className="resource-list dataset-card-grid">
                {pagedDatasets.map((item) => {
                    const columnCount = getColumnCount(item.last_columns_json);
                    return (
                        <div key={item.id} className="dataset-compact-card dataset-card">
                            <div className="dataset-card-accent" />
                            <div className="dataset-card-glow" />
                            <div className="dataset-card-body">
                                <div className="dataset-card-topline">
                                    <span className="dataset-card-kicker">数据集</span>
                                    <span className={`dataset-card-status ${item.is_active ? 'active' : 'inactive'}`}>
                                        {item.is_active ? '启用' : '停用'}
                                    </span>
                                </div>
                                <div className="dataset-card-row1">
                                    <div className="dataset-card-icon">
                                        <FileJson size={16} />
                                    </div>
                                    <div className="dataset-card-heading">
                                        <strong className="dataset-card-name">{item.name}</strong>
                                        <span className="dataset-card-subtitle">{item.connection_name || '未绑定连接'}</span>
                                    </div>
                                </div>
                                <div className="dataset-card-row2">
                                    <span className="dataset-card-badge">
                                        <Database size={10} /> {columnCount} 列
                                    </span>
                                    <span className="dataset-card-badge">
                                        上限 {item.row_limit}
                                    </span>
                                    {item.last_validated_at && (
                                        <span className="dataset-card-badge warm">已校验</span>
                                    )}
                                </div>
                                {item.description && (
                                    <div className="dataset-card-desc">{item.description}</div>
                                )}
                            </div>
                            <div className="dataset-card-actions">
                                <button onClick={() => onEdit(item)}>
                                    <Pencil size={13} />
                                    继续
                                </button>
                                <button className="danger" onClick={() => void onDelete(item.id)}>
                                    <Trash2 size={13} />
                                    删除
                                </button>
                            </div>
                        </div>
                    );
                })}
                {!pagedDatasets.length && (
                    <div className="empty-box">
                        {searchText.trim() ? '没有匹配当前搜索条件的数据集模型。' : '还没有数据集模型，先新建一个开始建模。'}
                    </div>
                )}
            </div>

            {datasets.length > pageSize && (
                <div className="dc-pagination">
                    <span className="dc-pagination-info">
                        显示 {(page - 1) * pageSize + 1}–{Math.min(page * pageSize, datasets.length)} 条，共 {datasets.length} 条
                    </span>
                    <div className="dc-pagination-controls">
                        <Select
                            className="dc-page-select"
                            value={String(pageSize)}
                            onChange={(v) => handlePageSizeChange(Number(v))}
                            options={[
                                { value: '12', label: '12 条/页' },
                                { value: '24', label: '24 条/页' },
                                { value: '48', label: '48 条/页' },
                            ]}
                        />
                        <div style={{ display: 'flex', gap: '0.25rem' }}>
                            <button className="dc-page-btn" disabled={page === 1} onClick={() => handlePageChange(1)}><ChevronsLeft size={14} /></button>
                            <button className="dc-page-btn" disabled={page === 1} onClick={() => handlePageChange(page - 1)}><ChevronLeft size={14} /></button>
                            <button className="dc-page-btn active">{page}</button>
                            {page < totalPages && <button className="dc-page-btn" onClick={() => handlePageChange(page + 1)}>{page + 1}</button>}
                            {page + 1 < totalPages && <span style={{ padding: '0 0.35rem', color: '#94a3b8' }}>...</span>}
                            {page + 1 < totalPages && <button className="dc-page-btn" onClick={() => handlePageChange(totalPages)}>{totalPages}</button>}
                            <button className="dc-page-btn" disabled={page === totalPages} onClick={() => handlePageChange(page + 1)}><ChevronRight size={14} /></button>
                            <button className="dc-page-btn" disabled={page === totalPages} onClick={() => handlePageChange(totalPages)}><ChevronsRight size={14} /></button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
