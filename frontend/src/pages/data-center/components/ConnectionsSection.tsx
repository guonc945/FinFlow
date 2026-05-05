import { Plus, RefreshCw, Search, Trash2 } from 'lucide-react';
import type { Connection } from '../types';

type ConnectionsSectionProps = {
    connections: Connection[];
    totalConnections: number;
    loading: boolean;
    selectedConnectionId: number | null;
    searchText: string;
    onCreate: () => void;
    onEdit: (connection: Connection) => void;
    onSearchChange: (value: string) => void;
    onReloadConnections: () => void | Promise<void>;
    onBrowse: (connectionId: number, schemaName?: string | null) => void | Promise<void>;
    onDelete: (connectionId: number) => void | Promise<void>;
};

export default function ConnectionsSection({
    connections,
    totalConnections,
    loading,
    selectedConnectionId,
    searchText,
    onCreate,
    onEdit,
    onSearchChange,
    onReloadConnections,
    onBrowse,
    onDelete,
}: ConnectionsSectionProps) {
    const hasFilter = Boolean(searchText.trim());

    return (
        <div className="card glass reporting-panel connection-workspace-panel">
            <div className="section-head">
                <div>
                    <h3>连接接入工作区</h3>
                    <div className="resource-meta">这里集中显示和管理已创建的外部连接。</div>
                </div>
                <div className="reporting-inline-tools">
                    <button className="btn-primary" onClick={onCreate}>
                        <Plus size={14} />
                        新建外部连接
                    </button>
                    <div className="resource-meta">已创建 {totalConnections} 个连接</div>
                    <button className="btn-outline" type="button" onClick={() => void onReloadConnections()}>
                        <RefreshCw size={14} />
                        刷新列表
                    </button>
                </div>
            </div>
            {totalConnections > 0 ? (
                <div className="connection-quick-strip">
                    {connections.slice(0, 6).map((item) => (
                        <button
                            key={`quick-${item.id}`}
                            type="button"
                            className={`connection-quick-chip ${selectedConnectionId === item.id ? 'active' : ''}`}
                            onClick={() => void onBrowse(item.id, item.schema_name || undefined)}
                        >
                            {item.name}
                        </button>
                    ))}
                </div>
            ) : null}
            <label className="connection-search-box">
                <Search size={14} />
                <input
                    value={searchText}
                    onChange={(e) => onSearchChange(e.target.value)}
                    placeholder="按名称、类型、主机或库名筛选"
                />
            </label>
            <div className="resource-list connection-card-grid">
                {loading ? (
                    <div className="preview-status loading">正在加载已创建的外部连接...</div>
                ) : connections.length ? (
                    connections.map((item) => (
                        <div
                            key={item.id}
                            className={`resource-card connection-compact-card ${selectedConnectionId === item.id ? 'selected' : ''}`}
                        >
                            <div className="connection-card-body">
                                <div className="connection-card-title">
                                    <strong>{item.name}</strong>
                                    <span className={`connection-status-chip ${item.is_active ? 'active' : 'inactive'}`}>
                                        {item.is_active ? '启用' : '停用'}
                                    </span>
                                    {item.has_password ? <span className="connection-status-chip">已设密码</span> : null}
                                </div>
                                <div className="resource-meta">
                                    {item.db_type} / {item.host || 'local'} / {item.database_name}
                                </div>
                                <div className="resource-meta">
                                    {item.schema_name || '默认 Schema'}
                                    {item.updated_at ? ` / 更新于 ${String(item.updated_at).replace('T', ' ').slice(0, 16)}` : ''}
                                </div>
                            </div>
                            <div className="resource-actions connection-card-actions">
                                <button onClick={() => onEdit(item)}>编辑</button>
                                <button onClick={() => void onBrowse(item.id, item.schema_name || undefined)}>结构浏览</button>
                                <button className="danger" onClick={() => void onDelete(item.id)}>
                                    <Trash2 size={14} />
                                </button>
                            </div>
                        </div>
                    ))
                ) : (
                    <div className="empty-box">
                        {hasFilter && totalConnections > 0
                            ? '当前筛选条件下没有匹配的连接，可以清空搜索后查看全部。'
                            : '当前还没有加载到任何外部连接，可以先新建一个连接。'}
                    </div>
                )}
            </div>
        </div>
    );
}
