import {
    BarChart3,
    Database,
    DatabaseZap,
    FileCode2,
    RefreshCw,
} from 'lucide-react';
import type { DataCenterTabKey } from '../types';

type DataCenterTabsProps = {
    activeTab: DataCenterTabKey;
    canManageReporting: boolean;
    loading: boolean;
    onSelectTab: (tab: DataCenterTabKey) => void;
    onRefresh: () => void;
};

export default function DataCenterTabs({
    activeTab,
    canManageReporting,
    loading,
    onSelectTab,
    onRefresh,
}: DataCenterTabsProps) {
    return (
        <div className="reporting-tabs">
            {canManageReporting && (
                <button className={`reporting-tab ${activeTab === 'connections' ? 'active' : ''}`} onClick={() => onSelectTab('connections')}>
                    <Database size={16} />
                    外部连接
                </button>
            )}
            {canManageReporting && (
                <button className={`reporting-tab ${activeTab === 'datasets' ? 'active' : ''}`} onClick={() => onSelectTab('datasets')}>
                    <FileCode2 size={16} />
                    数据集建模
                </button>
            )}
            <button className={`reporting-tab ${activeTab === 'reports' ? 'active' : ''}`} onClick={() => onSelectTab('reports')}>
                <BarChart3 size={16} />
                报表设计
            </button>
            {canManageReporting && (
                <button className={`reporting-tab ${activeTab === 'dictionaries' ? 'active' : ''}`} onClick={() => onSelectTab('dictionaries')}>
                    <DatabaseZap size={16} />
                    业务字典
                </button>
            )}
            <button className="btn-outline reporting-tab-action" onClick={onRefresh} disabled={loading}>
                <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
                {loading ? '刷新中...' : '刷新'}
            </button>
        </div>
    );
}
