import {
    BarChart3,
    Database,
    DatabaseZap,
    FileCode2,
    FolderTree,
} from 'lucide-react';
import type { DataCenterTabKey } from './types';

export const DATASET_PARAM_EXAMPLE = `{
  "tenant_id": 1001,
  "start_date": "2026-01-01",
  "status": "paid"
}`;

export const DATASET_PARAM_SQL_EXAMPLE = `SELECT *
FROM orders
WHERE tenant_id = :tenant_id
  AND created_at >= :start_date
  AND status = :status
  AND org_name = '{CURRENT_ORG_NAME}'`;

export const TAB_ROUTE_MAP: Record<DataCenterTabKey, string> = {
    connections: '/integrations/data-center/connections',
    datasets: '/integrations/data-center/datasets',
    reports: '/integrations/data-center/applications',
    dictionaries: '/integrations/data-center/dictionaries',
    categories: '/integrations/data-center/categories',
};

export const TAB_META: Array<{
    key: DataCenterTabKey;
    label: string;
    title: string;
    detail: string;
    icon: typeof Database;
}> = [
    {
        key: 'connections',
        label: '外部连接',
        title: '连接层',
        detail: '统一维护外部数据库连接、Schema 和表结构，作为数据中心的接入入口。',
        icon: Database,
    },
    {
        key: 'datasets',
        label: '数据集建模',
        title: '数据层',
        detail: '通过 SQL 构建可复用的数据集模型，沉淀字段结构、参数和校验结果。',
        icon: FileCode2,
    },
    {
        key: 'reports',
        label: '数据应用',
        title: '应用层',
        detail: '把数据集包装成可运行、可筛选、可导出的数据应用和分析视图。',
        icon: BarChart3,
    },
    {
        key: 'categories',
        label: '分类管理',
        title: '分类层',
        detail: '以树形结构组织数据应用分类，便于快速定位和管理应用。',
        icon: FolderTree,
    },
    {
        key: 'dictionaries',
        label: '业务字典',
        title: '映射层',
        detail: '统一维护人工业务字典与动态字典来源，为下拉选项、编码映射和字段翻译提供基础能力。',
        icon: DatabaseZap,
    },
];
