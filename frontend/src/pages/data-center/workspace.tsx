import { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import Select from '../../components/common/Select';
import {
    getBusinessDictionaries,
    getBusinessDictionaryItems,
    createReportingConnection,
    createReportingDataset,
    createReportingReport,
    deleteReportingConnection,
    deleteReportingDataset,
    deleteReportingReport,
    getDataDictionaries,
    getDataDictionaryItems,
    getReportingConnectionMetadata,
    getReportingConnectionSchemas,
    getReportingConnectionTables,
    getReportingConnections,
    getReportingDatasets,
    getReportingReports,
    getReportingReportCategories,
    getReportingReportCategoriesTree,
    getReportingTableColumns,
    previewReportingDatasetDraft,
    previewReportingReportDraft,
    testReportingConnection,
    updateReportingConnection,
    updateReportingDataset,
    updateReportingReport,
    validateReportingDataset,
    validateReportingDatasetDraft,
} from '../../services/api';
import { TAB_ROUTE_MAP } from './config';
import ConnectionModal from './components/ConnectionModal';
import ConnectionsSection from './components/ConnectionsSection';
import DataCenterHero from './components/DataCenterHero';
import DataCenterSectionHero from './components/DataCenterSectionHero';
import DataCenterTabs from './components/DataCenterTabs';
import DatasetsSection from './components/DatasetsSection';
import DatasetModal from './components/DatasetModal';
import ReportModal from './components/ReportModal';
import ReportsSection from './components/ReportsSection';
import CategoriesSection from './components/CategoriesSection';
import type {
    ChartType,
    Connection,
    ConnectionFormState,
    ConnectionMetadata,
    DataDictionary,
    DataDictionaryItem,
    DataCenterTabKey,
    Dataset,
    DatasetFormState,
    DatasetValidation,
    QueryResult,
    Report,
    ReportCategory,
    ReportColumnConfig,
    ReportConfig,
    ReportFilter,
    ReportFormState,
    TableEntry,
    TableStyleConfig,
} from './types';
import {
    createEmptyFilter,
    mergeReportColumnsWithDataset,
    formatCell,
    inferFiltersFromDataset,
    normalizeFilter,
    parseJson,
    parseDatasetColumns,
    sanitizeFilters,
    sanitizeReportColumns,
} from './utils';
import { getAuthUser } from '../../utils/authStorage';
import './DataCenter.css';

type ReportDictionarySource = {
    id?: number | string | null;
    key?: string | null;
    name?: string | null;
    description?: string | null;
    category?: string | null;
    is_active?: boolean | null;
};

type ReportDictionaryItemSource = {
    code?: string | number | null;
    label?: string | null;
    value?: string | number | null;
    path?: string | null;
};

type ApiErrorLike = {
    response?: {
        data?: {
            detail?: unknown;
            message?: unknown;
        };
    };
    message?: unknown;
};

const getErrorMessage = (error: unknown, fallback: string) => {
    const candidate = error as ApiErrorLike;
    const message = candidate?.response?.data?.detail ?? candidate?.response?.data?.message ?? candidate?.message;
    return typeof message === 'string' ? message : fallback;
};

const toReportDictionary = (dictionary: ReportDictionarySource): DataDictionary => ({
    id: -Number(dictionary.id),
    key: String(dictionary.key || ''),
    name: String(dictionary.name || ''),
    source_type: 'static',
    description: dictionary.description ?? null,
    category: dictionary.category ?? null,
    is_active: Boolean(dictionary.is_active),
});

const toReportDictionaryItems = (items: ReportDictionaryItemSource[]): DataDictionaryItem[] =>
    (items || []).map((item) => ({
        key: String(item.code || ''),
        label: String(item.label || ''),
        value: item.value == null ? null : String(item.value),
        path: item.path == null ? null : String(item.path),
        raw: null,
    }));

export default function DataCenterWorkspace({
    initialTab,
    pageMode,
}: {
    initialTab: DataCenterTabKey;
    pageMode?: DataCenterTabKey;
}) {
    const location = useLocation();
    const navigate = useNavigate();
    const user = getAuthUser<{ role?: string; api_keys?: string[] }>() || {};
    const isAdmin = user?.role === 'admin';
    const apiKeys = Array.isArray(user?.api_keys) ? user.api_keys : [];
    const canManageReporting = isAdmin || apiKeys.includes('reporting.manage');

    const [loading, setLoading] = useState(false);

    const [connections, setConnections] = useState<Connection[]>([]);
    const [datasets, setDatasets] = useState<Dataset[]>([]);
    const [reports, setReports] = useState<Report[]>([]);
    const [categories, setCategories] = useState<ReportCategory[]>([]);
    const [categoryTree, setCategoryTree] = useState<ReportCategory[]>([]);
    const [expandedCategoryIds, setExpandedCategoryIds] = useState<Set<number>>(new Set());
    const [selectedCategoryId, setSelectedCategoryId] = useState<number | null>(null);
    const [dictionaries, setDictionaries] = useState<DataDictionary[]>([]);
    const [dictionaryItemsById, setDictionaryItemsById] = useState<Record<number, DataDictionaryItem[]>>({});

    const [tables, setTables] = useState<TableEntry[]>([]);
    const [selectedConnectionId, setSelectedConnectionId] = useState<number | null>(null);
    const [selectedSchemaName, setSelectedSchemaName] = useState<string>('');
    const [connectionMetadata, setConnectionMetadata] = useState<ConnectionMetadata | null>(null);
    const [connectionSchemas, setConnectionSchemas] = useState<string[]>([]);
    const [connectionDrawerOpen, setConnectionDrawerOpen] = useState(false);
    const [connectionSearch, setConnectionSearch] = useState('');
    const [connectionSaving, setConnectionSaving] = useState(false);
    const [connectionTesting, setConnectionTesting] = useState(false);
    const [connectionValidationErrors, setConnectionValidationErrors] = useState<string[]>([]);
    const [connectionTestResult, setConnectionTestResult] = useState<{ success: boolean; message: string; metadata?: Record<string, unknown> } | null>(null);
    const [tableBrowserLoading, setTableBrowserLoading] = useState(false);
    const [datasetResult, setDatasetResult] = useState<QueryResult | null>(null);
    const [datasetPreviewLoading, setDatasetPreviewLoading] = useState(false);
    const [datasetPreviewError, setDatasetPreviewError] = useState<string | null>(null);
    const [datasetValidation, setDatasetValidation] = useState<DatasetValidation | null>(null);
    const [datasetValidationLoading, setDatasetValidationLoading] = useState(false);
    const [datasetSearch, setDatasetSearch] = useState('');
    const [reportSearch, setReportSearch] = useState('');
    const [reportPreviewResult, setReportPreviewResult] = useState<QueryResult | null>(null);
    const [reportPreviewLoading, setReportPreviewLoading] = useState(false);
    const [reportPreviewError, setReportPreviewError] = useState<string | null>(null);

    const [editingConnectionId, setEditingConnectionId] = useState<number | null>(null);
    const [editingDatasetId, setEditingDatasetId] = useState<number | null>(null);
    const [editingReportId, setEditingReportId] = useState<number | null>(null);
    const [connectionModalOpen, setConnectionModalOpen] = useState(false);
    const [datasetModalOpen, setDatasetModalOpen] = useState(false);
    const [reportModalOpen, setReportModalOpen] = useState(false);

    const tab = useMemo<DataCenterTabKey>(() => {
        if (pageMode) return pageMode;
        const pathname = location.pathname;
        if (pathname.endsWith('/connections')) return 'connections';
        if (pathname.endsWith('/datasets')) return 'datasets';
        if (pathname.endsWith('/applications')) return 'reports';
        if (pathname.endsWith('/categories')) return 'categories';
        return initialTab;
    }, [initialTab, location.pathname, pageMode]);

    const switchTab = (nextTab: DataCenterTabKey) => {
        if (location.pathname === TAB_ROUTE_MAP[nextTab]) return;
        navigate(TAB_ROUTE_MAP[nextTab]);
    };

    const sectionHero = useMemo(() => {
        if (tab === 'connections') {
            return {
                eyebrow: 'Connections',
                title: '外部连接',
                description: '这里专门负责接入外部数据库、维护连接参数，并浏览 Schema、表和字段结构，不再混入数据建模和应用运行内容。',
                aside: (
                    <>
                        <div className="overview-card active">
                            <span className="overview-card-content">
                                <span className="overview-card-label">连接数</span>
                                <span className="overview-card-title">{connections.length}</span>
                                <span className="overview-card-detail">当前已纳入数据中心管理的外部连接。</span>
                            </span>
                        </div>
                        <div className="overview-card active">
                            <span className="overview-card-content">
                                <span className="overview-card-label">当前浏览</span>
                                <span className="overview-card-title">{connectionMetadata?.database_name || '未选择'}</span>
                                <span className="overview-card-detail">支持按 Schema 深入查看结构。</span>
                            </span>
                        </div>
                    </>
                ),
            };
        }
        if (tab === 'datasets') {
            return {
                eyebrow: 'Datasets',
                title: '数据集建模',
                description: '这里专门负责 SQL 数据集设计、参数模板、结果预览与结构校验，作为数据中心的数据建模层。',
                aside: (
                    <>
                        <div className="overview-card active">
                            <span className="overview-card-content">
                                <span className="overview-card-label">数据集数</span>
                                <span className="overview-card-title">{datasets.length}</span>
                                <span className="overview-card-detail">已经沉淀并可复用的数据集模型。</span>
                            </span>
                        </div>
                        <div className="overview-card active">
                            <span className="overview-card-content">
                                <span className="overview-card-label">关联连接</span>
                                <span className="overview-card-title">{connections.length}</span>
                                <span className="overview-card-detail">可作为建模来源的外部数据库连接。</span>
                            </span>
                        </div>
                    </>
                ),
            };
        }
        if (tab === 'categories') {
            return {
                eyebrow: 'Categories',
                title: '分类管理',
                description: '以树形结构组织报表分类，便于快速定位和管理报表。',
                aside: (
                    <>
                        <div className="overview-card active">
                            <span className="overview-card-content">
                                <span className="overview-card-label">分类数</span>
                                <span className="overview-card-title">{categories.length}</span>
                                <span className="overview-card-detail">已建立的报表分类。</span>
                            </span>
                        </div>
                    </>
                ),
            };
        }
        return {
            eyebrow: 'Applications',
            title: '报表设计',
            description: '将数据集封装成业务应用，支持筛选器、图表配置和数据导出。',
            aside: (
                <>
                    <div className="overview-card active">
                        <span className="overview-card-content">
                            <span className="overview-card-label">报表数</span>
                            <span className="overview-card-title">{reports.length}</span>
                            <span className="overview-card-detail">可运行、可导出的报表模板。</span>
                        </span>
                    </div>
                    <div className="overview-card active">
                        <span className="overview-card-content">
                            <span className="overview-card-label">关联数据集</span>
                            <span className="overview-card-title">{datasets.length}</span>
                            <span className="overview-card-detail">可作为报表数据来源的数据集模型。</span>
                        </span>
                    </div>
                </>
            ),
        };
    }, [connectionMetadata?.database_name, connections.length, datasets.length, reports.length, categories.length, tab]);

    const pageVariantClass = pageMode
        ? tab === 'connections'
            ? 'data-center-page-connections'
            : tab === 'datasets'
              ? 'data-center-page-datasets'
              : tab === 'categories'
                ? 'data-center-page-categories'
                : 'data-center-page-applications'
        : 'data-center-page-hub';

    const [connectionForm, setConnectionForm] = useState<ConnectionFormState>({
        name: '',
        db_type: 'postgresql',
        host: '',
        port: '5432',
        database_name: '',
        schema_name: 'public',
        username: '',
        password: '',
        description: '',
        connection_options: '{\n  "connect_timeout": 10\n}',
        is_active: true,
    });

    const [datasetForm, setDatasetForm] = useState<DatasetFormState>({
        connection_id: '',
        name: '',
        description: '',
        sql_text: 'SELECT *\nFROM your_table\nLIMIT 100',
        params_json: '{\n}',
        row_limit: '200',
        is_active: true,
    });

    const [reportForm, setReportForm] = useState<ReportFormState>({
        dataset_id: '',
        name: '',
        description: '',
        report_type: 'table',
        category_id: '',
        visible_columns: '',
        column_configs_json: '[]',
        default_limit: '100',
        aggregate_scope: 'returned',
        filters_json: '[]',
        chart_enabled: false,
        chart_type: 'bar' as ChartType,
        category_field: '',
        value_field: '',
        aggregate: 'sum' as 'sum' | 'count',
        table_style_json: '{}',
        is_active: true,
    });

    const loadData = useCallback(async () => {
        setLoading(true);
        try {
            if (canManageReporting) {
                const [connectionData, datasetData, reportData, categoryData, categoryTreeData] = await Promise.all([
                    getReportingConnections(),
                    getReportingDatasets(),
                    getReportingReports(),
                    getReportingReportCategories(),
                    getReportingReportCategoriesTree(),
                ]);
                setConnections(connectionData);
                setDatasets(datasetData);
                setReports(reportData);
                setCategories(categoryData);
                setCategoryTree(categoryTreeData || []);
                const allIds = new Set<number>();
                const collect = (items: ReportCategory[]) => {
                    for (const item of items) {
                        allIds.add(item.id);
                        if (item.children?.length) collect(item.children);
                    }
                };
                if (categoryTreeData?.length) collect(categoryTreeData);
                setExpandedCategoryIds(allIds);
                try {
                    const [sourceDictionaries, businessDictionaries] = await Promise.all([
                        getDataDictionaries(),
                        getBusinessDictionaries(),
                    ]);
                    setDictionaries([
                        ...((sourceDictionaries || []) as DataDictionary[]),
                        ...((businessDictionaries || []).map(toReportDictionary)),
                    ]);
                } catch {
                    setDictionaries([]);
                }
            } else {
                setReports(await getReportingReports());
            }
        } catch (error: unknown) {
            alert(getErrorMessage(error, '加载失败'));
        } finally {
            setLoading(false);
        }
    }, [canManageReporting]);

    const loadConnectionBrowser = async (connectionId: number, schemaName?: string | null) => {
        setTableBrowserLoading(true);
        setSelectedConnectionId(connectionId);
        try {
            const metadataRes = await getReportingConnectionMetadata(connectionId);
            setConnectionMetadata(metadataRes);
            const resolvedSchema = schemaName || metadataRes.current_schema || metadataRes.schema_name || '';
            setSelectedSchemaName(resolvedSchema);

            try {
                const schemaRes = await getReportingConnectionSchemas(connectionId);
                setConnectionSchemas(schemaRes.schemas || []);
            } catch {
                setConnectionSchemas(metadataRes.available_schemas || []);
            }

            const tableRes = await getReportingConnectionTables(connectionId, resolvedSchema || undefined);
            setTables(tableRes.tables || []);
        } catch (error: unknown) {
            setConnectionMetadata(null);
            setConnectionSchemas([]);
            setSelectedSchemaName('');
            setTables([]);
            alert(getErrorMessage(error, '读取连接信息失败'));
        } finally {
            setTableBrowserLoading(false);
        }
    };

    const runDatasetValidation = useCallback(async (silent = false) => {
        if (!datasetForm.connection_id || !datasetForm.sql_text.trim()) {
            setDatasetValidation(null);
            return;
        }
        setDatasetValidationLoading(true);
        try {
            const result = await validateReportingDatasetDraft({
                connection_id: Number(datasetForm.connection_id),
                sql_text: datasetForm.sql_text,
                params_json: datasetForm.params_json,
                row_limit: Number(datasetForm.row_limit || 200),
            });
            setDatasetValidation(result);
        } catch (error: unknown) {
            setDatasetValidation(null);
            if (!silent) {
                alert(getErrorMessage(error, '校验失败'));
            }
        } finally {
            setDatasetValidationLoading(false);
        }
    }, [datasetForm]);

    useEffect(() => {
        void loadData();
    }, [loadData]);

    useEffect(() => {
        if (tab !== 'connections') {
            setConnectionDrawerOpen(false);
        }
    }, [tab]);

    useEffect(() => {
        if (!selectedConnectionId) return;
        const current = connections.find((item) => item.id === selectedConnectionId);
        if (!current) {
            setSelectedConnectionId(null);
            setConnectionMetadata(null);
            setConnectionSchemas([]);
            setSelectedSchemaName('');
            setTables([]);
            setConnectionDrawerOpen(false);
        }
    }, [connections, selectedConnectionId]);

    const toggleCategoryExpand = (id: number) => {
        setExpandedCategoryIds((prev) => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    const datasetPreviewColumns = useMemo(() => {
        const source = datasetResult?.columns || [];
        return source.map((column) => ({
            key: column.name,
            title: `${column.name} (${column.type})`,
            render: (value: unknown) => <span title={formatCell(value)}>{formatCell(value)}</span>,
        }));
    }, [datasetResult]);

    const selectedDatasetForForm = useMemo(
        () => datasets.find((item) => String(item.id) === reportForm.dataset_id),
        [datasets, reportForm.dataset_id]
    );
    const selectedDatasetColumns = useMemo(
        () => parseDatasetColumns(selectedDatasetForForm),
        [selectedDatasetForForm]
    );
    const selectedConnectionForDataset = useMemo(
        () => connections.find((item) => String(item.id) === datasetForm.connection_id),
        [connections, datasetForm.connection_id]
    );
    const filteredConnections = useMemo(() => {
        const keyword = connectionSearch.trim().toLowerCase();
        if (!keyword) return connections;
        return connections.filter((item) =>
            [item.name, item.db_type, item.host, item.database_name, item.schema_name]
                .filter(Boolean)
                .some((field) => String(field).toLowerCase().includes(keyword))
        );
    }, [connectionSearch, connections]);

    const filteredDatasets = useMemo(() => {
        const keyword = datasetSearch.trim().toLowerCase();
        if (!keyword) return datasets;
        return datasets.filter((item) =>
            [item.name, item.connection_name, item.description, item.sql_text]
                .filter(Boolean)
                .some((field) => String(field).toLowerCase().includes(keyword))
        );
    }, [datasetSearch, datasets]);

    const filteredReports = useMemo(() => {
        let result = selectedCategoryId !== null
            ? reports.filter((item) => item.category_id === selectedCategoryId)
            : reports;
        const keyword = reportSearch.trim().toLowerCase();
        if (keyword) {
            result = result.filter((item) =>
                [item.name, item.dataset_name, item.description]
                    .filter(Boolean)
                    .some((field) => String(field).toLowerCase().includes(keyword))
            );
        }
        return result;
    }, [reports, selectedCategoryId, reportSearch]);

    const reportFormFilters = useMemo(
        () => parseJson<ReportFilter[]>(reportForm.filters_json, []).map((filter) => normalizeFilter(filter)),
        [reportForm.filters_json]
    );
    const reportFormColumns = useMemo(
        () => parseJson<ReportColumnConfig[]>(reportForm.column_configs_json, []),
        [reportForm.column_configs_json]
    );

    // 按需加载字典项数据，供报表预览中字典映射使用
    const requiredDictionaryIds = useMemo(() => {
        const ids = new Set<number>();
        reportFormColumns.forEach((col) => {
            if (typeof col.dictionary_id === 'number') ids.add(col.dictionary_id);
        });
        reportFormFilters.forEach((filter) => {
            if (typeof filter.dictionary_id === 'number') ids.add(filter.dictionary_id);
        });
        return Array.from(ids);
    }, [reportFormColumns, reportFormFilters]);

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

    const syncReportFilters = (nextFilters: ReportFilter[]) => {
        setReportForm((prev) => ({
            ...prev,
            filters_json: JSON.stringify(nextFilters, null, 2),
        }));
    };

    const syncReportColumns = (nextColumns: ReportColumnConfig[]) => {
        const sanitizedColumns = sanitizeReportColumns(nextColumns);
        setReportForm((prev) => ({
            ...prev,
            visible_columns: sanitizedColumns.filter((column) => column.visible).map((column) => column.key).join(', '),
            column_configs_json: JSON.stringify(sanitizedColumns, null, 2),
        }));
    };

    const resetConnection = () => {
        setEditingConnectionId(null);
        setConnectionValidationErrors([]);
        setConnectionTestResult(null);
        setConnectionForm({
            name: '',
            db_type: 'postgresql',
            host: '',
            port: '5432',
            database_name: '',
            schema_name: 'public',
            username: '',
            password: '',
            description: '',
            connection_options: '{\n  "connect_timeout": 10\n}',
            is_active: true,
        });
    };

    const openConnectionCreateModal = () => {
        resetConnection();
        setConnectionModalOpen(true);
    };

    const openConnectionEditModal = (connection: Connection) => {
        setConnectionValidationErrors([]);
        setConnectionTestResult(null);
        setConnectionForm({
            name: connection.name,
            db_type: connection.db_type,
            host: connection.host || '',
            port: connection.port ? String(connection.port) : '',
            database_name: connection.database_name,
            schema_name: connection.schema_name || '',
            username: connection.username || '',
            password: '',
            description: connection.description || '',
            connection_options: connection.connection_options || '{\n}',
            is_active: connection.is_active,
        });
        setEditingConnectionId(connection.id);
        setConnectionModalOpen(true);
    };

    const buildConnectionPayload = () => {
        const errors: string[] = [];
        const normalizedType = (connectionForm.db_type || '').trim().toLowerCase();
        const isSqlite = normalizedType === 'sqlite';
        const trimmedName = connectionForm.name.trim();
        const trimmedDatabaseName = connectionForm.database_name.trim();
        const trimmedHost = connectionForm.host.trim();
        const trimmedSchema = connectionForm.schema_name.trim();
        const trimmedUsername = connectionForm.username.trim();
        const trimmedDescription = connectionForm.description.trim();
        const trimmedOptions = connectionForm.connection_options.trim();

        if (!trimmedName) errors.push('连接名称不能为空。');
        if (!trimmedDatabaseName) errors.push(isSqlite ? 'SQLite 数据库文件路径不能为空。' : '数据库名不能为空。');
        if (!isSqlite && !trimmedHost) errors.push('非 SQLite 连接必须填写 Host。');
        if (connectionForm.port) {
            const numericPort = Number(connectionForm.port);
            if (!Number.isInteger(numericPort) || numericPort <= 0) errors.push('端口必须是正整数。');
        }

        let normalizedOptions: string | null = null;
        if (trimmedOptions) {
            try {
                normalizedOptions = JSON.stringify(JSON.parse(trimmedOptions), null, 2);
            } catch {
                errors.push('连接选项 JSON 格式不正确。');
            }
        }

        setConnectionValidationErrors(errors);
        if (errors.length) return null;

        return {
            name: trimmedName,
            db_type: normalizedType,
            host: isSqlite ? null : (trimmedHost || null),
            port: isSqlite ? null : (connectionForm.port ? Number(connectionForm.port) : null),
            database_name: trimmedDatabaseName,
            schema_name: isSqlite ? null : (trimmedSchema || null),
            username: isSqlite ? null : (trimmedUsername || null),
            password: connectionForm.password || null,
            description: trimmedDescription || null,
            connection_options: normalizedOptions,
            is_active: connectionForm.is_active,
        };
    };

    const runConnectionTest = async () => {
        const payload = buildConnectionPayload();
        if (!payload) {
            setConnectionTestResult(null);
            return;
        }
        setConnectionTesting(true);
        try {
            const result = await testReportingConnection(payload);
            setConnectionTestResult(result);
        } catch (error: unknown) {
            setConnectionTestResult({
                success: false,
                message: getErrorMessage(error, '测试失败'),
            });
        } finally {
            setConnectionTesting(false);
        }
    };

    const resetDataset = () => {
        setEditingDatasetId(null);
        setDatasetResult(null);
        setDatasetValidation(null);
        setDatasetPreviewError(null);
        setDatasetPreviewLoading(false);
        setDatasetForm({
            connection_id: connections[0] ? String(connections[0].id) : '',
            name: '',
            description: '',
            sql_text: 'SELECT *\nFROM your_table\nLIMIT 100',
            params_json: '{\n}',
            row_limit: '200',
            is_active: true,
        });
    };

    const openDatasetCreateModal = () => {
        resetDataset();
        setDatasetModalOpen(true);
    };

    const openDatasetEditModal = (dataset: Dataset) => {
        setEditingDatasetId(dataset.id);
        setDatasetValidation(null);
        setDatasetForm({
            connection_id: String(dataset.connection_id),
            name: dataset.name,
            description: dataset.description || '',
            sql_text: dataset.sql_text,
            params_json: dataset.params_json || '{\n}',
            row_limit: String(dataset.row_limit || 200),
            is_active: dataset.is_active,
        });
        setDatasetModalOpen(true);
    };

    const resetReport = () => {
        const initialDataset = datasets[0];
        const initialColumns = mergeReportColumnsWithDataset(parseDatasetColumns(initialDataset));
        setEditingReportId(null);
        setReportForm({
            dataset_id: datasets[0] ? String(datasets[0].id) : '',
            name: '',
            description: '',
            report_type: 'table',
            category_id: '',
            visible_columns: initialColumns.filter((column) => column.visible).map((column) => column.key).join(', '),
            column_configs_json: JSON.stringify(initialColumns, null, 2),
            default_limit: '100',
            aggregate_scope: 'returned',
            filters_json: '[]',
            chart_enabled: false,
            chart_type: 'bar',
            category_field: '',
            value_field: '',
            aggregate: 'sum',
            table_style_json: '{}',
            is_active: true,
        });
        setReportPreviewResult(null);
        setReportPreviewError(null);
        setReportPreviewLoading(false);
    };

    const openReportCreateModal = () => {
        resetReport();
        setReportModalOpen(true);
    };

    const saveConnection = async () => {
        const payload = buildConnectionPayload();
        if (!payload) return;
        setConnectionSaving(true);
        try {
            if (editingConnectionId) {
                await updateReportingConnection(editingConnectionId, payload);
            } else {
                await createReportingConnection(payload);
            }
            setConnectionModalOpen(false);
            resetConnection();
            await loadData();
        } catch (error: unknown) {
            alert(getErrorMessage(error, '保存连接失败'));
        } finally {
            setConnectionSaving(false);
        }
    };

    const saveDataset = async () => {
        try {
            if (datasetForm.params_json?.trim()) {
                JSON.parse(datasetForm.params_json);
            }
            const payload = {
                ...datasetForm,
                connection_id: Number(datasetForm.connection_id),
                row_limit: Number(datasetForm.row_limit || 200),
            };
            if (editingDatasetId) {
                await updateReportingDataset(editingDatasetId, payload);
                const validation = await validateReportingDataset(editingDatasetId);
                setDatasetValidation(validation);
            } else {
                await createReportingDataset(payload);
            }
            setDatasetModalOpen(false);
            resetDataset();
            await loadData();
        } catch (error: unknown) {
            alert(getErrorMessage(error, '保存数据集失败'));
        }
    };

    const saveReport = async () => {
        try {
            const filters = sanitizeFilters(reportFormFilters);
            const columns = sanitizeReportColumns(reportFormColumns);
            const tableStyle = parseJson<TableStyleConfig>(reportForm.table_style_json, {});
            const payload = {
                dataset_id: Number(reportForm.dataset_id),
                name: reportForm.name,
                description: reportForm.description || null,
                report_type: reportForm.report_type,
                category_id: reportForm.category_id ? Number(reportForm.category_id) : null,
                config_json: JSON.stringify({
                    visible_columns: columns.filter((column) => column.visible).map((column) => column.key),
                    columns,
                    aggregate_scope: reportForm.aggregate_scope,
                    filters,
                    chart: {
                        enabled: reportForm.chart_enabled,
                        chart_type: reportForm.chart_type,
                        category_field: reportForm.category_field,
                        value_field: reportForm.value_field,
                        aggregate: reportForm.aggregate,
                    },
                    table_style: tableStyle,
                } satisfies ReportConfig),
                is_active: reportForm.is_active,
            };

            if (editingReportId) {
                await updateReportingReport(editingReportId, payload);
            } else {
                await createReportingReport(payload);
            }
            setReportModalOpen(false);
            resetReport();
            await loadData();
        } catch (error: unknown) {
            alert(getErrorMessage(error, '保存报表失败'));
        }
    };

    const runDatasetDraftPreview = useCallback(async (silent = false) => {
        if (!datasetForm.connection_id || !datasetForm.sql_text.trim()) {
            setDatasetResult(null);
            setDatasetPreviewError(null);
            return;
        }

        setDatasetPreviewLoading(true);
        setDatasetPreviewError(null);

        try {
            const params = datasetForm.params_json?.trim() ? JSON.parse(datasetForm.params_json) : {};
            const result = await previewReportingDatasetDraft({
                connection_id: Number(datasetForm.connection_id),
                sql_text: datasetForm.sql_text,
                params_json: datasetForm.params_json,
                row_limit: Number(datasetForm.row_limit || 200),
                params,
                limit: Number(datasetForm.row_limit || 200),
            });
            setDatasetResult(result);
            setDatasetPreviewError(null);
        } catch (error: unknown) {
            const message = getErrorMessage(error, '预览失败');
            setDatasetResult(null);
            setDatasetPreviewError(message);
            if (!silent) {
                alert(message);
            }
        } finally {
            setDatasetPreviewLoading(false);
        }
    }, [datasetForm]);

    const runReportDraftPreview = useCallback(async (silent = false) => {
        if (!reportForm.dataset_id) {
            setReportPreviewResult(null);
            setReportPreviewError(null);
            return;
        }

        setReportPreviewLoading(true);
        setReportPreviewError(null);

        try {
            const filters = sanitizeFilters(reportFormFilters);
            const columns = sanitizeReportColumns(reportFormColumns);
            const tableStyle = parseJson<TableStyleConfig>(reportForm.table_style_json, {});
            const configJson = JSON.stringify({
                visible_columns: columns.filter((column) => column.visible).map((column) => column.key),
                columns,
                aggregate_scope: reportForm.aggregate_scope,
                filters,
                chart: {
                    enabled: reportForm.chart_enabled,
                    chart_type: reportForm.chart_type,
                    category_field: reportForm.category_field,
                    value_field: reportForm.value_field,
                    aggregate: reportForm.aggregate,
                },
                table_style: tableStyle,
            } satisfies ReportConfig);

            const result = await previewReportingReportDraft({
                dataset_id: Number(reportForm.dataset_id),
                report_type: reportForm.report_type,
                config_json: configJson,
            });
            setReportPreviewResult(result);
            setReportPreviewError(null);
        } catch (error: unknown) {
            const message = getErrorMessage(error, '预览失败');
            setReportPreviewResult(null);
            setReportPreviewError(message);
            if (!silent) {
                alert(message);
            }
        } finally {
            setReportPreviewLoading(false);
        }
    }, [reportForm, reportFormColumns, reportFormFilters]);

    const editReport = (report: Report) => {
        const config = parseJson<ReportConfig>(report.config_json, {});
        setEditingReportId(report.id);
        switchTab('reports');
        setReportForm({
            dataset_id: String(report.dataset_id),
            name: report.name,
            description: report.description || '',
            report_type: report.report_type || 'table',
            category_id: report.category_id ? String(report.category_id) : '',
            visible_columns: (config.visible_columns || []).join(', '),
            column_configs_json: JSON.stringify(
                mergeReportColumnsWithDataset(
                    parseDatasetColumns(datasets.find((item) => item.id === report.dataset_id)),
                    config.columns || (config.visible_columns || []).map((key) => ({ key, label: key, visible: true }))
                ),
                null,
                2
            ),
            default_limit: String(config.default_limit || 100),
            aggregate_scope: config.aggregate_scope || 'returned',
            filters_json: JSON.stringify(config.filters || inferFiltersFromDataset(datasets.find((item) => item.id === report.dataset_id)), null, 2),
            chart_enabled: !!config.chart?.enabled,
            chart_type: config.chart?.chart_type || 'bar',
            category_field: config.chart?.category_field || '',
            value_field: config.chart?.value_field || '',
            aggregate: config.chart?.aggregate || 'sum',
            table_style_json: JSON.stringify(config.table_style || {}, null, 2),
            is_active: report.is_active,
        });
        setReportModalOpen(true);
    };

    const updateReportFilter = (index: number, patch: Partial<ReportFilter>) => {
        syncReportFilters(
            reportFormFilters.map((filter, filterIndex) => {
                if (filterIndex !== index) return filter;
                const nextFilter = normalizeFilter({ ...filter, ...patch });
                if (nextFilter.type !== 'select') {
                    nextFilter.options = [];
                }
                return nextFilter;
            })
        );
    };

    const addReportFilter = () => {
        syncReportFilters([...reportFormFilters, createEmptyFilter()]);
    };

    const removeReportFilter = (index: number) => {
        syncReportFilters(reportFormFilters.filter((_, filterIndex) => filterIndex !== index));
    };

    const addReportFilterOption = (filterIndex: number) => {
        syncReportFilters(
            reportFormFilters.map((filter, index) =>
                index === filterIndex
                    ? {
                          ...filter,
                          options: [...(filter.options || []), { label: '', value: '' }],
                      }
                    : filter
            )
        );
    };

    const updateReportFilterOption = (
        filterIndex: number,
        optionIndex: number,
        patch: Partial<{ label: string; value: string }>
    ) => {
        syncReportFilters(
            reportFormFilters.map((filter, index) =>
                index === filterIndex
                    ? {
                          ...filter,
                          options: (filter.options || []).map((option, currentOptionIndex) =>
                              currentOptionIndex === optionIndex ? { ...option, ...patch } : option
                          ),
                      }
                    : filter
            )
        );
    };

    const removeReportFilterOption = (filterIndex: number, optionIndex: number) => {
        syncReportFilters(
            reportFormFilters.map((filter, index) =>
                index === filterIndex
                    ? {
                          ...filter,
                          options: (filter.options || []).filter((_, currentOptionIndex) => currentOptionIndex !== optionIndex),
                      }
                    : filter
            )
        );
    };

    const handleAutoFilters = () => {
        const inferred = inferFiltersFromDataset(selectedDatasetForForm);
        syncReportFilters(inferred);
    };

    const handleReportDatasetChange = (datasetId: string) => {
        const dataset = datasets.find((item) => String(item.id) === datasetId);
        const mergedColumns = mergeReportColumnsWithDataset(parseDatasetColumns(dataset), reportFormColumns);
        setReportForm((prev) => ({
            ...prev,
            dataset_id: datasetId,
            visible_columns: mergedColumns.filter((column) => column.visible).map((column) => column.key).join(', '),
            column_configs_json: JSON.stringify(mergedColumns, null, 2),
            category_field:
                prev.category_field && mergedColumns.some((column) => column.key === prev.category_field && column.visible)
                    ? prev.category_field
                    : '',
            value_field:
                prev.value_field && mergedColumns.some((column) => column.key === prev.value_field && column.visible)
                    ? prev.value_field
                    : '',
        }));
        if (editingReportId) {
            return;
        }
        if (!reportFormFilters.length) {
            syncReportFilters(inferFiltersFromDataset(dataset));
        }
    };

    const updateReportColumn = (index: number, patch: Partial<ReportColumnConfig>) => {
        syncReportColumns(
            reportFormColumns.map((column, columnIndex) =>
                columnIndex === index ? { ...column, ...patch } : column
            )
        );
    };

    const toggleAllReportColumns = (visible: boolean) => {
        syncReportColumns(reportFormColumns.map((column) => ({ ...column, visible })));
    };

    const reorderReportGroups = (sourceGroupKey: string, targetGroupKey: string, position: 'before' | 'after') => {
        const sourceGroupName = sourceGroupKey.split(':type:')[1] || sourceGroupKey;
        const targetGroupName = targetGroupKey.split(':type:')[1] || targetGroupKey;
        if (!sourceGroupName || !targetGroupName || sourceGroupName === targetGroupName) return;

        const groupMeta = reportFormColumns.reduce((acc, col) => {
            if (col.group && !acc[col.group]) {
                acc[col.group] = {
                    order: col.group_order ?? 0,
                    parent: col.parent_group ?? null,
                };
            }
            return acc;
        }, {} as Record<string, { order: number; parent: string | null }>);

        const targetMeta = groupMeta[targetGroupName] || { order: 0, parent: null };
        const targetOrder = targetMeta.order;
        const targetParent = targetMeta.parent;
        const delta = position === 'before' ? -0.5 : 0.5;
        const newOrder = targetOrder + delta;

        const updatedColumns = reportFormColumns.map((col) => {
            if (col.group === sourceGroupName) {
                return {
                    ...col,
                    group_order: newOrder,
                    parent_group: targetParent || undefined,
                };
            }
            return col;
        });
        syncReportColumns(updatedColumns);
    };

    const moveColumnToGroup = (columnIndex: number, targetGroupKey: string | null) => {
        if (columnIndex < 0 || columnIndex >= reportFormColumns.length) return;
        const targetGroupName = targetGroupKey ? (targetGroupKey.split(':type:')[1] || targetGroupKey) : null;
        const targetParentName = targetGroupName
            ? reportFormColumns.find((column) => column.group === targetGroupName)?.parent_group || null
            : null;
        const updatedColumns = [...reportFormColumns];
        updatedColumns[columnIndex] = {
            ...updatedColumns[columnIndex],
            group: targetGroupName || undefined,
            parent_group: targetParentName || undefined,
        };
        syncReportColumns(updatedColumns);
    };

    const placeReportColumn = (
        sourceIndex: number,
        targetIndex: number,
        position: 'before' | 'after',
        targetGroupKey: string | null
    ) => {
        if (sourceIndex < 0 || sourceIndex >= reportFormColumns.length) return;
        if (targetIndex < 0 || targetIndex >= reportFormColumns.length) return;

        const nextColumns = [...reportFormColumns];
        const [current] = nextColumns.splice(sourceIndex, 1);
        if (!current) return;

        const targetGroupName = targetGroupKey ? (targetGroupKey.split(':type:')[1] || targetGroupKey) : null;
        const targetParentName = targetGroupName
            ? reportFormColumns.find((column) => column.group === targetGroupName)?.parent_group || null
            : null;

        current.group = targetGroupName || undefined;
        current.parent_group = targetParentName || undefined;

        let insertIndex = targetIndex;
        if (position === 'after') {
            insertIndex = sourceIndex < targetIndex ? targetIndex : targetIndex + 1;
        } else if (sourceIndex < targetIndex) {
            insertIndex = targetIndex - 1;
        }

        insertIndex = Math.max(0, Math.min(insertIndex, nextColumns.length));
        nextColumns.splice(insertIndex, 0, current);
        syncReportColumns(nextColumns);
    };

    const setParentGroup = (groupKey: string, parentGroupKey: string | null) => {
        const groupName = groupKey.split(':type:')[1] || groupKey;
        const parentName = parentGroupKey ? (parentGroupKey.split(':type:')[1] || parentGroupKey) : null;
        if (!groupName || groupName === parentName) return;

        const parentMap = reportFormColumns.reduce((acc, col) => {
            if (col.group && !acc[col.group]) {
                acc[col.group] = col.parent_group || null;
            }
            return acc;
        }, {} as Record<string, string | null>);

        let cursor = parentName;
        while (cursor) {
            if (cursor === groupName) return;
            cursor = parentMap[cursor] || null;
        }

        const siblingOrders = reportFormColumns
            .filter((col) => col.group && col.group !== groupName && (col.parent_group || null) === parentName)
            .map((col) => col.group_order ?? 0);
        const nextOrder = siblingOrders.length ? Math.max(...siblingOrders) + 1 : 0;

        const updatedColumns = reportFormColumns.map((col) => {
            if (col.group === groupName) {
                return {
                    ...col,
                    parent_group: parentName || undefined,
                    group_order: nextOrder,
                };
            }
            return col;
        });
        syncReportColumns(updatedColumns);
    };

    const bulkUpdateReportColumns = (keys: string[], patch: Partial<ReportColumnConfig>) => {
        if (!keys.length) return;
        const targetSet = new Set(keys);
        syncReportColumns(
            reportFormColumns.map((column) => (targetSet.has(column.key) ? { ...column, ...patch } : column))
        );
    };

    return (
        <div className={`page-container fade-in reporting-page ${pageVariantClass}`}>
            {pageMode ? (
                tab === 'connections' || tab === 'datasets' || tab === 'reports' || tab === 'categories' ? null : (
                    <DataCenterSectionHero
                        eyebrow={sectionHero.eyebrow}
                        title={sectionHero.title}
                        description={sectionHero.description}
                        aside={sectionHero.aside}
                    />
                )
            ) : (
                <>
                    <DataCenterHero activeTab={tab} canManageReporting={canManageReporting} onSelectTab={switchTab} />
                    <DataCenterTabs
                        activeTab={tab}
                        canManageReporting={canManageReporting}
                        loading={loading}
                        onSelectTab={switchTab}
                        onRefresh={() => void loadData()}
                    />
                </>
            )}

            {tab === 'connections' && canManageReporting && (
                <ConnectionsSection
                    connections={filteredConnections}
                    totalConnections={connections.length}
                    loading={loading}
                    selectedConnectionId={selectedConnectionId}
                    searchText={connectionSearch}
                    onCreate={openConnectionCreateModal}
                    onEdit={openConnectionEditModal}
                    onSearchChange={setConnectionSearch}
                    onReloadConnections={() => void loadData()}
                    onBrowse={(connectionId, schemaName) => {
                        setConnectionDrawerOpen(true);
                        void loadConnectionBrowser(connectionId, schemaName);
                    }}
                    onDelete={(connectionId) => {
                        if (!window.confirm('确定删除该连接及其下数据集和报表吗？')) return;
                        void deleteReportingConnection(connectionId)
                            .then(() => loadData())
                            .catch((error: unknown) => alert(getErrorMessage(error, '删除失败')));
                    }}
                />
            )}

            {tab === 'connections' && canManageReporting && connectionDrawerOpen ? (
                <div className="reporting-drawer-backdrop" onClick={() => setConnectionDrawerOpen(false)}>
                    <aside className="reporting-drawer" onClick={(event) => event.stopPropagation()}>
                        <div className="reporting-drawer-head">
                            <div>
                                <h3>结构浏览</h3>
                                <div className="resource-meta">
                                    {connectionMetadata
                                        ? `${connectionMetadata.db_type} / ${connectionMetadata.database_name} / 表 ${connectionMetadata.table_count} / 视图 ${connectionMetadata.view_count}`
                                        : '正在读取所选连接的结构信息...'}
                                </div>
                            </div>
                            <div className="reporting-inline-tools">
                                {selectedConnectionId ? (
                                    <>
                                        <Select
                                            value={selectedSchemaName}
                                            onChange={(v) => void loadConnectionBrowser(selectedConnectionId, v)}
                                            options={[
                                                { value: '', label: '默认 Schema' },
                                                ...connectionSchemas.map((schema) => ({ value: schema, label: schema })),
                                            ]}
                                        />
                                        <button className="btn-outline" type="button" onClick={() => {
                                            if (!selectedConnectionId) return;
                                            void loadConnectionBrowser(selectedConnectionId, selectedSchemaName || undefined);
                                        }}>
                                            刷新结构
                                        </button>
                                    </>
                                ) : null}
                                <button className="ghost-btn" type="button" onClick={() => setConnectionDrawerOpen(false)}>关闭</button>
                            </div>
                        </div>
                        {connectionMetadata ? (
                            <div className="connection-meta-grid">
                                <div className="meta-tile"><span>当前 Schema</span><strong>{connectionMetadata.current_schema || '-'}</strong></div>
                                <div className="meta-tile"><span>默认 Schema</span><strong>{connectionMetadata.schema_name || '-'}</strong></div>
                                <div className="meta-tile"><span>可用 Schema</span><strong>{connectionMetadata.available_schemas.length}</strong></div>
                                <div className="meta-tile"><span>数据库版本</span><strong title={connectionMetadata.server_version || '-'}>{connectionMetadata.server_version || '-'}</strong></div>
                            </div>
                        ) : null}
                        <div className="schema-list reporting-drawer-body">
                            {tableBrowserLoading ? (
                                <div className="preview-status loading">正在读取外部连接结构...</div>
                            ) : tables.map((table) => (
                                <div key={`${table.schema_name || ''}.${table.table_name}`} className="schema-card">
                                    <div className="schema-title">
                                        {table.schema_name ? `${table.schema_name}.` : ''}
                                        {table.table_name}
                                        <span className="schema-kind">{table.object_type || 'table'}</span>
                                    </div>
                                    <div className="schema-columns">
                                        {table.columns.length ? table.columns.map((column) => (
                                            <button
                                                key={`${table.table_name}-${column.name}`}
                                                className="schema-chip"
                                                type="button"
                                                onClick={() =>
                                                    void getReportingTableColumns(selectedConnectionId || 0, table.table_name, table.schema_name || undefined)
                                                        .then((columns) =>
                                                            setTables((prev) =>
                                                                prev.map((item) =>
                                                                    item.table_name === table.table_name && item.schema_name === table.schema_name
                                                                        ? { ...item, columns }
                                                                        : item
                                                                )
                                                            )
                                                        )
                                                        .catch((error: unknown) => alert(getErrorMessage(error, '读取字段失败')))
                                                }
                                            >
                                                {column.name}: {column.type}{column.nullable === false ? ' · not null' : ''}
                                            </button>
                                        )) : (
                                            <button
                                                className="btn-outline schema-load-btn"
                                                type="button"
                                                onClick={() =>
                                                    void getReportingTableColumns(selectedConnectionId || 0, table.table_name, table.schema_name || undefined)
                                                        .then((columns) =>
                                                            setTables((prev) =>
                                                                prev.map((item) =>
                                                                    item.table_name === table.table_name && item.schema_name === table.schema_name
                                                                        ? { ...item, columns }
                                                                        : item
                                                                )
                                                            )
                                                        )
                                                        .catch((error: unknown) => alert(getErrorMessage(error, '读取字段失败')))
                                                }
                                            >
                                                加载字段
                                            </button>
                                        )}
                                    </div>
                                </div>
                            ))}
                            {!tableBrowserLoading && !tables.length ? (
                                <div className="empty-box">当前连接下还没有读取到可用表、视图或字段。</div>
                            ) : null}
                        </div>
                    </aside>
                </div>
            ) : null}

            {tab === 'datasets' && canManageReporting && (
                <DatasetsSection
                    datasets={filteredDatasets}
                    totalDatasets={datasets.length}
                    searchText={datasetSearch}
                    onSearchChange={setDatasetSearch}
                    onCreate={openDatasetCreateModal}
                    onEdit={openDatasetEditModal}
                    onReloadDatasets={() => void loadData()}
                    onDelete={(datasetId) => {
                        if (!window.confirm('确定删除该数据集及其下报表吗？')) return;
                        void deleteReportingDataset(datasetId)
                            .then(() => loadData())
                            .catch((error: unknown) => alert(getErrorMessage(error, '删除失败')));
                    }}
                />
            )}

            {tab === 'reports' && (
                <ReportsSection
                    reports={filteredReports}
                    totalReports={reports.length}
                    searchText={reportSearch}
                    onSearchChange={setReportSearch}
                    onCreate={openReportCreateModal}
                    onEdit={editReport}
                    onReloadReports={() => void loadData()}
                    onDelete={(reportId) => {
                        if (!window.confirm('确定删除该报表吗？')) return;
                        void deleteReportingReport(reportId)
                            .then(() => loadData())
                            .catch((error: unknown) => alert(getErrorMessage(error, '删除失败')));
                    }}
                    categoryTree={categoryTree}
                    expandedCategoryIds={expandedCategoryIds}
                    selectedCategoryId={selectedCategoryId}
                    onToggleCategoryExpand={toggleCategoryExpand}
                    onSelectCategory={setSelectedCategoryId}
                />
            )}

            {tab === 'categories' && canManageReporting && (
                <CategoriesSection />
            )}

            <ConnectionModal
                open={connectionModalOpen}
                editingConnectionId={editingConnectionId}
                connectionForm={connectionForm}
                setConnectionForm={setConnectionForm}
                validationErrors={connectionValidationErrors}
                testResult={connectionTestResult}
                isSaving={connectionSaving}
                isTesting={connectionTesting}
                onClose={() => {
                    setConnectionModalOpen(false);
                    resetConnection();
                }}
                onReset={resetConnection}
                onTest={runConnectionTest}
                onSave={saveConnection}
            />

            <DatasetModal
                open={datasetModalOpen}
                editingDatasetId={editingDatasetId}
                datasetForm={datasetForm}
                setDatasetForm={setDatasetForm}
                connections={connections}
                selectedConnectionForDataset={selectedConnectionForDataset}
                datasetValidation={datasetValidation}
                datasetValidationLoading={datasetValidationLoading}
                datasetResult={datasetResult}
                datasetPreviewLoading={datasetPreviewLoading}
                datasetPreviewError={datasetPreviewError}
                dynamicColumns={datasetPreviewColumns}
                onClose={() => {
                    setDatasetModalOpen(false);
                    resetDataset();
                }}
                onReset={resetDataset}
                onValidate={() => void runDatasetValidation(false)}
                onPreview={() => void runDatasetDraftPreview(false)}
                onSave={saveDataset}
            />

            <ReportModal
                open={reportModalOpen}
                editingReportId={editingReportId}
                reportForm={reportForm}
                setReportForm={setReportForm}
                datasets={datasets}
                categories={categories}
                dictionaries={dictionaries.filter((item) => item.is_active)}
                dictionaryItemsById={dictionaryItemsById}
                selectedDataset={selectedDatasetForForm}
                selectedDatasetColumns={selectedDatasetColumns}
                reportFormFilters={reportFormFilters}
                reportFormColumns={reportFormColumns}
                onClose={() => {
                    setReportModalOpen(false);
                    resetReport();
                }}
                onDatasetChange={handleReportDatasetChange}
                onAutoFilters={handleAutoFilters}
                onAddFilter={addReportFilter}
                onRemoveFilter={removeReportFilter}
                onUpdateFilter={updateReportFilter}
                onAddFilterOption={addReportFilterOption}
                                onUpdateFilterOption={updateReportFilterOption}
                                onRemoveFilterOption={removeReportFilterOption}
                                onUpdateColumn={updateReportColumn}
                                onReorderGroups={reorderReportGroups}
                                onMoveColumnToGroup={moveColumnToGroup}
                                onPlaceColumn={placeReportColumn}
                                onSetParentGroup={setParentGroup}
                                onToggleAllColumns={toggleAllReportColumns}
                onBulkUpdateColumns={bulkUpdateReportColumns}
                onReset={resetReport}
                onSave={saveReport}
                reportPreviewResult={reportPreviewResult}
                reportPreviewLoading={reportPreviewLoading}
                reportPreviewError={reportPreviewError}
                onPreview={() => void runReportDraftPreview(false)}
            />
        </div>
    );
}
