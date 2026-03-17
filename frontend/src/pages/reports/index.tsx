import { useEffect, useEffectEvent, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import {
    BarChart3,
    Database,
    Download,
    FileCode2,
    LineChart as LineChartIcon,
    PieChart as PieChartIcon,
    Play,
    Plus,
    RefreshCw,
    Save,
    SearchCheck,
    Trash2,
    X,
} from 'lucide-react';
import {
    Bar,
    BarChart,
    CartesianGrid,
    Cell,
    Legend,
    Line,
    LineChart,
    Pie,
    PieChart,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from 'recharts';
import DataTable from '../../components/data/DataTable';
import {
    createReportingConnection,
    createReportingDataset,
    createReportingReport,
    deleteReportingConnection,
    deleteReportingDataset,
    deleteReportingReport,
    getReportingConnectionTables,
    getReportingConnections,
    getReportingDatasets,
    getReportingReports,
    previewReportingDatasetDraft,
    runReportingReport,
    testReportingConnection,
    updateReportingConnection,
    updateReportingDataset,
    updateReportingReport,
} from '../../services/api';
import JsonEditor from '../../components/data/JsonEditor';
import SqlEditor from '../../components/data/SqlEditor';
import './Reports.css';

type TabKey = 'connections' | 'datasets' | 'reports';

type Connection = {
    id: number;
    name: string;
    db_type: string;
    host?: string | null;
    port?: number | null;
    database_name: string;
    schema_name?: string | null;
    username?: string | null;
    description?: string | null;
    connection_options?: string | null;
    is_active: boolean;
};

type Dataset = {
    id: number;
    connection_id: number;
    connection_name?: string | null;
    name: string;
    description?: string | null;
    sql_text: string;
    params_json?: string | null;
    row_limit: number;
    is_active: boolean;
};

type Report = {
    id: number;
    dataset_id: number;
    dataset_name?: string | null;
    name: string;
    description?: string | null;
    report_type: string;
    config_json?: string | null;
    is_active: boolean;
};

type QueryColumn = {
    name: string;
    type: string;
    sample?: unknown;
};

type QueryResult = {
    columns: QueryColumn[];
    rows: Record<string, unknown>[];
    numeric_summary: Record<string, number>;
    row_count: number;
    limit: number;
};

type FilterType = 'text' | 'number' | 'date' | 'select';

type ReportFilter = {
    key: string;
    label: string;
    type: FilterType;
    placeholder?: string;
    default_value?: string;
    options?: Array<{ label: string; value: string }>;
};

type ChartType = 'bar' | 'line' | 'pie';

type ReportChartConfig = {
    enabled: boolean;
    chart_type: ChartType;
    category_field: string;
    value_field: string;
    series_field?: string;
    aggregate?: 'sum' | 'count';
};

type ReportConfig = {
    visible_columns?: string[];
    default_limit?: number;
    filters?: ReportFilter[];
    chart?: ReportChartConfig;
};

const COLORS = ['#0f766e', '#0ea5e9', '#f59e0b', '#ef4444', '#8b5cf6', '#14b8a6', '#ec4899'];
const DATASET_PARAM_EXAMPLE = `{
  "tenant_id": 1001,
  "start_date": "2026-01-01",
  "status": "paid"
}`;
const DATASET_PARAM_SQL_EXAMPLE = `SELECT *
FROM orders
WHERE tenant_id = :tenant_id
  AND created_at >= :start_date
  AND status = :status
  AND org_name = '{CURRENT_ORG_NAME}'`;

const parseJson = <T,>(raw: string | null | undefined, fallback: T): T => {
    if (!raw) return fallback;
    try {
        return JSON.parse(raw) as T;
    } catch {
        return fallback;
    }
};

const formatCell = (value: unknown) => {
    if (value === null || value === undefined || value === '') return '-';
    return typeof value === 'object' ? JSON.stringify(value) : String(value);
};

const createEmptyFilter = (): ReportFilter => ({
    key: '',
    label: '',
    type: 'text',
    placeholder: '',
    default_value: '',
    options: [],
});

const normalizeFilter = (filter?: Partial<ReportFilter>): ReportFilter => ({
    key: filter?.key || '',
    label: filter?.label || '',
    type: filter?.type || 'text',
    placeholder: filter?.placeholder || '',
    default_value: filter?.default_value || '',
    options: (filter?.options || []).map((option) => ({
        label: option?.label || '',
        value: option?.value || '',
    })),
});

const sanitizeFilters = (filters: ReportFilter[]): ReportFilter[] =>
    filters
        .map((filter) => {
            const normalized = normalizeFilter(filter);
            const sanitized: ReportFilter = {
                key: normalized.key.trim(),
                label: (normalized.label || normalized.key).trim(),
                type: normalized.type,
            };

            if (normalized.placeholder?.trim()) {
                sanitized.placeholder = normalized.placeholder.trim();
            }
            if (normalized.default_value) {
                sanitized.default_value = normalized.default_value;
            }
            if (normalized.type === 'select') {
                const options = (normalized.options || [])
                    .map((option) => ({
                        label: option.label.trim() || option.value.trim(),
                        value: option.value.trim(),
                    }))
                    .filter((option) => option.label && option.value);

                sanitized.options = options;
            }

            return sanitized;
        })
        .filter((filter) => filter.key);

const buildFilterDefaults = (filters: ReportFilter[]) => {
    const result: Record<string, string> = {};
    filters.forEach((filter) => {
        result[filter.key] = filter.default_value || '';
    });
    return result;
};

const inferFiltersFromDataset = (dataset: Dataset | undefined): ReportFilter[] => {
    if (!dataset) return [];
    const params = parseJson<Record<string, unknown>>(dataset.params_json, {});
    return Object.entries(params).map(([key, value]) => ({
        key,
        label: key,
        type: typeof value === 'number' ? 'number' : String(key).toLowerCase().includes('date') ? 'date' : 'text',
        default_value: value === null || value === undefined ? '' : String(value),
    }));
};

const buildCsvContent = (columns: string[], rows: Record<string, unknown>[]) => {
    const escapeCsvValue = (value: unknown) => {
        const text = formatCell(value);
        return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
    };

    return [
        columns.map((column) => escapeCsvValue(column)).join(','),
        ...rows.map((row) => columns.map((column) => escapeCsvValue(row[column])).join(',')),
    ].join('\r\n');
};

const buildChartData = (rows: Record<string, unknown>[], chart?: ReportChartConfig) => {
    if (!chart?.enabled || !chart.category_field || !chart.value_field) return [];
    const grouped = new Map<string, { key: string; value: number }>();

    rows.forEach((row) => {
        const category = String(row[chart.category_field] ?? '未分类');
        const rawValue = row[chart.value_field];
        const numericValue =
            chart.aggregate === 'count'
                ? 1
                : typeof rawValue === 'number'
                  ? rawValue
                  : Number(rawValue ?? 0);
        const prev = grouped.get(category);
        grouped.set(category, {
            key: category,
            value: (prev?.value || 0) + (Number.isFinite(numericValue) ? numericValue : 0),
        });
    });

    return Array.from(grouped.values()).map((item) => ({
        name: item.key,
        value: Number(item.value.toFixed(2)),
    }));
};

function ReportChart({
    rows,
    chart,
}: {
    rows: Record<string, unknown>[];
    chart?: ReportChartConfig;
}) {
    const data = useMemo(() => buildChartData(rows, chart), [rows, chart]);

    if (!chart?.enabled) {
        return <div className="empty-box">为报表配置图表后，这里会显示可视化结果。</div>;
    }

    if (!data.length) {
        return <div className="empty-box">当前筛选结果没有可用于绘图的数据。</div>;
    }

    return (
        <div className="chart-stage">
            <ResponsiveContainer>
                {chart.chart_type === 'pie' ? (
                    <PieChart>
                        <Pie data={data} dataKey="value" nameKey="name" outerRadius={96}>
                            {data.map((_, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip />
                        <Legend />
                    </PieChart>
                ) : chart.chart_type === 'line' ? (
                    <LineChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="name" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Line type="monotone" dataKey="value" stroke="#0ea5e9" strokeWidth={3} />
                    </LineChart>
                ) : (
                    <BarChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="name" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Bar dataKey="value" fill="#0f766e" radius={[8, 8, 0, 0]} />
                    </BarChart>
                )}
            </ResponsiveContainer>
        </div>
    );
}

function FormModal({
    open,
    title,
    subtitle,
    width = '1080px',
    onClose,
    children,
}: {
    open: boolean;
    title: string;
    subtitle?: string;
    width?: string;
    onClose: () => void;
    children: ReactNode;
}) {
    if (!open) return null;

    return (
        <div className="reporting-modal-backdrop" onClick={onClose}>
            <div className="reporting-modal" style={{ maxWidth: width }} onClick={(event) => event.stopPropagation()}>
                <div className="reporting-modal-head">
                    <div>
                        <h3>{title}</h3>
                        {subtitle ? <div className="resource-meta">{subtitle}</div> : null}
                    </div>
                    <button className="ghost-btn" type="button" onClick={onClose}>
                        <X size={16} />
                    </button>
                </div>
                <div className="reporting-modal-body">{children}</div>
            </div>
        </div>
    );
}

export default function Reports() {
    const user = parseJson<{ role?: string }>(localStorage.getItem('user'), {});
    const isAdmin = user?.role === 'admin';

    const [tab, setTab] = useState<TabKey>(isAdmin ? 'connections' : 'reports');
    const [loading, setLoading] = useState(false);

    const [connections, setConnections] = useState<Connection[]>([]);
    const [datasets, setDatasets] = useState<Dataset[]>([]);
    const [reports, setReports] = useState<Report[]>([]);

    const [tables, setTables] = useState<Array<{ table_name: string; columns: Array<{ name: string; type: string }> }>>([]);
    const [datasetResult, setDatasetResult] = useState<QueryResult | null>(null);
    const [datasetPreviewLoading, setDatasetPreviewLoading] = useState(false);
    const [datasetPreviewError, setDatasetPreviewError] = useState<string | null>(null);
    const [reportResult, setReportResult] = useState<QueryResult | null>(null);
    const [activeReport, setActiveReport] = useState<Report | null>(null);
    const [runtimeFilters, setRuntimeFilters] = useState<Record<string, string>>({});
    const [runtimeLimit, setRuntimeLimit] = useState<string>('100');

    const [editingConnectionId, setEditingConnectionId] = useState<number | null>(null);
    const [editingDatasetId, setEditingDatasetId] = useState<number | null>(null);
    const [editingReportId, setEditingReportId] = useState<number | null>(null);
    const [connectionModalOpen, setConnectionModalOpen] = useState(false);
    const [datasetModalOpen, setDatasetModalOpen] = useState(false);
    const [reportModalOpen, setReportModalOpen] = useState(false);

    const [connectionForm, setConnectionForm] = useState({
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

    const [datasetForm, setDatasetForm] = useState({
        connection_id: '',
        name: '',
        description: '',
        sql_text: 'SELECT *\nFROM your_table\nLIMIT 100',
        params_json: '{\n}',
        row_limit: '200',
        is_active: true,
    });

    const [reportForm, setReportForm] = useState({
        dataset_id: '',
        name: '',
        description: '',
        report_type: 'table',
        visible_columns: '',
        default_limit: '100',
        filters_json: '[]',
        chart_enabled: false,
        chart_type: 'bar' as ChartType,
        category_field: '',
        value_field: '',
        aggregate: 'sum' as 'sum' | 'count',
        is_active: true,
    });

    const loadData = async () => {
        setLoading(true);
        try {
            if (isAdmin) {
                const [connectionData, datasetData, reportData] = await Promise.all([
                    getReportingConnections(),
                    getReportingDatasets(),
                    getReportingReports(),
                ]);
                setConnections(connectionData);
                setDatasets(datasetData);
                setReports(reportData);
            } else {
                setReports(await getReportingReports());
            }
        } catch (error: any) {
            alert(error?.response?.data?.detail || error?.message || '加载失败');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void loadData();
    }, []);

    const dynamicColumns = useMemo(() => {
        const source = reportResult?.columns || datasetResult?.columns || [];
        return source.map((column) => ({
            key: column.name,
            title: `${column.name} (${column.type})`,
            render: (value: unknown) => <span title={formatCell(value)}>{formatCell(value)}</span>,
        }));
    }, [datasetResult, reportResult]);

    const selectedDatasetForForm = useMemo(
        () => datasets.find((item) => String(item.id) === reportForm.dataset_id),
        [datasets, reportForm.dataset_id]
    );
    const selectedConnectionForDataset = useMemo(
        () => connections.find((item) => String(item.id) === datasetForm.connection_id),
        [connections, datasetForm.connection_id]
    );

    const activeReportConfig = useMemo(
        () => parseJson<ReportConfig>(activeReport?.config_json, {}),
        [activeReport]
    );

    const reportFormFilters = useMemo(
        () => parseJson<ReportFilter[]>(reportForm.filters_json, []).map((filter) => normalizeFilter(filter)),
        [reportForm.filters_json]
    );

    const syncReportFilters = (nextFilters: ReportFilter[]) => {
        setReportForm((prev) => ({
            ...prev,
            filters_json: JSON.stringify(nextFilters, null, 2),
        }));
    };

    const resetConnection = () => {
        setEditingConnectionId(null);
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

    const resetDataset = () => {
        setEditingDatasetId(null);
        setDatasetResult(null);
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
        setEditingReportId(null);
        setReportForm({
            dataset_id: datasets[0] ? String(datasets[0].id) : '',
            name: '',
            description: '',
            report_type: 'table',
            visible_columns: '',
            default_limit: '100',
            filters_json: '[]',
            chart_enabled: false,
            chart_type: 'bar',
            category_field: '',
            value_field: '',
            aggregate: 'sum',
            is_active: true,
        });
    };

    const openReportCreateModal = () => {
        resetReport();
        setReportModalOpen(true);
    };

    const saveConnection = async () => {
        try {
            const payload = {
                ...connectionForm,
                port: connectionForm.port ? Number(connectionForm.port) : null,
                connection_options: connectionForm.connection_options || null,
            };
            if (editingConnectionId) {
                await updateReportingConnection(editingConnectionId, payload);
            } else {
                await createReportingConnection(payload);
            }
            setConnectionModalOpen(false);
            resetConnection();
            await loadData();
        } catch (error: any) {
            alert(error?.response?.data?.detail || error?.message || '保存连接失败');
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
            } else {
                await createReportingDataset(payload);
            }
            setDatasetModalOpen(false);
            resetDataset();
            await loadData();
        } catch (error: any) {
            alert(error?.response?.data?.detail || error?.message || '保存数据集失败');
        }
    };

    const saveReport = async () => {
        try {
            const filters = sanitizeFilters(reportFormFilters);
            const payload = {
                dataset_id: Number(reportForm.dataset_id),
                name: reportForm.name,
                description: reportForm.description || null,
                report_type: reportForm.report_type,
                config_json: JSON.stringify({
                    visible_columns: reportForm.visible_columns
                        .split(',')
                        .map((item) => item.trim())
                        .filter(Boolean),
                    default_limit: Number(reportForm.default_limit || 100),
                    filters,
                    chart: {
                        enabled: reportForm.chart_enabled,
                        chart_type: reportForm.chart_type,
                        category_field: reportForm.category_field,
                        value_field: reportForm.value_field,
                        aggregate: reportForm.aggregate,
                    },
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
        } catch (error: any) {
            alert(error?.response?.data?.detail || error?.message || '保存报表失败');
        }
    };

    const runDatasetDraftPreview = useEffectEvent(async (silent = false) => {
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
        } catch (error: any) {
            const message = error?.response?.data?.detail || error?.message || '预览失败';
            setDatasetResult(null);
            setDatasetPreviewError(message);
            if (!silent) {
                alert(message);
            }
        } finally {
            setDatasetPreviewLoading(false);
        }
    });

    useEffect(() => {
        if (!datasetModalOpen) {
            return;
        }

        const timer = window.setTimeout(() => {
            void runDatasetDraftPreview(true);
        }, 450);

        return () => window.clearTimeout(timer);
    }, [
        datasetModalOpen,
        datasetForm.connection_id,
        datasetForm.sql_text,
        datasetForm.params_json,
        datasetForm.row_limit,
        runDatasetDraftPreview,
    ]);

    const openReport = (report: Report) => {
        const config = parseJson<ReportConfig>(report.config_json, {});
        const defaults = buildFilterDefaults(config.filters || []);
        setActiveReport(report);
        setRuntimeFilters(defaults);
        setRuntimeLimit(String(config.default_limit || 100));
        setTab('reports');
        void runReport(report.id, defaults, String(config.default_limit || 100));
    };

    const editReport = (report: Report) => {
        const config = parseJson<ReportConfig>(report.config_json, {});
        setEditingReportId(report.id);
        setTab('reports');
        setReportForm({
            dataset_id: String(report.dataset_id),
            name: report.name,
            description: report.description || '',
            report_type: report.report_type || 'table',
            visible_columns: (config.visible_columns || []).join(', '),
            default_limit: String(config.default_limit || 100),
            filters_json: JSON.stringify(config.filters || inferFiltersFromDataset(datasets.find((item) => item.id === report.dataset_id)), null, 2),
            chart_enabled: !!config.chart?.enabled,
            chart_type: config.chart?.chart_type || 'bar',
            category_field: config.chart?.category_field || '',
            value_field: config.chart?.value_field || '',
            aggregate: config.chart?.aggregate || 'sum',
            is_active: report.is_active,
        });
        setReportModalOpen(true);
    };

    const runReport = async (reportId: number, filterValues?: Record<string, string>, limitValue?: string) => {
        try {
            const params = Object.fromEntries(
                Object.entries(filterValues || runtimeFilters).filter(([, value]) => value !== '' && value !== null && value !== undefined)
            );
            const result = await runReportingReport(reportId, {
                params,
                limit: Number(limitValue || runtimeLimit || 100),
            });
            const reportMeta = reports.find((item) => item.id === reportId) || null;
            if (reportMeta) setActiveReport(reportMeta);
            setReportResult({
                columns: result.columns || [],
                rows: result.rows || [],
                numeric_summary: result.numeric_summary || {},
                row_count: result.row_count || 0,
                limit: result.limit || 0,
            });
        } catch (error: any) {
            alert(error?.response?.data?.detail || error?.message || '运行失败');
        }
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

    const exportReportCsv = () => {
        if (!reportResult?.rows?.length) {
            alert('当前没有可导出的报表结果');
            return;
        }

        const columns =
            reportResult.columns?.map((column) => column.name) ||
            Array.from(new Set(reportResult.rows.flatMap((row) => Object.keys(row))));
        const csvContent = buildCsvContent(columns, reportResult.rows);
        const filenameBase = (activeReport?.name || 'report')
            .replace(/[\\/:*?"<>|]+/g, '_')
            .replace(/\s+/g, '_');
        const blob = new Blob([`\ufeff${csvContent}`], { type: 'text/csv;charset=utf-8;' });
        const downloadUrl = URL.createObjectURL(blob);
        const link = document.createElement('a');

        link.href = downloadUrl;
        link.download = `${filenameBase || 'report'}_${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(link);
        link.click();
        link.remove();

        window.setTimeout(() => {
            URL.revokeObjectURL(downloadUrl);
        }, 1000);
    };

    return (
        <div className="page-container fade-in reporting-page">
            <div className="reporting-tabs">
                {isAdmin && (
                    <button className={`reporting-tab ${tab === 'connections' ? 'active' : ''}`} onClick={() => setTab('connections')}>
                        <Database size={16} />
                        数据源连接
                    </button>
                )}
                {isAdmin && (
                    <button className={`reporting-tab ${tab === 'datasets' ? 'active' : ''}`} onClick={() => setTab('datasets')}>
                        <FileCode2 size={16} />
                        SQL 数据集
                    </button>
                )}
                <button className={`reporting-tab ${tab === 'reports' ? 'active' : ''}`} onClick={() => setTab('reports')}>
                    <BarChart3 size={16} />
                    报表中心
                </button>
                <button className="btn-outline reporting-tab-action" onClick={() => void loadData()} disabled={loading}>
                    <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
                    {loading ? '刷新中...' : '刷新'}
                </button>
            </div>

            {tab === 'connections' && isAdmin && (
                <div className="reporting-grid">
                    <div className="card glass reporting-panel">
                        <div className="section-head">
                            <h3>连接列表</h3>
                            <button className="btn-primary" onClick={openConnectionCreateModal}>
                                <Plus size={14} />
                                新建连接
                            </button>
                        </div>
                        <div className="resource-list">
                            {connections.map((item) => (
                                <div key={item.id} className="resource-card">
                                    <div>
                                        <strong>{item.name}</strong>
                                        <div className="resource-meta">
                                            {item.db_type} / {item.host || 'local'} / {item.database_name}
                                        </div>
                                    </div>
                                    <div className="resource-actions">
                                        <button onClick={() => openConnectionEditModal(item)}>编辑</button>
                                        <button
                                            onClick={() =>
                                                void getReportingConnectionTables(item.id, item.schema_name || undefined)
                                                    .then((res) => setTables(res.tables || []))
                                                    .catch((error: any) => alert(error?.response?.data?.detail || error?.message || '读取表结构失败'))
                                            }
                                        >
                                            表结构
                                        </button>
                                        <button
                                            className="danger"
                                            onClick={() => {
                                                if (!window.confirm('确定删除该连接及其下数据集和报表吗？')) return;
                                                void deleteReportingConnection(item.id)
                                                    .then(() => loadData())
                                                    .catch((error: any) => alert(error?.response?.data?.detail || error?.message || '删除失败'));
                                            }}
                                        >
                                            <Trash2 size={14} />
                                        </button>
                                    </div>
                                </div>
                            ))}
                            {!connections.length && <div className="empty-box">还没有数据库连接。</div>}
                        </div>
                    </div>
                    <div className="card glass reporting-panel reporting-panel-wide">
                        <div className="section-head"><h3>表结构浏览</h3></div>
                        <div className="schema-list">
                            {tables.map((table) => (
                                <div key={table.table_name} className="schema-card">
                                    <div className="schema-title">{table.table_name}</div>
                                    <div className="schema-columns">
                                        {table.columns.map((column) => (
                                            <span key={`${table.table_name}-${column.name}`} className="schema-chip">
                                                {column.name}: {column.type}
                                            </span>
                                        ))}
                                    </div>
                                </div>
                            ))}
                            {!tables.length && <div className="empty-box">点击“表结构”后，这里会展示数据库中的可用表和字段。</div>}
                        </div>
                    </div>
                </div>
            )}

            {tab === 'datasets' && isAdmin && (
                <div className="reporting-grid reporting-grid-two">
                    <div className="card glass reporting-panel">
                        <div className="section-head">
                            <h3>数据集列表</h3>
                            <button className="btn-primary" onClick={openDatasetCreateModal}><Plus size={14} />新建数据集</button>
                        </div>
                        <div className="resource-list">
                            {datasets.map((item) => (
                                <div key={item.id} className="resource-card">
                                    <div>
                                        <strong>{item.name}</strong>
                                        <div className="resource-meta">{item.connection_name || '-'} / 预览上限 {item.row_limit}</div>
                                    </div>
                                    <div className="resource-actions">
                                        <button onClick={() => openDatasetEditModal(item)}>编辑</button>
                                        <button
                                            className="danger"
                                            onClick={() => {
                                                if (!window.confirm('确定删除该数据集及其下报表吗？')) return;
                                                void deleteReportingDataset(item.id)
                                                    .then(() => loadData())
                                                    .catch((error: any) => alert(error?.response?.data?.detail || error?.message || '删除失败'));
                                            }}
                                        >
                                            <Trash2 size={14} />
                                        </button>
                                    </div>
                                </div>
                            ))}
                            {!datasets.length && <div className="empty-box">还没有 SQL 数据集。</div>}
                        </div>
                    </div>
                    <div className="card glass reporting-panel">
                        <div className="section-head">
                            <h3>建模说明</h3>
                        </div>
                        <div className="journey-list">
                            <div className="journey-step static">
                                <div className="journey-step-index">1</div>
                                <div>
                                    <strong>在弹窗中编辑数据集</strong>
                                    <div className="resource-meta">新建和编辑都会在弹窗中完成，不再占用页面主区域。</div>
                                </div>
                            </div>
                            <div className="journey-step static">
                                <div className="journey-step-index">2</div>
                                <div>
                                    <strong>在弹窗内直接预览</strong>
                                    <div className="resource-meta">填写连接、SQL 和参数后，在弹窗里直接预览查询结果。</div>
                                </div>
                            </div>
                            <div className="journey-step static">
                                <div className="journey-step-index">3</div>
                                <div>
                                    <strong>保存后供报表复用</strong>
                                    <div className="resource-meta">数据集保存后即可被报表定义选择，成为可复用的数据源模型。</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {tab === 'reports' && (
                <div className="reporting-grid report-grid">
                    <div className="card glass reporting-panel">
                        <div className="section-head">
                            <h3>报表目录</h3>
                            {isAdmin && <button className="btn-primary" onClick={openReportCreateModal}><Plus size={14} />新建报表</button>}
                        </div>
                        <div className="resource-list">
                            {reports.map((item) => (
                                <div key={item.id} className={`resource-card ${activeReport?.id === item.id ? 'selected' : ''}`}>
                                    <div>
                                        <strong>{item.name}</strong>
                                        <div className="resource-meta">{item.dataset_name || '-'} / {item.report_type}</div>
                                    </div>
                                    <div className="resource-actions">
                                        <button onClick={() => openReport(item)}>运行</button>
                                        {isAdmin && <button onClick={() => editReport(item)}>设计</button>}
                                        {isAdmin && (
                                            <button
                                                className="danger"
                                                onClick={() => {
                                                    if (!window.confirm('确定删除该报表吗？')) return;
                                                    void deleteReportingReport(item.id)
                                                        .then(() => loadData())
                                                        .catch((error: any) => alert(error?.response?.data?.detail || error?.message || '删除失败'));
                                                }}
                                            >
                                                <Trash2 size={14} />
                                            </button>
                                        )}
                                    </div>
                                </div>
                            ))}
                            {!reports.length && <div className="empty-box">还没有报表定义，可以从数据集创建一个业务报表。</div>}
                        </div>
                    </div>

                    <div className="card glass reporting-panel report-runtime-panel">
                        <div className="section-head">
                            <div>
                                <h3>报表运行</h3>
                                <div className="resource-meta">{activeReport?.name || '请选择一个报表'}</div>
                            </div>
                        </div>
                        {activeReport ? (
                            <>
                                <div className="runtime-filters">
                                    {(activeReportConfig.filters || []).map((filter) => (
                                        <label key={filter.key}>
                                            <span>{filter.label}</span>
                                            {filter.type === 'select' ? (
                                                <select value={runtimeFilters[filter.key] || ''} onChange={(e) => setRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}>
                                                    <option value="">全部</option>
                                                    {(filter.options || []).map((option) => (
                                                        <option key={`${filter.key}-${option.value}`} value={option.value}>{option.label}</option>
                                                    ))}
                                                </select>
                                            ) : (
                                                <input
                                                    type={filter.type === 'number' ? 'number' : filter.type === 'date' ? 'date' : 'text'}
                                                    value={runtimeFilters[filter.key] || ''}
                                                    placeholder={filter.placeholder || ''}
                                                    onChange={(e) => setRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}
                                                />
                                            )}
                                        </label>
                                    ))}
                                    <label>
                                        <span>返回行数</span>
                                        <input type="number" value={runtimeLimit} onChange={(e) => setRuntimeLimit(e.target.value)} />
                                    </label>
                                </div>
                                <div className="runtime-actions">
                                    <button className="btn-outline" onClick={() => setRuntimeFilters(buildFilterDefaults(activeReportConfig.filters || []))}>重置筛选</button>
                                    <button className="btn-primary" onClick={() => void runReport(activeReport.id)}><Play size={14} />运行报表</button>
                                    <button className="btn-outline" onClick={exportReportCsv} disabled={!reportResult?.rows?.length}>
                                        <Download size={14} />
                                        导出 CSV
                                    </button>
                                </div>
                                <div className="chart-header">
                                    <div className="chart-badge"><LineChartIcon size={14} />图表</div>
                                    <div className="chart-badge"><PieChartIcon size={14} />明细</div>
                                </div>
                                <ReportChart rows={reportResult?.rows || []} chart={activeReportConfig.chart} />
                                {reportResult ? (
                                    <DataTable columns={dynamicColumns} data={reportResult.rows} />
                                ) : (
                                    <div className="empty-box">点击“运行报表”后，这里会展示图表和明细结果。</div>
                                )}
                            </>
                        ) : (
                            <div className="empty-box">从左侧报表目录中选择一个报表，即可在这里按筛选条件运行与导出。</div>
                        )}
                    </div>
                </div>
            )}

            <FormModal
                open={connectionModalOpen}
                title={editingConnectionId ? '编辑连接' : '新建连接'}
                subtitle="连接配置、连通性测试都在这里完成。"
                width="860px"
                onClose={() => {
                    setConnectionModalOpen(false);
                    resetConnection();
                }}
            >
                <div className="form-grid two">
                    <label><span>名称</span><input value={connectionForm.name} onChange={(e) => setConnectionForm((prev) => ({ ...prev, name: e.target.value }))} /></label>
                    <label>
                        <span>类型</span>
                        <select value={connectionForm.db_type} onChange={(e) => setConnectionForm((prev) => ({ ...prev, db_type: e.target.value }))}>
                            <option value="postgresql">PostgreSQL</option>
                            <option value="mysql">MySQL</option>
                            <option value="sqlite">SQLite</option>
                            <option value="mssql">SQL Server</option>
                        </select>
                    </label>
                    <label><span>Host</span><input value={connectionForm.host} onChange={(e) => setConnectionForm((prev) => ({ ...prev, host: e.target.value }))} /></label>
                    <label><span>Port</span><input value={connectionForm.port} onChange={(e) => setConnectionForm((prev) => ({ ...prev, port: e.target.value }))} /></label>
                    <label><span>数据库名 / 文件路径</span><input value={connectionForm.database_name} onChange={(e) => setConnectionForm((prev) => ({ ...prev, database_name: e.target.value }))} /></label>
                    <label><span>Schema</span><input value={connectionForm.schema_name} onChange={(e) => setConnectionForm((prev) => ({ ...prev, schema_name: e.target.value }))} /></label>
                    <label><span>用户名</span><input value={connectionForm.username} onChange={(e) => setConnectionForm((prev) => ({ ...prev, username: e.target.value }))} /></label>
                    <label><span>密码</span><input type="password" value={connectionForm.password} onChange={(e) => setConnectionForm((prev) => ({ ...prev, password: e.target.value }))} placeholder={editingConnectionId ? '留空表示不修改' : ''} /></label>
                </div>
                <label className="form-block"><span>描述</span><input value={connectionForm.description} onChange={(e) => setConnectionForm((prev) => ({ ...prev, description: e.target.value }))} /></label>
                <label className="form-block"><span>连接参数 JSON</span><textarea value={connectionForm.connection_options} onChange={(e) => setConnectionForm((prev) => ({ ...prev, connection_options: e.target.value }))} /></label>
                <div className="editor-actions">
                    <button
                        className="btn-outline"
                        onClick={() =>
                            void testReportingConnection({
                                ...connectionForm,
                                port: Number(connectionForm.port || 0) || null,
                            })
                                .then((res: any) => alert(res.message || '测试完成'))
                                .catch((error: any) => alert(error?.response?.data?.detail || error?.message || '测试失败'))
                        }
                    >
                        <SearchCheck size={14} />
                        测试连接
                    </button>
                    <button className="btn-outline" onClick={resetConnection}>重置</button>
                    <button className="btn-primary" onClick={() => void saveConnection()}><Save size={14} />保存连接</button>
                </div>
            </FormModal>

            <FormModal
                open={datasetModalOpen}
                title={editingDatasetId ? '编辑数据集' : '新建数据集'}
                subtitle="在弹窗内直接编写 SQL、维护参数并预览结果。"
                width="1180px"
                onClose={() => {
                    setDatasetModalOpen(false);
                    resetDataset();
                }}
            >
                <div className="modal-split">
                    <div className="modal-form-pane">
                        <div className="form-grid two">
                            <label>
                                <span>连接</span>
                                <select value={datasetForm.connection_id} onChange={(e) => setDatasetForm((prev) => ({ ...prev, connection_id: e.target.value }))}>
                                    <option value="">请选择</option>
                                    {connections.map((item) => (
                                        <option key={item.id} value={item.id}>{item.name}</option>
                                    ))}
                                </select>
                            </label>
                            <label><span>预览行数</span><input value={datasetForm.row_limit} onChange={(e) => setDatasetForm((prev) => ({ ...prev, row_limit: e.target.value }))} /></label>
                        </div>
                        <label className="form-block"><span>名称</span><input value={datasetForm.name} onChange={(e) => setDatasetForm((prev) => ({ ...prev, name: e.target.value }))} /></label>
                        <label className="form-block"><span>描述</span><input value={datasetForm.description} onChange={(e) => setDatasetForm((prev) => ({ ...prev, description: e.target.value }))} /></label>
                        <div className="form-block reporting-editor-field">
                            <span>SQL</span>
                            <SqlEditor
                                value={datasetForm.sql_text}
                                onChange={(value) => setDatasetForm((prev) => ({ ...prev, sql_text: value }))}
                                dialect={selectedConnectionForDataset?.db_type}
                                onPreview={() => void runDatasetDraftPreview(false)}
                                previewLoading={datasetPreviewLoading}
                                height="320px"
                            />
                            <div className="editor-help-text">
                                在 SQL 里用 `:参数名` 定义可绑定参数，例如 `:tenant_id`、`:start_date`。系统全局变量继续使用 {'{变量名}'}，两者可以混用。
                            </div>
                        </div>
                        <div className="form-block reporting-editor-field">
                            <div className="field-head">
                                <span>参数 JSON</span>
                                <button
                                    className="inline-link-btn"
                                    type="button"
                                    onClick={() => setDatasetForm((prev) => ({ ...prev, params_json: DATASET_PARAM_EXAMPLE }))}
                                >
                                    填充示例
                                </button>
                            </div>
                            <JsonEditor
                                value={datasetForm.params_json}
                                onChange={(value) => setDatasetForm((prev) => ({ ...prev, params_json: value }))}
                                height="220px"
                                placeholder={DATASET_PARAM_EXAMPLE}
                            />
                            <div className="param-guide-card">
                                <div className="param-guide-title">参数 JSON 怎么用</div>
                                <div className="param-guide-list">
                                    <div>1. SQL 中先写命名参数，例如 `WHERE tenant_id = :tenant_id`。</div>
                                    <div>2. 参数 JSON 里的键名必须和 SQL 里的参数名一致。</div>
                                    <div>3. 预览时会直接使用这份 JSON 作为查询参数默认值。</div>
                                    <div>4. {'{CURRENT_ORG_NAME}'} 这类系统全局变量会先解析，再和参数 JSON 一起执行。</div>
                                </div>
                                <div className="param-guide-examples">
                                    <div className="param-guide-example">
                                        <div className="param-guide-label">示例 SQL</div>
                                        <pre>{DATASET_PARAM_SQL_EXAMPLE}</pre>
                                    </div>
                                    <div className="param-guide-example">
                                        <div className="param-guide-label">示例参数</div>
                                        <pre>{DATASET_PARAM_EXAMPLE}</pre>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="editor-actions">
                            <button className="btn-outline" onClick={resetDataset}>重置</button>
                            <button className="btn-primary" onClick={() => void saveDataset()}><Save size={14} />保存数据集</button>
                        </div>
                    </div>
                    <div className="modal-preview-pane">
                        <div className="section-head">
                            <h3>预览结果</h3>
                            <div className="resource-meta">
                                {datasetPreviewLoading
                                    ? '正在自动预览当前 SQL'
                                    : datasetResult
                                        ? `返回 ${datasetResult.row_count} 行 / 上限 ${datasetResult.limit}`
                                        : '填写 SQL 后会在这里自动预览'}
                            </div>
                        </div>
                        {datasetPreviewError ? (
                            <div className="preview-status error">{datasetPreviewError}</div>
                        ) : datasetPreviewLoading ? (
                            <div className="preview-status loading">正在执行只读预览查询，请稍候...</div>
                        ) : datasetResult ? (
                            <DataTable columns={dynamicColumns} data={datasetResult.rows} />
                        ) : (
                            <div className="empty-box">选择连接并编写 SQL 后，这里会自动显示预览结果。</div>
                        )}
                    </div>
                </div>
            </FormModal>

            <FormModal
                open={reportModalOpen}
                title={editingReportId ? '编辑报表' : '新建报表'}
                subtitle="在弹窗里完成报表定义，页面只保留目录和运行区。"
                width="1180px"
                onClose={() => {
                    setReportModalOpen(false);
                    resetReport();
                }}
            >
                <div className="section-head workspace-head">
                    <div className="resource-meta">把数据集、筛选器和图表配置沉淀成可运行报表模板。</div>
                    <button className="btn-outline" onClick={handleAutoFilters}>
                        <SearchCheck size={14} />
                        从数据集生成筛选
                    </button>
                </div>
                <div className="form-grid two">
                    <label>
                        <span>数据集</span>
                        <select value={reportForm.dataset_id} onChange={(e) => setReportForm((prev) => ({ ...prev, dataset_id: e.target.value }))}>
                            <option value="">请选择</option>
                            {datasets.map((item) => (
                                <option key={item.id} value={item.id}>{item.name}</option>
                            ))}
                        </select>
                    </label>
                    <label>
                        <span>类型</span>
                        <select value={reportForm.report_type} onChange={(e) => setReportForm((prev) => ({ ...prev, report_type: e.target.value }))}>
                            <option value="table">表格报表</option>
                            <option value="summary">汇总报表</option>
                        </select>
                    </label>
                </div>
                <label className="form-block"><span>名称</span><input value={reportForm.name} onChange={(e) => setReportForm((prev) => ({ ...prev, name: e.target.value }))} /></label>
                <label className="form-block"><span>描述</span><input value={reportForm.description} onChange={(e) => setReportForm((prev) => ({ ...prev, description: e.target.value }))} /></label>
                <label className="form-block"><span>可见列</span><input value={reportForm.visible_columns} onChange={(e) => setReportForm((prev) => ({ ...prev, visible_columns: e.target.value }))} placeholder="order_date, customer_name, amount" /></label>
                <label className="form-block"><span>默认返回行数</span><input value={reportForm.default_limit} onChange={(e) => setReportForm((prev) => ({ ...prev, default_limit: e.target.value }))} /></label>
                <div className="filter-builder">
                    <div className="filter-builder-head">
                        <div>
                            <div className="filter-builder-title">筛选器定义</div>
                            <div className="resource-meta">配置运行时过滤条件，替代手工编辑 JSON。</div>
                        </div>
                        <button className="btn-outline" type="button" onClick={addReportFilter}>
                            <Plus size={14} />
                            添加筛选器
                        </button>
                    </div>
                    {reportFormFilters.length ? (
                        <div className="filter-list">
                            {reportFormFilters.map((filter, filterIndex) => (
                                <div key={`filter-${filterIndex}`} className="filter-card">
                                    <div className="filter-card-head">
                                        <strong>筛选器 {filterIndex + 1}</strong>
                                        <button className="danger ghost-btn" type="button" onClick={() => removeReportFilter(filterIndex)}>
                                            <Trash2 size={14} />
                                        </button>
                                    </div>
                                    <div className="form-grid two">
                                        <label>
                                            <span>参数键</span>
                                            <input value={filter.key} onChange={(e) => updateReportFilter(filterIndex, { key: e.target.value })} placeholder="例如 project_id" />
                                        </label>
                                        <label>
                                            <span>显示名</span>
                                            <input value={filter.label} onChange={(e) => updateReportFilter(filterIndex, { label: e.target.value })} placeholder="例如 项目" />
                                        </label>
                                        <label>
                                            <span>类型</span>
                                            <select value={filter.type} onChange={(e) => updateReportFilter(filterIndex, { type: e.target.value as FilterType })}>
                                                <option value="text">文本</option>
                                                <option value="number">数字</option>
                                                <option value="date">日期</option>
                                                <option value="select">下拉</option>
                                            </select>
                                        </label>
                                        <label>
                                            <span>默认值</span>
                                            <input value={filter.default_value || ''} onChange={(e) => updateReportFilter(filterIndex, { default_value: e.target.value })} placeholder="可选" />
                                        </label>
                                    </div>
                                    <label className="form-block">
                                        <span>占位提示</span>
                                        <input value={filter.placeholder || ''} onChange={(e) => updateReportFilter(filterIndex, { placeholder: e.target.value })} placeholder="例如 输入项目名称" />
                                    </label>
                                    {filter.type === 'select' && (
                                        <div className="filter-options">
                                            <div className="filter-options-head">
                                                <span>下拉选项</span>
                                                <button className="btn-outline" type="button" onClick={() => addReportFilterOption(filterIndex)}>
                                                    <Plus size={14} />
                                                    添加选项
                                                </button>
                                            </div>
                                            {(filter.options || []).length ? (
                                                <div className="filter-options-list">
                                                    {(filter.options || []).map((option, optionIndex) => (
                                                        <div key={`filter-${filterIndex}-option-${optionIndex}`} className="filter-option-row">
                                                            <input value={option.label} onChange={(e) => updateReportFilterOption(filterIndex, optionIndex, { label: e.target.value })} placeholder="显示文本" />
                                                            <input value={option.value} onChange={(e) => updateReportFilterOption(filterIndex, optionIndex, { value: e.target.value })} placeholder="实际值" />
                                                            <button className="danger ghost-btn" type="button" onClick={() => removeReportFilterOption(filterIndex, optionIndex)}>
                                                                <Trash2 size={14} />
                                                            </button>
                                                        </div>
                                                    ))}
                                                </div>
                                            ) : (
                                                <div className="helper-text">添加选项后，运行时会显示下拉筛选。</div>
                                            )}
                                        </div>
                                    )}
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="empty-box compact-empty">暂无筛选器，报表会直接按默认参数运行。</div>
                    )}
                </div>
                <div className="chart-config-grid">
                    <label className="switch-chip">
                        <input type="checkbox" checked={reportForm.chart_enabled} onChange={(e) => setReportForm((prev) => ({ ...prev, chart_enabled: e.target.checked }))} />
                        启用图表
                    </label>
                    <label>
                        <span>图表类型</span>
                        <select value={reportForm.chart_type} onChange={(e) => setReportForm((prev) => ({ ...prev, chart_type: e.target.value as ChartType }))}>
                            <option value="bar">柱状图</option>
                            <option value="line">折线图</option>
                            <option value="pie">饼图</option>
                        </select>
                    </label>
                    <label><span>分类字段</span><input value={reportForm.category_field} onChange={(e) => setReportForm((prev) => ({ ...prev, category_field: e.target.value }))} placeholder="例如 order_date" /></label>
                    <label><span>数值字段</span><input value={reportForm.value_field} onChange={(e) => setReportForm((prev) => ({ ...prev, value_field: e.target.value }))} placeholder="例如 amount" /></label>
                    <label>
                        <span>聚合方式</span>
                        <select value={reportForm.aggregate} onChange={(e) => setReportForm((prev) => ({ ...prev, aggregate: e.target.value as 'sum' | 'count' }))}>
                            <option value="sum">求和</option>
                            <option value="count">计数</option>
                        </select>
                    </label>
                </div>
                <div className="editor-actions">
                    <button className="btn-outline" onClick={resetReport}>重置</button>
                    <button className="btn-primary" onClick={() => void saveReport()}><Save size={14} />保存报表</button>
                </div>
            </FormModal>
        </div>
    );
}
