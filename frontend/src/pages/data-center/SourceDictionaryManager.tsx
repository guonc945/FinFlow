import { useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import type { ReactNode } from 'react';
import {
    AlertCircle,
    ArrowRightLeft,
    CheckCircle2,
    ChevronDown,
    Database,
    Edit2,
    Eye,
    FileCode2,
    List,
    Plus,
    Save,
    Search,
    SquareStack,
    Table2,
    Trash2,
    Wand2,
    X,
} from 'lucide-react';
import SqlEditor from '../../components/data/SqlEditor';
import Select from '../../components/common/Select';
import {
    createBusinessDictionary,
    createBusinessDictionaryItem,
    createDataDictionary,
    deleteBusinessDictionary,
    deleteBusinessDictionaryItem,
    deleteDataDictionary,
    getDataDictionaryItems,
    getBusinessDictionaries,
    getBusinessDictionaryItems,
    getDataDictionaries,
    getReportingConnectionSchemas,
    getReportingConnectionTables,
    getReportingConnections,
    getReportingDatasets,
    getReportingTableColumns,
    previewDataDictionaryDraft,
    updateBusinessDictionary,
    updateBusinessDictionaryItem,
    updateDataDictionary,
} from '../../services/api';
import type { Dataset, QueryColumn } from './types';
import { parseDatasetColumns } from './utils';

type SourceDictionaryType = 'dataset' | 'table' | 'sql';
type BusinessDictionaryType = 'enum' | 'hierarchy';
type DictionaryStorage = 'business' | 'source';

type DataDictionary = {
    id: number;
    key: string;
    name: string;
    dict_type: BusinessDictionaryType;
    source_type: 'static' | SourceDictionaryType;
    config_json: string;
    description?: string | null;
    category?: string | null;
    is_active: boolean;
};

type BusinessDictionary = {
    id: number;
    key: string;
    name: string;
    dict_type: BusinessDictionaryType;
    category?: string | null;
    description?: string | null;
    is_active: boolean;
    item_count: number;
};

type BusinessDictionaryItem = {
    id: number;
    dictionary_id: number;
    code: string;
    label: string;
    value?: string | null;
    parent_id?: number | null;
    sort_order?: number;
    status?: number;
    description?: string | null;
    extra_json?: string | null;
};

type UnifiedDictionary = {
    id: number;
    storage: DictionaryStorage;
    dict_type: BusinessDictionaryType;
    source_type: 'static' | SourceDictionaryType;
    key: string;
    name: string;
    description?: string | null;
    category?: string | null;
    is_active: boolean;
    item_count?: number;
    config_json?: string;
};

type SelectOption = {
    id: number;
    name: string;
    db_type?: string;
    connection_id?: number;
    last_columns_json?: string | null;
};

type DictionaryConfig = Record<string, unknown>;

type ApiErrorDetail = {
    errors?: unknown;
    message?: unknown;
};

type ApiErrorLike = {
    response?: {
        data?: {
            detail?: string | ApiErrorDetail;
        };
    };
    message?: unknown;
};

type ReportingConnectionSummary = Pick<SelectOption, 'id' | 'name' | 'db_type'>;

type PreviewItem = {
    key: string;
    label: string;
    value?: string | null;
    raw?: Record<string, unknown> | null;
};

type DictionaryFormState = {
    id?: number;
    storage: DictionaryStorage;
    dict_type: BusinessDictionaryType;
    source_type: 'static' | SourceDictionaryType;
    key: string;
    name: string;
    description: string;
    category: string;
    is_active: boolean;
    config: DictionaryConfig;
};

type DraftItem = {
    client_id: string;
    id?: number;
    code: string;
    label: string;
    value: string;
    parent_ref: string;
    sort_order: string;
    status: string;
    description: string;
    extra_json: string;
};

const DICT_TYPE_LABEL: Record<BusinessDictionaryType, string> = {
    enum: '键值类型',
    hierarchy: '层级类型',
};

const DICT_TYPE_DESC: Record<BusinessDictionaryType, string> = {
    enum: '适合状态、类型、选项等平铺键值场景。',
    hierarchy: '适合分类、地区、组织等父子层级场景。',
};

const SOURCE_LABEL: Record<'static' | SourceDictionaryType, string> = {
    static: '静态维护',
    dataset: '数据集来源',
    table: '数据表来源',
    sql: 'SQL 来源',
};

const SOURCE_DESC: Record<'static' | SourceDictionaryType, string> = {
    static: '直接在系统内维护字典项。',
    dataset: '直接复用数据中心里已经创建好的自定义数据集。',
    table: '从数据连接中的某张表实时读取字典值。',
    sql: '通过只读 SQL 自定义字典来源，适合复杂映射逻辑。',
};

const SOURCE_ICON: Record<'static' | SourceDictionaryType, ReactNode> = {
    static: <List size={16} />,
    dataset: <Database size={16} />,
    table: <Table2 size={16} />,
    sql: <FileCode2 size={16} />,
};

const createEmptyConfigByType = (sourceType: 'static' | SourceDictionaryType): DictionaryConfig => {
    if (sourceType === 'dataset') {
        return {
            dataset_id: '',
            key_field: '',
            label_field: '',
            value_field: '',
            parent_id_field: '',
        };
    }
    if (sourceType === 'table') {
        return {
            connection_id: '',
            schema_name: '',
            table_name: '',
            key_field: '',
            label_field: '',
            value_field: '',
            parent_id_field: '',
        };
    }
    if (sourceType === 'sql') {
        return {
            connection_id: '',
            sql_text: 'SELECT code, name\nFROM your_table\nLIMIT 50',
            params_json: {},
            key_field: '',
            label_field: '',
            value_field: '',
            parent_id_field: '',
        };
    }
    return {};
};

const createEmptyForm = (): DictionaryFormState => ({
    key: '',
    name: '',
    storage: 'business',
    dict_type: 'enum',
    source_type: 'static',
    description: '',
    category: 'common',
    is_active: true,
    config: createEmptyConfigByType('static'),
});

const createClientId = () => `draft-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const createEmptyDraftItem = (): DraftItem => ({
    client_id: createClientId(),
    code: '',
    label: '',
    value: '',
    parent_ref: '',
    sort_order: '0',
    status: '1',
    description: '',
    extra_json: '',
});

const toDraftItem = (item: BusinessDictionaryItem): DraftItem => ({
    client_id: `item-${item.id}`,
    id: item.id,
    code: item.code,
    label: item.label,
    value: item.value || '',
    parent_ref: item.parent_id ? `item-${item.parent_id}` : '',
    sort_order: String(item.sort_order ?? 0),
    status: String(item.status ?? 1),
    description: item.description || '',
    extra_json: item.extra_json || '',
});

const parseDictionaryConfig = (raw?: string | null): DictionaryConfig => {
    if (!raw) return {};
    try {
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch {
        return {};
    }
};

const parseJsonInput = (raw: string): Record<string, unknown> => {
    const text = raw.trim();
    if (!text) return {};
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error('参数 JSON 必须是对象格式');
    }
    return parsed as Record<string, unknown>;
};

const getErrorMessage = (error: unknown) => {
    const detail = (error as ApiErrorLike)?.response?.data?.detail;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail?.errors) && detail.errors.length) return detail.errors.join('\n');
    return (typeof detail?.message === 'string' ? detail.message : undefined)
        || (typeof (error as ApiErrorLike)?.message === 'string' ? (error as ApiErrorLike).message : undefined)
        || '发生未知错误';
};

const toColumnOptions = (columns: QueryColumn[] | Array<{ name: string; type?: string }>) =>
    (columns || []).map((column) => ({
        name: String(column.name),
        type: String(column.type || ''),
    }));

const buildIndentedOptions = (items: DraftItem[]) => {
    const itemMap = new Map(items.map((item) => [item.client_id, item]));
    const getLevel = (item: DraftItem) => {
        let level = 0;
        let currentParent = item.parent_ref;
        let guard = 0;
        while (currentParent && itemMap.has(currentParent) && guard < items.length + 1) {
            level += 1;
            currentParent = itemMap.get(currentParent)?.parent_ref || '';
            guard += 1;
        }
        return level;
    };

    return items.map((item) => ({
        id: item.client_id,
        label: `${'　'.repeat(getLevel(item))}${item.label || item.code || '未命名节点'}`,
    }));
};


export default function SourceDictionaryManager() {
    const [dictionaries, setDictionaries] = useState<UnifiedDictionary[]>([]);
    const [connections, setConnections] = useState<SelectOption[]>([]);
    const [datasets, setDatasets] = useState<SelectOption[]>([]);
    const [searchQuery, setSearchQuery] = useState('');
    const [filterType, setFilterType] = useState<'all' | BusinessDictionaryType>('all');
    const [isEditing, setIsEditing] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [isSaving, setIsSaving] = useState(false);
    const [previewLoading, setPreviewLoading] = useState(false);
    const [previewItems, setPreviewItems] = useState<PreviewItem[]>([]);
    const [previewColumns, setPreviewColumns] = useState<Array<{ name: string; type: string }>>([]);
    const [previewRows, setPreviewRows] = useState<Record<string, unknown>[]>([]);
    const [previewModalOpen, setPreviewModalOpen] = useState(false);
    const [tableSchemas, setTableSchemas] = useState<string[]>([]);
    const [tableOptions, setTableOptions] = useState<Array<{ table_name: string; schema_name?: string | null }>>([]);
    const [datasetPickerOpen, setDatasetPickerOpen] = useState(false);
    const [datasetSearchText, setDatasetSearchText] = useState('');
    const [tableSearchText, setTableSearchText] = useState('');
    const [tablePickerOpen, setTablePickerOpen] = useState(false);
    const [columnOptions, setColumnOptions] = useState<Array<{ name: string; type: string }>>([]);
    const [sqlParamsText, setSqlParamsText] = useState('{}');
    const [previewLimit, setPreviewLimit] = useState('50');
    const [currentDictionary, setCurrentDictionary] = useState<DictionaryFormState>(createEmptyForm());
    const [originalBusinessItems, setOriginalBusinessItems] = useState<BusinessDictionaryItem[]>([]);
    const [draftItems, setDraftItems] = useState<DraftItem[]>([]);

    const loadBaseData = async () => {
        setIsLoading(true);
        try {
            const [sourceRes, businessRes, connectionRes, datasetRes] = await Promise.all([
                getDataDictionaries(),
                getBusinessDictionaries(),
                getReportingConnections(),
                getReportingDatasets(),
            ]);

            const sourceCards: UnifiedDictionary[] = (sourceRes || [])
                .filter((item: DataDictionary) => item.source_type !== 'static')
                .map((item: DataDictionary) => ({
                    id: item.id,
                    storage: 'source',
                    dict_type: item.dict_type || 'enum',
                    source_type: item.source_type,
                    key: item.key,
                    name: item.name,
                    description: item.description,
                    category: item.category,
                    is_active: item.is_active,
                    config_json: item.config_json,
                }));

            const businessCards: UnifiedDictionary[] = (businessRes || []).map((item: BusinessDictionary) => ({
                id: item.id,
                storage: 'business',
                dict_type: item.dict_type,
                source_type: 'static',
                key: item.key,
                name: item.name,
                description: item.description,
                category: item.category,
                is_active: item.is_active,
                item_count: item.item_count,
            }));

            setDictionaries([...businessCards, ...sourceCards].sort((a, b) => a.name.localeCompare(b.name, 'zh-CN')));
            setConnections(
                ((connectionRes || []) as ReportingConnectionSummary[]).map((item) => ({
                    id: item.id,
                    name: item.name,
                    db_type: item.db_type,
                }))
            );
            setDatasets(
                (datasetRes || []).map((item: Dataset) => ({
                    id: item.id,
                    name: item.name,
                    connection_id: item.connection_id,
                    last_columns_json: item.last_columns_json,
                }))
            );
        } catch (error) {
            alert(`加载业务字典失败: ${getErrorMessage(error)}`);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        void loadBaseData();
    }, []);

    const filteredDictionaries = useMemo(() => {
        const keyword = searchQuery.trim().toLowerCase();
        return dictionaries.filter((item) => {
            if (filterType !== 'all' && item.dict_type !== filterType) return false;
            if (!keyword) return true;
            return [item.key, item.name, item.description, item.category, item.source_type, item.dict_type]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(keyword));
        });
    }, [dictionaries, filterType, searchQuery]);

    const visibleSourceLabel = SOURCE_LABEL[currentDictionary.source_type];

    const selectedDataset = datasets.find((item) => String(item.id) === String(currentDictionary.config.dataset_id || ''));
    const selectedConnection = connections.find((item) => String(item.id) === String(currentDictionary.config.connection_id || ''));

    const filteredDatasetsForPicker = useMemo(() => {
        const keyword = datasetSearchText.trim().toLowerCase();
        if (!keyword) return datasets;
        return datasets.filter((item) =>
            [item.name, item.connection_id]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(keyword))
        );
    }, [datasetSearchText, datasets]);

    const selectedDatasetLabel = selectedDataset?.name || '';

    const selectedTableLabel = useMemo(() => {
        const selected = tableOptions.find((item) => item.table_name === String(currentDictionary.config.table_name || ''));
        if (!selected) return '';
        return `${selected.schema_name ? `${selected.schema_name}.` : ''}${selected.table_name}`;
    }, [currentDictionary.config.table_name, tableOptions]);

    const filteredTableOptions = useMemo(() => {
        const keyword = tableSearchText.trim().toLowerCase();
        if (!keyword) return tableOptions;
        return tableOptions.filter((item) =>
            [item.table_name, item.schema_name]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(keyword))
        );
    }, [tableOptions, tableSearchText]);

    const resetEditorState = () => {
        setPreviewItems([]);
        setPreviewColumns([]);
        setPreviewRows([]);
        setTableSchemas([]);
        setTableOptions([]);
        setDatasetPickerOpen(false);
        setDatasetSearchText('');
        setTableSearchText('');
        setTablePickerOpen(false);
        setColumnOptions([]);
        setSqlParamsText('{}');
        setPreviewLimit('50');
        setOriginalBusinessItems([]);
        setDraftItems([]);
    };

    const openCreate = () => {
        setCurrentDictionary(createEmptyForm());
        resetEditorState();
        setIsEditing(true);
    };

    const updateConfig = (patch: DictionaryConfig) => {
        setCurrentDictionary((prev) => ({
            ...prev,
            config: { ...prev.config, ...patch },
        }));
    };

    const setDictType = (dictType: BusinessDictionaryType) => {
        setCurrentDictionary((prev) => ({
            ...prev,
            dict_type: dictType,
        }));
        if (dictType !== 'hierarchy') {
            setDraftItems((prev) => prev.map((item) => ({ ...item, parent_ref: '' })));
        }
    };

    const setSourceType = (sourceType: 'static' | SourceDictionaryType) => {
        const storage: DictionaryStorage = sourceType === 'static' ? 'business' : 'source';
        setCurrentDictionary((prev) => ({
            ...prev,
            storage,
            source_type: sourceType,
            config: createEmptyConfigByType(sourceType),
        }));
        setPreviewItems([]);
        setPreviewColumns([]);
        setPreviewRows([]);
        setColumnOptions([]);
        setTableSchemas([]);
        setTableOptions([]);
        setDatasetPickerOpen(false);
        setTablePickerOpen(false);
        setDatasetSearchText('');
        setTableSearchText('');
        setSqlParamsText('{}');
    };

    const loadConnectionMetadata = async (connectionId: number, schemaName?: string, tableName?: string) => {
        const [schemaRes, tableRes] = await Promise.all([
            getReportingConnectionSchemas(connectionId),
            getReportingConnectionTables(connectionId, schemaName || undefined),
        ]);
        setTableSchemas(schemaRes.schemas || []);
        setTableOptions(tableRes.tables || []);
        if (tableName) {
            const columnsRes = await getReportingTableColumns(connectionId, tableName, schemaName || undefined);
            setColumnOptions(toColumnOptions(columnsRes.columns || columnsRes || []));
            return;
        }
        setColumnOptions([]);
    };

    const openEdit = async (item: UnifiedDictionary) => {
        resetEditorState();
        setCurrentDictionary({
            id: item.id,
            key: item.key,
            name: item.name,
            storage: item.storage,
            dict_type: item.dict_type,
            source_type: item.source_type,
            description: item.description || '',
            category: item.category || 'common',
            is_active: item.is_active,
            config: item.storage === 'source' ? parseDictionaryConfig(item.config_json) : createEmptyConfigByType('static'),
        });

        try {
            if (item.storage === 'business') {
                const items = await getBusinessDictionaryItems(item.id, false);
                const nextItems = Array.isArray(items) ? items : [];
                setOriginalBusinessItems(nextItems);
                setDraftItems(nextItems.map(toDraftItem));
            } else {
                const config = parseDictionaryConfig(item.config_json);
                if (item.source_type === 'dataset') {
                    const dataset = datasets.find((entry) => entry.id === Number(config.dataset_id || 0));
                    setColumnOptions(toColumnOptions(parseDatasetColumns(dataset as Dataset | undefined)));
                }
                if (item.source_type === 'table' || item.source_type === 'sql') {
                    const connectionId = Number(config.connection_id || 0);
                    if (connectionId) {
                        await loadConnectionMetadata(connectionId, String(config.schema_name || ''), String(config.table_name || ''));
                    }
                }
                if (item.source_type === 'sql') {
                    setSqlParamsText(JSON.stringify(config.params_json || {}, null, 2));
                }
                try {
                    const preview = await getDataDictionaryItems(item.id, Number(previewLimit || 50));
                    const columns = toColumnOptions(preview.columns || []);
                    setPreviewItems(Array.isArray(preview.items) ? preview.items : []);
                    setPreviewColumns(columns);
                    setPreviewRows(Array.isArray(preview.rows) ? preview.rows : []);
                    if (columns.length) {
                        setColumnOptions(columns);
                    }
                } catch {
                    // Keep the editor usable even if the saved source cannot be previewed right now.
                }
            }
            setIsEditing(true);
        } catch (error) {
            alert(`加载字典配置失败: ${getErrorMessage(error)}`);
        }
    };

    const recommendMappingFromColumns = (columns: Array<{ name: string; type: string }>) => {
        const normalizedColumns = columns.map((column) => ({
            ...column,
            normalized: column.name.trim().toLowerCase(),
        }));
        const findByPriority = (candidates: string[]) =>
            normalizedColumns.find((column) => candidates.some((candidate) => column.normalized === candidate))
                || normalizedColumns.find((column) => candidates.some((candidate) => column.normalized.includes(candidate)));

        const keyColumn = findByPriority(['key', 'code', 'id', 'value']);
        const labelColumn = findByPriority(['label', 'name', 'title', 'text', 'caption', 'desc']);
        const valueColumn = findByPriority(['value', 'raw_value', 'ext_value', 'data']);

        return {
            key_field: keyColumn?.name || columns[0]?.name || '',
            label_field: labelColumn?.name || columns[1]?.name || columns[0]?.name || '',
            value_field: valueColumn?.name || columns[2]?.name || '',
        };
    };

    const handleDatasetChange = (datasetId: string) => {
        const dataset = datasets.find((item) => String(item.id) === datasetId);
        updateConfig({
            dataset_id: datasetId,
            key_field: '',
            label_field: '',
            value_field: '',
            parent_id_field: '',
        });
        setDatasetPickerOpen(false);
        setColumnOptions(toColumnOptions(parseDatasetColumns(dataset as Dataset | undefined)));
        setPreviewItems([]);
        setPreviewColumns([]);
        setPreviewRows([]);
    };

    const handleConnectionChange = async (connectionId: string) => {
        updateConfig({
            connection_id: connectionId,
            schema_name: '',
            table_name: '',
            key_field: '',
            label_field: '',
            value_field: '',
            parent_id_field: '',
        });
        setPreviewItems([]);
        setPreviewColumns([]);
        setPreviewRows([]);
        setTableSearchText('');
        setTablePickerOpen(false);
        setColumnOptions([]);
        if (!connectionId) {
            setTableSchemas([]);
            setTableOptions([]);
            return;
        }
        await loadConnectionMetadata(Number(connectionId));
    };

    const handleTableChange = async (tableName: string) => {
        updateConfig({
            table_name: tableName,
            key_field: '',
            label_field: '',
            value_field: '',
            parent_id_field: '',
        });
        const connectionId = Number(currentDictionary.config.connection_id || 0);
        if (!connectionId || !tableName) {
            setColumnOptions([]);
            return;
        }
        const columnsRes = await getReportingTableColumns(
            connectionId,
            tableName,
            String(currentDictionary.config.schema_name || '') || undefined
        );
        setColumnOptions(toColumnOptions(columnsRes.columns || columnsRes || []));
        setPreviewItems([]);
        setPreviewColumns([]);
        setPreviewRows([]);
        setTablePickerOpen(false);
    };

    const applyPreviewColumnsToMapping = () => {
        const currentKey = String(currentDictionary.config.key_field || '');
        const currentLabel = String(currentDictionary.config.label_field || '');
        const currentValue = String(currentDictionary.config.value_field || '');
        const available = previewColumns.map((item) => item.name);
        const suggested = recommendMappingFromColumns(previewColumns);
        updateConfig({
            key_field: available.includes(currentKey) ? currentKey : suggested.key_field,
            label_field: available.includes(currentLabel) ? currentLabel : suggested.label_field,
            value_field: available.includes(currentValue) ? currentValue : suggested.value_field,
        });
        if (currentDictionary.source_type === 'sql') {
            setColumnOptions(previewColumns);
        }
    };

    const applyColumnSuggestions = () => {
        if (!columnOptions.length) return;
        updateConfig(recommendMappingFromColumns(columnOptions));
    };

    const handleAddDraftItemRow = () => {
        setDraftItems((prev) => [...prev, createEmptyDraftItem()]);
    };

    const updateDraftItem = (clientId: string, patch: Partial<DraftItem>) => {
        setDraftItems((prev) =>
            prev.map((item) => {
                if (item.client_id !== clientId) return item;
                const nextItem = { ...item, ...patch };
                if (currentDictionary.dict_type !== 'hierarchy') {
                    nextItem.parent_ref = '';
                }
                return nextItem;
            })
        );
    };

    const handleRemoveDraftItem = (item: DraftItem) => {
        const nextItems = draftItems.filter((entry) => entry.client_id !== item.client_id);
        setDraftItems(nextItems.map((entry) => (entry.parent_ref === item.client_id ? { ...entry, parent_ref: '' } : entry)));
    };

    const getParentOptionsFor = (clientId: string) => {
        if (currentDictionary.dict_type !== 'hierarchy') return [];
        return buildIndentedOptions(draftItems.filter((item) => item.client_id !== clientId));
    };

    const syncBusinessItems = async (dictionaryId: number, dictType: BusinessDictionaryType) => {
        const originalIdSet = new Set(originalBusinessItems.map((item) => item.id));
        const currentIdSet = new Set(draftItems.map((item) => item.id).filter((item): item is number => typeof item === 'number'));
        const deletedIds = [...originalIdSet].filter((id) => !currentIdSet.has(id));

        const resolvedIdMap = new Map<string, number>();
        draftItems.forEach((item) => {
            if (item.id) resolvedIdMap.set(item.client_id, item.id);
        });

        const pendingItems = draftItems.map((item) => ({ ...item }));
        let guard = 0;
        while (pendingItems.length) {
            let progressed = false;
            for (let index = 0; index < pendingItems.length; index += 1) {
                const item = pendingItems[index];
                const parentId = dictType === 'hierarchy' && item.parent_ref ? resolvedIdMap.get(item.parent_ref) : null;

                if (dictType === 'hierarchy' && item.parent_ref && !parentId) continue;

                const payload = {
                    code: item.code.trim(),
                    label: item.label.trim(),
                    value: item.value.trim() || null,
                    parent_id: dictType === 'hierarchy' ? parentId || null : null,
                    sort_order: Number(item.sort_order || 0),
                    status: Number(item.status || 1),
                    description: item.description.trim() || null,
                    extra_json: item.extra_json.trim() || null,
                };

                if (item.id) {
                    await updateBusinessDictionaryItem(item.id, payload);
                    resolvedIdMap.set(item.client_id, item.id);
                } else {
                    const created = await createBusinessDictionaryItem(dictionaryId, payload);
                    if (created?.id) {
                        resolvedIdMap.set(item.client_id, created.id);
                    }
                }

                pendingItems.splice(index, 1);
                progressed = true;
                index -= 1;
            }

            guard += 1;
            if (!progressed || guard > draftItems.length + 2) {
                throw new Error('层级字典存在无法解析的父子关系，请检查父级配置。');
            }
        }

        for (const id of deletedIds) {
            await deleteBusinessDictionaryItem(id);
        }
    };

    const runPreview = async () => {
        if (currentDictionary.storage !== 'source') return;
        setPreviewLoading(true);
        try {
            const config =
                currentDictionary.source_type === 'sql'
                    ? {
                          ...currentDictionary.config,
                          params_json: parseJsonInput(sqlParamsText),
                      }
                    : currentDictionary.config;

            const result = await previewDataDictionaryDraft({
                dict_type: currentDictionary.dict_type,
                source_type: currentDictionary.source_type as SourceDictionaryType,
                config_json: JSON.stringify(config),
                limit: Number(previewLimit || 50),
            });
            const columns = toColumnOptions(result.columns || []);
            setPreviewItems(result.items || []);
            setPreviewColumns(columns);
            setPreviewRows(Array.isArray(result.rows) ? result.rows : []);
            if (currentDictionary.source_type === 'sql') {
                setColumnOptions(columns);
            }
            setPreviewModalOpen(true);
        } catch (error) {
            alert(`预览失败: ${getErrorMessage(error)}`);
        } finally {
            setPreviewLoading(false);
        }
    };

    const validationMessages = useMemo(() => {
        const messages: string[] = [];
        if (!currentDictionary.key.trim()) messages.push('请填写字典标识');
        if (!currentDictionary.name.trim()) messages.push('请填写字典名称');

        if (currentDictionary.storage === 'business') {
            const validItems = draftItems.filter((item) => item.code.trim() && item.label.trim());
            if (!validItems.length) messages.push('业务字典至少需要 1 条有效字典项');
            return messages;
        }

        if (currentDictionary.source_type === 'dataset' && !String(currentDictionary.config.dataset_id || '')) {
            messages.push('请选择数据集');
        }
        if (currentDictionary.source_type === 'table') {
            if (!String(currentDictionary.config.connection_id || '')) messages.push('请选择数据连接');
            if (!String(currentDictionary.config.table_name || '')) messages.push('请选择数据表');
        }
        if (currentDictionary.source_type === 'sql') {
            if (!String(currentDictionary.config.connection_id || '')) messages.push('请选择 SQL 执行连接');
            if (!String(currentDictionary.config.sql_text || '').trim()) messages.push('请输入 SQL 语句');
        }
        if (currentDictionary.source_type === 'dataset' || currentDictionary.source_type === 'table') {
            if (!String(currentDictionary.config.key_field || '')) messages.push('请配置键字段');
            if (!String(currentDictionary.config.label_field || '')) messages.push('请配置显示字段');
            if (currentDictionary.dict_type === 'hierarchy' && !String(currentDictionary.config.parent_id_field || '')) messages.push('请配置父级ID字段');
        }
        if (currentDictionary.source_type === 'sql' && previewColumns.length > 0) {
            if (!String(currentDictionary.config.key_field || '')) messages.push('请配置键字段');
            if (!String(currentDictionary.config.label_field || '')) messages.push('请配置显示字段');
            if (currentDictionary.dict_type === 'hierarchy' && !String(currentDictionary.config.parent_id_field || '')) messages.push('请配置父级ID字段');
        }
        return messages;
    }, [currentDictionary, draftItems, previewColumns.length]);

    const canPreviewCurrentDictionary =
        currentDictionary.storage === 'source'
        && !!currentDictionary.key.trim()
        && !!currentDictionary.name.trim()
        && (
            currentDictionary.source_type === 'sql'
                ? !!String(currentDictionary.config.connection_id || '') && !!String(currentDictionary.config.sql_text || '').trim()
                : currentDictionary.source_type === 'table'
                  ? !!String(currentDictionary.config.connection_id || '') && !!String(currentDictionary.config.table_name || '')
                  : !!String(currentDictionary.config.dataset_id || '')
        );

    const mappingPreview = {
        key: String(currentDictionary.config.key_field || ''),
        label: String(currentDictionary.config.label_field || ''),
        value: String(currentDictionary.config.value_field || ''),
    };

    const handleDelete = async (item: UnifiedDictionary) => {
        if (!window.confirm(`确定要删除“${item.name}”吗？`)) return;
        try {
            if (item.storage === 'business') {
                await deleteBusinessDictionary(item.id);
            } else {
                await deleteDataDictionary(item.id);
            }
            await loadBaseData();
        } catch (error) {
            alert(`删除失败: ${getErrorMessage(error)}`);
        }
    };

    const handleSave = async () => {
        if (!currentDictionary.key.trim()) {
            alert('请填写字典标识');
            return;
        }
        if (!currentDictionary.name.trim()) {
            alert('请填写字典名称');
            return;
        }

        setIsSaving(true);
        try {
            if (currentDictionary.storage === 'business') {
                const payload = {
                    key: currentDictionary.key.trim(),
                    name: currentDictionary.name.trim(),
                    dict_type: currentDictionary.dict_type,
                    category: currentDictionary.category.trim() || 'common',
                    description: currentDictionary.description.trim() || null,
                    is_active: currentDictionary.is_active,
                };

                let dictionaryId = currentDictionary.id;
                if (dictionaryId) {
                    await updateBusinessDictionary(dictionaryId, payload);
                } else {
                    const created = await createBusinessDictionary(payload);
                    dictionaryId = created?.id;
                }

                if (!dictionaryId) {
                    throw new Error('未能获取业务字典 ID');
                }

                await syncBusinessItems(dictionaryId, currentDictionary.dict_type);
            } else {
                const payload = {
                    key: currentDictionary.key.trim(),
                    name: currentDictionary.name.trim(),
                    dict_type: currentDictionary.dict_type,
                    source_type: currentDictionary.source_type as SourceDictionaryType,
                    description: currentDictionary.description.trim() || null,
                    category: currentDictionary.category.trim() || 'common',
                    is_active: currentDictionary.is_active,
                    config_json: JSON.stringify(
                        currentDictionary.source_type === 'sql'
                            ? {
                                  ...currentDictionary.config,
                                  params_json: parseJsonInput(sqlParamsText),
                              }
                            : currentDictionary.config
                    ),
                };
                if (currentDictionary.id) {
                    await updateDataDictionary(currentDictionary.id, payload);
                } else {
                    await createDataDictionary(payload);
                }
            }

            setIsEditing(false);
            await loadBaseData();
        } catch (error) {
            alert(`保存失败: ${getErrorMessage(error)}`);
        } finally {
            setIsSaving(false);
        }
    };

    const mappingSelector = (
        <div className="dictionary-mapping-shell">
            <div className="dictionary-mapping-toolbar">
                <div className="dictionary-mapping-summary">
                    <strong>字段映射</strong>
                    <span>{columnOptions.length ? `当前已识别 ${columnOptions.length} 个字段` : '先选择数据源或先运行预览'}</span>
                </div>
                <button type="button" className="btn-outline" onClick={applyColumnSuggestions} disabled={!columnOptions.length}>
                    <Wand2 size={14} />
                    自动映射
                </button>
            </div>
            <div className="form-grid two dictionary-mapping-grid">
                <label className="form-block">
                    <span>键字段</span>
                    <Select
                        value={mappingPreview.key}
                        onChange={(v) => updateConfig({ key_field: v })}
                        options={[
                            { value: '', label: '请选择' },
                            ...columnOptions.map(col => ({ value: col.name, label: col.name })),
                        ]}
                    />
                </label>
                <label className="form-block">
                    <span>显示字段</span>
                    <Select
                        value={mappingPreview.label}
                        onChange={(v) => updateConfig({ label_field: v })}
                        options={[
                            { value: '', label: '请选择' },
                            ...columnOptions.map(col => ({ value: col.name, label: col.name })),
                        ]}
                    />
                </label>
                <label className="form-block dictionary-field-span">
                    <span>值字段</span>
                    <Select
                        value={mappingPreview.value}
                        onChange={(v) => updateConfig({ value_field: v })}
                        options={[
                            { value: '', label: '可选' },
                            ...columnOptions.map(col => ({ value: col.name, label: col.name })),
                        ]}
                    />
                </label>
                {currentDictionary.dict_type === 'hierarchy' ? (
                    <label className="form-block dictionary-field-span">
                        <span>父级ID字段</span>
                        <Select
                            value={String(currentDictionary.config.parent_id_field || '')}
                            onChange={(v) => updateConfig({ parent_id_field: v })}
                            options={[
                                { value: '', label: '请选择' },
                                ...columnOptions.map(col => ({ value: col.name, label: col.name })),
                            ]}
                        />
                    </label>
                ) : null}
            </div>
        </div>
    );

    return (
        <>
            <div className="dictionary-page-header">
                <div className="dictionary-page-title">
                    <div className="dictionary-page-icon">
                        <List size={22} />
                    </div>
                    <div className="dictionary-page-text">
                        <h2>业务字典</h2>
                        <p>统一管理键值字典、层级字典与动态来源字典</p>
                    </div>
                </div>
                <div className="dictionary-page-stats">
                    <div className="dict-stat-item">
                        <span className="dict-stat-value">{dictionaries.length}</span>
                        <span className="dict-stat-label">字典总数</span>
                    </div>
                    <div className="dict-stat-divider" />
                    <div className="dict-stat-item">
                        <span className="dict-stat-value">{dictionaries.filter(d => d.is_active).length}</span>
                        <span className="dict-stat-label">已启用</span>
                    </div>
                </div>
                <button className="dictionary-add-btn" onClick={openCreate}>
                    <Plus size={18} />
                    新增字典
                </button>
            </div>

            <div className="dictionary-toolbar">
                <div className="dictionary-search-box">
                    <Search size={16} />
                    <input
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        placeholder="搜索字典键、名称、描述..."
                    />
                    {searchQuery && (
                        <button className="dictionary-search-clear" onClick={() => setSearchQuery('')}>
                            <X size={14} />
                        </button>
                    )}
                </div>
                <div className="dictionary-filter-group">
                    <Select
                        className="dictionary-filter-select"
                        value={filterType}
                        onChange={(v) => setFilterType(v as 'all' | BusinessDictionaryType)}
                        options={[
                            { value: 'all', label: '全部类型' },
                            { value: 'enum', label: '键值类型' },
                            { value: 'hierarchy', label: '层级类型' },
                        ]}
                    />
                </div>
            </div>

            <div className="dictionary-panel">
                <div className="dictionary-grid">
                    {filteredDictionaries.map((item) => (
                        <div key={`${item.storage}-${item.id}`} className="dictionary-card">
                            <div className="dictionary-card-accent" />
                            <div className="dictionary-card-glow" />
                            <div className="dictionary-card-header">
                                <div className="dictionary-card-icon" data-type={item.dict_type}>
                                    {SOURCE_ICON[item.source_type]}
                                </div>
                                <div className="dictionary-card-info">
                                    <div className="dictionary-card-key">{item.key}</div>
                                    <div className="dictionary-card-name">{item.name}</div>
                                </div>
                                <div className="dictionary-card-actions">
                                    <button onClick={() => void openEdit(item)} className="dictionary-action-btn" title="编辑">
                                        <Edit2 size={14} />
                                    </button>
                                    <button onClick={() => void handleDelete(item)} className="dictionary-action-btn delete" title="删除">
                                        <Trash2 size={14} />
                                    </button>
                                </div>
                            </div>

                            <div className="dictionary-card-badges">
                                <span className="dict-badge type">{DICT_TYPE_LABEL[item.dict_type]}</span>
                                <span className="dict-badge source">{SOURCE_LABEL[item.source_type]}</span>
                                <span className={`dict-badge ${item.is_active ? 'active' : 'inactive'}`}>
                                    {item.is_active ? '启用' : '停用'}
                                </span>
                                {item.category && <span className="dict-badge category">{item.category}</span>}
                                {typeof item.item_count === 'number' && (
                                    <span className="dict-badge count">
                                        <SquareStack size={12} />
                                        {item.item_count}
                                    </span>
                                )}
                            </div>

                            <p className="dictionary-card-desc">{item.description || '暂无描述信息'}</p>
                        </div>
                    ))}

                    {!filteredDictionaries.length && (
                        <div className="dictionary-empty-state">
                            <div className="empty-icon">
                                <SquareStack size={48} />
                            </div>
                            <p>{isLoading ? '字典加载中...' : '暂无匹配的字典，点击上方按钮新增'}</p>
                        </div>
                    )}
                </div>
            </div>

            {isEditing && typeof document !== 'undefined' ? createPortal(
                <div className="modal-overlay">
                    <div className="modal-content-pro dictionary-modal">
                        <header className="modal-header-clean dictionary-modal-header-sticky">
                            <div>
                                <h3 className="font-bold text-slate-900">{currentDictionary.id ? '编辑字典' : '新增字典'}</h3>
                                <p className="dictionary-modal-subtitle">{DICT_TYPE_DESC[currentDictionary.dict_type]} {SOURCE_DESC[currentDictionary.source_type]}</p>
                            </div>
                            <div className="dictionary-header-actions">
                                {currentDictionary.storage === 'source' ? (
                                    <button
                                        type="button"
                                        className="btn-outline"
                                        onClick={() => void runPreview()}
                                        disabled={previewLoading || !canPreviewCurrentDictionary}
                                    >
                                        <Eye size={14} />
                                        {previewLoading ? '预览中...' : '预览字典'}
                                    </button>
                                ) : null}
                                <button className="btn-secondary-clean px-4 text-sm" onClick={() => setIsEditing(false)} disabled={isSaving}>
                                    取消
                                </button>
                                <button className="btn-primary-clean px-6 flex items-center gap-2" onClick={() => void handleSave()} disabled={isSaving}>
                                    <Save size={16} />
                                    {isSaving ? '保存中...' : '保存字典'}
                                </button>
                                <button onClick={() => setIsEditing(false)} className="text-slate-400 hover:text-slate-600" disabled={isSaving}>
                                    <X size={20} />
                                </button>
                            </div>
                        </header>

                        <div className="dictionary-modal-body-scroll">
                            <div className="dictionary-layout compact">
                                <div className="dictionary-main">
                                    <div className="dictionary-section-card">
                                        <div className="dictionary-section-head">
                                            <div>
                                                <strong>基础属性</strong>
                                                <p>在同一个窗口里完成所有字典配置。</p>
                                            </div>
                                        </div>
                                        <div className="form-grid two">
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">字典标识</label>
                                                <input
                                                    className="modern-input-pro font-mono text-sm"
                                                    value={currentDictionary.key}
                                                    onChange={(e) => setCurrentDictionary((prev) => ({ ...prev, key: e.target.value }))}
                                                    placeholder="例如 customer_level"
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">字典名称</label>
                                                <input
                                                    className="modern-input-pro text-sm"
                                                    value={currentDictionary.name}
                                                    onChange={(e) => setCurrentDictionary((prev) => ({ ...prev, name: e.target.value }))}
                                                    placeholder="例如 客户等级字典"
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">字典类型</label>
                                                <Select
                                                    value={currentDictionary.dict_type}
                                                    onChange={(v) => setDictType(v as BusinessDictionaryType)}
                                                    options={[
                                                        { value: 'enum', label: '键值类型' },
                                                        { value: 'hierarchy', label: '层级类型' },
                                                    ]}
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">数据来源</label>
                                                <Select
                                                    value={currentDictionary.source_type}
                                                    onChange={(v) => setSourceType(v as 'static' | SourceDictionaryType)}
                                                    options={[
                                                        { value: 'static', label: '静态维护（直接在系统内维护字典项）' },
                                                        { value: 'dataset', label: '数据集来源（复用数据中心已有数据集）' },
                                                        { value: 'table', label: '数据表来源（从连接表实时读取）' },
                                                        { value: 'sql', label: 'SQL 来源（通过只读 SQL 自定义）' },
                                                    ]}
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">启用状态</label>
                                                <label className="settings-switch-row">
                                                    <input
                                                        type="checkbox"
                                                        checked={currentDictionary.is_active}
                                                        onChange={(e) => setCurrentDictionary((prev) => ({ ...prev, is_active: e.target.checked }))}
                                                    />
                                                    <em>{currentDictionary.is_active ? '当前启用' : '当前停用'}</em>
                                                </label>
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">分类</label>
                                                <input
                                                    className="modern-input-pro text-sm"
                                                    value={currentDictionary.category}
                                                    onChange={(e) => setCurrentDictionary((prev) => ({ ...prev, category: e.target.value }))}
                                                    placeholder="例如 common"
                                                />
                                            </div>
                                            <div className="field-container horizontal">
                                                <label className="modern-label text-xs">描述</label>
                                                <input
                                                    className="modern-input-pro text-sm"
                                                    value={currentDictionary.description}
                                                    onChange={(e) => setCurrentDictionary((prev) => ({ ...prev, description: e.target.value }))}
                                                    placeholder="描述字典用途，方便后续维护"
                                                />
                                            </div>
                                        </div>
                                    </div>

                                    {currentDictionary.source_type === 'static' ? (
                                        <div className="dictionary-section-card">
                                            <div className="dictionary-section-head">
                                                <div>
                                                    <strong>{currentDictionary.dict_type === 'hierarchy' ? '层级项列表' : '字典项列表'}</strong>
                                                    <p>直接在多行子表里增删改字典项，保存字典时会一并提交。</p>
                                                </div>
                                                <div className="dictionary-header-actions">
                                                    <button type="button" className="btn-outline" onClick={handleAddDraftItemRow}>
                                                        <Plus size={14} />
                                                        {currentDictionary.dict_type === 'hierarchy' ? '新增节点' : '新增字典项'}
                                                    </button>
                                                </div>
                                            </div>
                                            <div className="dictionary-config-grid">
                                                <div className="dictionary-section-head">
                                                    <div>
                                                        <strong>当前字典项</strong>
                                                        <p>{draftItems.length} 项，可直接在行内维护。</p>
                                                    </div>
                                                </div>

                                                {draftItems.length ? (
                                                    <div className="dictionary-inline-table-wrap">
                                                        <table className="dictionary-inline-table">
                                                            <thead>
                                                                <tr>
                                                                    <th>字典键</th>
                                                                    <th>显示名称</th>
                                                                    <th>业务值</th>
                                                                    {currentDictionary.dict_type === 'hierarchy' ? <th>父级节点</th> : null}
                                                                    <th>状态</th>
                                                                    <th>排序</th>
                                                                    <th>描述</th>
                                                                    <th>扩展 JSON</th>
                                                                    <th>操作</th>
                                                                </tr>
                                                            </thead>
                                                            <tbody>
                                                                {draftItems.map((item) => (
                                                                    <tr key={item.client_id}>
                                                                        <td>
                                                                            <input
                                                                                className="modern-input-pro text-sm font-mono"
                                                                                value={item.code}
                                                                                onChange={(e) => updateDraftItem(item.client_id, { code: e.target.value })}
                                                                                placeholder="字典键"
                                                                            />
                                                                        </td>
                                                                        <td>
                                                                            <input
                                                                                className="modern-input-pro text-sm"
                                                                                value={item.label}
                                                                                onChange={(e) => updateDraftItem(item.client_id, { label: e.target.value })}
                                                                                placeholder="显示名称"
                                                                            />
                                                                        </td>
                                                                        <td>
                                                                            <input
                                                                                className="modern-input-pro text-sm"
                                                                                value={item.value}
                                                                                onChange={(e) => updateDraftItem(item.client_id, { value: e.target.value })}
                                                                                placeholder="业务值"
                                                                            />
                                                                        </td>
                                                                        {currentDictionary.dict_type === 'hierarchy' ? (
                                                                            <td>
                                                                                <Select
                                                                                    value={item.parent_ref}
                                                                                    onChange={(v) => updateDraftItem(item.client_id, { parent_ref: v })}
                                                                                    options={[
                                                                                        { value: '', label: '作为根节点' },
                                                                                        ...getParentOptionsFor(item.client_id).map((option) => ({ value: option.id, label: option.label })),
                                                                                    ]}
                                                                                />
                                                                            </td>
                                                                        ) : null}
                                                                        <td>
                                                                            <Select
                                                                                value={item.status}
                                                                                onChange={(v) => updateDraftItem(item.client_id, { status: v })}
                                                                                dropdownMinWidth={120}
                                                                                options={[
                                                                                    { value: '1', label: '启用' },
                                                                                    { value: '0', label: '停用' },
                                                                                ]}
                                                                            />
                                                                        </td>
                                                                        <td>
                                                                            <input
                                                                                className="modern-input-pro text-sm"
                                                                                value={item.sort_order}
                                                                                onChange={(e) => updateDraftItem(item.client_id, { sort_order: e.target.value })}
                                                                                placeholder="0"
                                                                            />
                                                                        </td>
                                                                        <td>
                                                                            <textarea
                                                                                className="modern-textarea-pro text-sm dictionary-inline-textarea"
                                                                                value={item.description}
                                                                                onChange={(e) => updateDraftItem(item.client_id, { description: e.target.value })}
                                                                                placeholder="描述"
                                                                            />
                                                                        </td>
                                                                        <td>
                                                                            <textarea
                                                                                className="modern-textarea-pro text-sm font-mono dictionary-inline-textarea"
                                                                                value={item.extra_json}
                                                                                onChange={(e) => updateDraftItem(item.client_id, { extra_json: e.target.value })}
                                                                                placeholder='{"color":"green"}'
                                                                            />
                                                                        </td>
                                                                        <td>
                                                                            <button
                                                                                type="button"
                                                                                className="action-btn-pro text-red-400 hover:bg-red-50"
                                                                                onClick={() => handleRemoveDraftItem(item)}
                                                                            >
                                                                                <Trash2 size={14} />
                                                                            </button>
                                                                        </td>
                                                                    </tr>
                                                                ))}
                                                            </tbody>
                                                        </table>
                                                    </div>
                                                ) : (
                                                    <div className="empty-box">
                                                        {currentDictionary.dict_type === 'hierarchy'
                                                            ? '当前还没有层级节点，点击右上角新增节点开始维护。'
                                                            : '当前还没有键值项，点击右上角新增字典项开始维护。'}
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    ) : null}

                                    {currentDictionary.source_type === 'dataset' ? (
                                        <div className="dictionary-section-card">
                                            <div className="dictionary-section-head">
                                                <div>
                                                    <strong>数据集映射</strong>
                                                    <p>选择已创建的数据集，然后把字段映射成字典键、显示名称和业务值。</p>
                                                </div>
                                            </div>
                                            <div className="dictionary-config-grid">
                                                <label className="form-block">
                                                    <span>数据集</span>
                                                    <div className="dictionary-table-picker">
                                                        <button
                                                            type="button"
                                                            className={`dictionary-table-picker-trigger ${datasetPickerOpen ? 'active' : ''}`}
                                                            onClick={() => setDatasetPickerOpen((open) => !open)}
                                                        >
                                                            <span>{selectedDatasetLabel || '请选择数据集'}</span>
                                                            <ChevronDown size={16} />
                                                        </button>
                                                        {datasetPickerOpen ? (
                                                            <div className="dictionary-table-picker-panel">
                                                                <div className="dictionary-table-picker-search">
                                                                    <Search size={14} className="text-slate-400" />
                                                                    <input
                                                                        value={datasetSearchText}
                                                                        onChange={(e) => setDatasetSearchText(e.target.value)}
                                                                        placeholder="搜索数据集名称"
                                                                    />
                                                                </div>
                                                                <div className="dictionary-table-picker-list">
                                                                    {filteredDatasetsForPicker.length ? (
                                                                        filteredDatasetsForPicker.map((item) => {
                                                                            const isActive = String(currentDictionary.config.dataset_id || '') === String(item.id);
                                                                            return (
                                                                                <button
                                                                                    key={item.id}
                                                                                    type="button"
                                                                                    className={`dictionary-table-picker-item ${isActive ? 'active' : ''}`}
                                                                                    onClick={() => handleDatasetChange(String(item.id))}
                                                                                >
                                                                                    <strong>{item.name}</strong>
                                                                                    <small>连接 ID: {item.connection_id || '-'}</small>
                                                                                </button>
                                                                            );
                                                                        })
                                                                    ) : (
                                                                        <div className="empty-box">没有匹配的数据集，请尝试其他关键字。</div>
                                                                    )}
                                                                </div>
                                                            </div>
                                                        ) : null}
                                                    </div>
                                                </label>
                                                {mappingSelector}
                                            </div>
                                        </div>
                                    ) : null}

                                    {currentDictionary.source_type === 'table' ? (
                                        <div className="dictionary-section-card">
                                            <div className="dictionary-section-head">
                                                <div>
                                                    <strong>数据表映射</strong>
                                                    <p>从已有连接选择 schema 与数据表，再指定映射字段。</p>
                                                </div>
                                            </div>
                                            <div className="dictionary-config-grid">
                                                <div className="form-grid two">
                                                    <label className="form-block">
                                                        <span>连接</span>
                                                        <select
                                                            value={String(currentDictionary.config.connection_id || '')}
                                                            onChange={(e) => void handleConnectionChange(e.target.value)}
                                                        >
                                                            <option value="">请选择连接</option>
                                                            {connections.map((item) => (
                                                                <option key={item.id} value={item.id}>
                                                                    {item.name}
                                                                </option>
                                                            ))}
                                                        </select>
                                                    </label>
                                                    <label className="form-block">
                                                        <span>Schema</span>
                                                        <select
                                                            value={String(currentDictionary.config.schema_name || '')}
                                                            onChange={(e) => {
                                                                updateConfig({
                                                                    schema_name: e.target.value,
                                                                    table_name: '',
                                                                    key_field: '',
                                                                    label_field: '',
                                                                    value_field: '',
                                                                });
                                                                void loadConnectionMetadata(Number(currentDictionary.config.connection_id || 0), e.target.value);
                                                            }}
                                                        >
                                                            <option value="">默认</option>
                                                            {tableSchemas.map((schema) => (
                                                                <option key={schema} value={schema}>
                                                                    {schema}
                                                                </option>
                                                            ))}
                                                        </select>
                                                    </label>
                                                </div>
                                                <label className="form-block">
                                                    <span>数据表</span>
                                                    <div className="dictionary-table-picker">
                                                        <button
                                                            type="button"
                                                            className={`dictionary-table-picker-trigger ${tablePickerOpen ? 'active' : ''}`}
                                                            onClick={() => setTablePickerOpen((open) => !open)}
                                                        >
                                                            <span>{selectedTableLabel || '请选择数据表'}</span>
                                                            <ChevronDown size={16} />
                                                        </button>
                                                        {tablePickerOpen ? (
                                                            <div className="dictionary-table-picker-panel">
                                                                <div className="dictionary-table-picker-search">
                                                                    <Search size={14} className="text-slate-400" />
                                                                    <input
                                                                        value={tableSearchText}
                                                                        onChange={(e) => setTableSearchText(e.target.value)}
                                                                        placeholder="搜索表名或 Schema"
                                                                    />
                                                                </div>
                                                                <div className="dictionary-table-picker-list">
                                                                    {filteredTableOptions.length ? (
                                                                        filteredTableOptions.map((item) => {
                                                                            const isActive = String(currentDictionary.config.table_name || '') === item.table_name;
                                                                            return (
                                                                                <button
                                                                                    key={`${item.schema_name || 'default'}-${item.table_name}`}
                                                                                    type="button"
                                                                                    className={`dictionary-table-picker-item ${isActive ? 'active' : ''}`}
                                                                                    onClick={() => void handleTableChange(item.table_name)}
                                                                                >
                                                                                    <strong>{item.table_name}</strong>
                                                                                    <small>{item.schema_name || 'default schema'}</small>
                                                                                </button>
                                                                            );
                                                                        })
                                                                    ) : (
                                                                        <div className="empty-box">没有匹配的数据表，请尝试更换关键字。</div>
                                                                    )}
                                                                </div>
                                                            </div>
                                                        ) : null}
                                                    </div>
                                                </label>
                                                {mappingSelector}
                                            </div>
                                        </div>
                                    ) : null}

                                    {currentDictionary.source_type === 'sql' ? (
                                        <div className="dictionary-section-card">
                                            <div className="dictionary-section-head">
                                                <div>
                                                    <strong>SQL 来源</strong>
                                                    <p>可直接写只读 SQL，预览后自动加载返回字段做映射。</p>
                                                </div>
                                                <button type="button" className="btn-outline" onClick={applyPreviewColumnsToMapping} disabled={!previewColumns.length}>
                                                    <ArrowRightLeft size={14} />
                                                    使用预览字段映射
                                                </button>
                                            </div>
                                            <div className="dictionary-config-grid">
                                                <label className="form-block">
                                                    <span>连接</span>
                                                    <select
                                                        value={String(currentDictionary.config.connection_id || '')}
                                                        onChange={(e) => void handleConnectionChange(e.target.value)}
                                                    >
                                                        <option value="">请选择连接</option>
                                                        {connections.map((item) => (
                                                            <option key={item.id} value={item.id}>
                                                                {item.name}
                                                            </option>
                                                        ))}
                                                    </select>
                                                </label>
                                                <div className="form-block">
                                                    <span>SQL 语句</span>
                                                    <SqlEditor
                                                        value={String(currentDictionary.config.sql_text || '')}
                                                        onChange={(value) => updateConfig({ sql_text: value })}
                                                        dialect={selectedConnection?.db_type || 'postgresql'}
                                                        onPreview={() => void runPreview()}
                                                        previewLoading={previewLoading}
                                                        height="220px"
                                                        showCopy={false}
                                                    />
                                                </div>
                                                <label className="form-block">
                                                    <span>参数 JSON</span>
                                                    <textarea
                                                        className="modern-textarea-pro dictionary-json-editor"
                                                        value={sqlParamsText}
                                                        onChange={(e) => setSqlParamsText(e.target.value)}
                                                        placeholder='例如 { "status": "active" }'
                                                    />
                                                </label>
                                                {mappingSelector}
                                            </div>
                                        </div>
                                    ) : null}

                                    <div className={`formula-side-card ${validationMessages.length ? 'dictionary-validation-card invalid' : 'dictionary-validation-card valid'}`}>
                                        <div className="formula-side-header">
                                            <strong>配置检查</strong>
                                            {validationMessages.length ? <AlertCircle size={16} /> : <CheckCircle2 size={16} />}
                                        </div>
                                        {validationMessages.length ? (
                                            <ul className="formula-message-list">
                                                {validationMessages.map((message) => (
                                                    <li key={message}>{message}</li>
                                                ))}
                                            </ul>
                                        ) : (
                                            <div className="empty-box">当前字典配置已经完整，可以直接保存。</div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            , document.body) : null}

            {previewModalOpen && typeof document !== 'undefined' ? createPortal(
                <div className="modal-overlay">
                    <div className="modal-content-pro dictionary-preview-modal">
                        <header className="modal-header-clean dictionary-modal-header-sticky">
                            <div>
                                <h3 className="font-bold text-slate-900">字典预览</h3>
                                <p className="dictionary-modal-subtitle">查看当前配置生成的字典结果、返回字段和原始行数据。</p>
                            </div>
                            <div className="dictionary-header-actions">
                                <div className="dictionary-preview-toolbar">
                                    <label className="form-block">
                                        <span>预览条数</span>
                                        <Select
                                            value={previewLimit}
                                            onChange={(v) => setPreviewLimit(v)}
                                            options={[
                                                { value: '20', label: '20' },
                                                { value: '50', label: '50' },
                                                { value: '100', label: '100' },
                                                { value: '200', label: '200' },
                                            ]}
                                        />
                                    </label>
                                </div>
                                <button className="btn-outline" onClick={() => void runPreview()} disabled={previewLoading || !canPreviewCurrentDictionary}>
                                    <Eye size={14} />
                                    {previewLoading ? '预览中...' : '刷新预览'}
                                </button>
                                <button onClick={() => setPreviewModalOpen(false)} className="text-slate-400 hover:text-slate-600">
                                    <X size={20} />
                                </button>
                            </div>
                        </header>
                        <div className="dictionary-preview-modal-body">
                            <div className="formula-side-card">
                                <div className="formula-side-header">
                                    <strong>预览字典结果</strong>
                                    <div className="dictionary-preview-meta">
                                        <span className="formula-status-badge type">{visibleSourceLabel}</span>
                                        <span className="formula-status-badge muted">
                                            <Eye size={12} />
                                            {previewItems.length} 项
                                        </span>
                                    </div>
                                </div>
                                <div className="dictionary-preview-list">
                                    {previewItems.length ? (
                                        previewItems.map((item, index) => (
                                            <div key={`preview-${index}`} className="dictionary-preview-item">
                                                <div className="dictionary-preview-key">{item.key}</div>
                                                <div className="dictionary-preview-label">{item.label}</div>
                                                {item.value ? <div className="dictionary-preview-value">{item.value}</div> : null}
                                            </div>
                                        ))
                                    ) : (
                                        <div className="empty-box">当前还没有预览结果，请先运行预览。</div>
                                    )}
                                </div>
                            </div>
                            <div className="formula-side-card">
                                <div className="formula-side-header">
                                    <strong>返回字段</strong>
                                    <span className="formula-status-badge muted">{previewColumns.length} 列</span>
                                </div>
                                {previewColumns.length ? (
                                    <div className="dictionary-column-tags">
                                        {previewColumns.map((column) => (
                                            <span key={column.name} className="formula-dep-tag">
                                                {column.name}
                                                {column.type ? ` : ${column.type}` : ''}
                                            </span>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="empty-box">预览后会自动识别可映射字段。</div>
                                )}
                            </div>
                            <div className="formula-side-card">
                                <div className="formula-side-header">
                                    <strong>原始数据预览</strong>
                                    <span className="formula-status-badge muted">{previewRows.length} 行</span>
                                </div>
                                {previewRows.length ? (
                                    <div className="dictionary-raw-preview">
                                        {previewRows.slice(0, 10).map((row, index) => (
                                            <pre key={`raw-${index}`}>{JSON.stringify(row, null, 2)}</pre>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="empty-box">这里会展示预览返回的原始行数据，方便确认映射是否正确。</div>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            , document.body) : null}

        </>
    );
}
