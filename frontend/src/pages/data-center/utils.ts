import type {
    ColumnAggregateMethod,
    ColumnStyleBackgroundMode,
    ReportAggregateSummary,
    ReportColumnStyleRule,
    DataDictionaryItem,
    ColumnDisplayType,
    Dataset,
    QueryColumn,
    ReportChartConfig,
    ReportColumnConfig,
    ReportFilter,
} from './types';

export const parseJson = <T,>(raw: string | null | undefined, fallback: T): T => {
    if (!raw) return fallback;
    try {
        return JSON.parse(raw) as T;
    } catch {
        return fallback;
    }
};

export const formatCell = (value: unknown) => {
    if (value === null || value === undefined || value === '') return '-';
    return typeof value === 'object' ? JSON.stringify(value) : String(value);
};

const NUMERIC_TYPES = ['int', 'integer', 'bigint', 'smallint', 'decimal', 'numeric', 'float', 'double', 'real', 'number'];
const DATE_TYPES = ['date'];
const DATETIME_TYPES = ['time', 'datetime', 'timestamp'];
const BOOLEAN_TYPES = ['bool', 'boolean'];

export const inferColumnDisplayType = (
    configuredType: ColumnDisplayType | undefined,
    columnType?: string,
    sample?: unknown
): ColumnDisplayType => {
    if (configuredType && configuredType !== 'auto') return configuredType;

    const normalizedColumnType = String(columnType || '').trim().toLowerCase();
    if (BOOLEAN_TYPES.some((type) => normalizedColumnType.includes(type))) return 'boolean';
    if (DATE_TYPES.some((type) => normalizedColumnType.includes(type))) return 'date';
    if (DATETIME_TYPES.some((type) => normalizedColumnType.includes(type))) return 'datetime';
    if (NUMERIC_TYPES.some((type) => normalizedColumnType.includes(type))) return 'number';

    if (typeof sample === 'boolean') return 'boolean';
    if (typeof sample === 'number') return 'number';
    if (sample instanceof Date) return 'datetime';

    return 'text';
};

const parseDateValue = (value: unknown) => {
    if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
    if (typeof value === 'string' || typeof value === 'number') {
        const parsed = new Date(value);
        if (!Number.isNaN(parsed.getTime())) return parsed;
    }
    return null;
};

const parseNumericValue = (value: unknown) => {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim()) {
        const normalized = Number(value);
        if (Number.isFinite(normalized)) return normalized;
    }
    return null;
};

export const formatValueByColumn = (
    value: unknown,
    options?: {
        configuredType?: ColumnDisplayType;
        columnType?: string;
        sample?: unknown;
    }
) => {
    if (value === null || value === undefined || value === '') return '-';

    const resolvedType = inferColumnDisplayType(options?.configuredType, options?.columnType, options?.sample ?? value);

    if (resolvedType === 'boolean') {
        if (typeof value === 'boolean') return value ? '是' : '否';
        if (typeof value === 'number') return value === 0 ? '否' : '是';
        const normalized = String(value).trim().toLowerCase();
        if (['true', '1', 'yes', 'y'].includes(normalized)) return '是';
        if (['false', '0', 'no', 'n'].includes(normalized)) return '否';
        return formatCell(value);
    }

    if (resolvedType === 'currency') {
        const numeric = parseNumericValue(value);
        return numeric === null
            ? formatCell(value)
            : new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(numeric);
    }

    if (resolvedType === 'percent') {
        const numeric = parseNumericValue(value);
        return numeric === null
            ? formatCell(value)
            : `${new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(numeric)}%`;
    }

    if (resolvedType === 'number') {
        const numeric = parseNumericValue(value);
        return numeric === null ? formatCell(value) : new Intl.NumberFormat('zh-CN').format(numeric);
    }

    if (resolvedType === 'date') {
        const parsed = parseDateValue(value);
        return parsed === null ? formatCell(value) : new Intl.DateTimeFormat('zh-CN', { dateStyle: 'medium' }).format(parsed);
    }

    if (resolvedType === 'datetime') {
        const parsed = parseDateValue(value);
        return parsed === null
            ? formatCell(value)
            : new Intl.DateTimeFormat('zh-CN', {
                  dateStyle: 'medium',
                  timeStyle: 'short',
              }).format(parsed);
    }

    return formatCell(value);
};

export const createEmptyFilter = (): ReportFilter => ({
    key: '',
    label: '',
    type: 'text',
    placeholder: '',
    default_value: '',
    options: [],
    dictionary_id: null,
    width: '',
});

export const createReportColumnConfig = (
    column: Pick<QueryColumn, 'name' | 'type'>,
    overrides?: Partial<ReportColumnConfig>
): ReportColumnConfig => ({
    key: column.name,
    label: overrides?.label || column.name,
    visible: overrides?.visible ?? true,
    type: overrides?.type || 'auto',
    width: overrides?.width || '',
    description: overrides?.description || '',
    pinned: overrides?.pinned || 'none',
    dictionary_id: typeof overrides?.dictionary_id === 'number' ? overrides.dictionary_id : null,
    dictionary_display: 'label',
    aggregate: overrides?.aggregate || 'none',
    style_rules: Array.isArray(overrides?.style_rules) ? overrides.style_rules : [],
    group: overrides?.group,
    group_order: typeof overrides?.group_order === 'number' ? overrides.group_order : undefined,
    parent_group: overrides?.parent_group || undefined,
});

export const normalizeReportColumnConfig = (config?: Partial<ReportColumnConfig>): ReportColumnConfig => ({
    key: config?.key || '',
    label: config?.label || config?.key || '',
    visible: config?.visible ?? true,
    type: config?.type || 'auto',
    width: config?.width || '',
    description: config?.description || '',
    pinned: config?.pinned === 'left' ? 'left' : 'none',
    dictionary_id: typeof config?.dictionary_id === 'number' ? config.dictionary_id : null,
    dictionary_display: 'label',
    aggregate: config?.aggregate || 'none',
    style_rules: Array.isArray(config?.style_rules) ? config.style_rules : [],
    group: config?.group,
    group_order: typeof config?.group_order === 'number' ? config.group_order : undefined,
    parent_group: config?.parent_group || undefined,
});

export const sanitizeReportColumns = (columns: ReportColumnConfig[]): ReportColumnConfig[] =>
    columns
        .map((column) => {
            const normalized = normalizeReportColumnConfig(column);
            return {
                key: normalized.key.trim(),
                label: (normalized.label || normalized.key).trim(),
                visible: normalized.visible,
                type: normalized.type || 'auto',
                width: normalized.width?.trim() || '',
                description: normalized.description?.trim() || '',
                pinned: normalized.pinned === 'left' ? 'left' : 'none',
                dictionary_id: typeof normalized.dictionary_id === 'number' ? normalized.dictionary_id : null,
                dictionary_display: 'label',
                aggregate: normalized.aggregate || 'none',
                style_rules: (normalized.style_rules || [])
                    .map((rule, index) => ({
                        id: rule.id || `rule-${index + 1}`,
                        compare_field: rule.compare_field?.trim() || '',
                        operator: rule.operator || 'eq',
                        value: rule.value?.trim() || '',
                        second_value: rule.second_value?.trim() || '',
                        text_color: rule.text_color?.trim() || '',
                        background_color: rule.background_color?.trim() || '',
                        background_mode: rule.background_mode || 'soft',
                    }))
                    .filter((rule) => rule.operator === 'empty' || rule.operator === 'not_empty' || rule.value || rule.text_color || rule.background_color),
                group: normalized.group,
                group_order: typeof normalized.group_order === 'number' ? normalized.group_order : undefined,
                parent_group: normalized.parent_group?.trim() || undefined,
            } satisfies ReportColumnConfig;
        })
        .filter((column) => column.key);

export const formatValueWithDictionary = (
    value: unknown,
    options?: {
        dictionaryItems?: DataDictionaryItem[];
        dictionaryDisplay?: 'label' | 'value';
    }
) => {
    if (value === null || value === undefined || value === '') return '-';

    const dictionaryItems = options?.dictionaryItems || [];
    if (!dictionaryItems.length) return formatCell(value);

    const normalizedKey = String(value);
    const matched = dictionaryItems.find((item) => item.key === normalizedKey);
    if (!matched) return formatCell(value);

    if (options?.dictionaryDisplay === 'value' && matched.value !== null && matched.value !== undefined && matched.value !== '') {
        return String(matched.value);
    }
    return matched.path || matched.label || formatCell(value);
};

export const formatValueByColumnConfig = (
    value: unknown,
    options?: {
        configuredType?: ColumnDisplayType;
        columnType?: string;
        sample?: unknown;
        dictionaryItems?: DataDictionaryItem[];
        dictionaryDisplay?: 'label' | 'value';
    }
) => {
    const dictionaryText = formatValueWithDictionary(value, {
        dictionaryItems: options?.dictionaryItems,
        dictionaryDisplay: options?.dictionaryDisplay,
    });
    if ((options?.dictionaryItems || []).length > 0 && dictionaryText !== formatCell(value)) {
        return dictionaryText;
    }
    return formatValueByColumn(value, {
        configuredType: options?.configuredType,
        columnType: options?.columnType,
        sample: options?.sample,
    });
};

const normalizeComparableNumber = (value: unknown) => {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim()) {
        const parsed = Number(value);
        if (Number.isFinite(parsed)) return parsed;
    }
    return null;
};

const normalizeComparableText = (value: unknown) => String(value ?? '').trim().toLowerCase();

export const evaluateStyleRule = (
    value: unknown,
    rule: ReportColumnStyleRule,
    row?: Record<string, unknown>
) => {
    const compareValue =
        rule.compare_field && row && Object.prototype.hasOwnProperty.call(row, rule.compare_field)
            ? row[rule.compare_field]
            : value;
    const operator = rule.operator || 'eq';
    const rawText = String(compareValue ?? '').trim();
    const normalizedText = normalizeComparableText(compareValue);
    const compareText = String(rule.value || '').trim().toLowerCase();
    const numericValue = normalizeComparableNumber(compareValue);
    const compareNumber = normalizeComparableNumber(rule.value);
    const secondCompareNumber = normalizeComparableNumber(rule.second_value);

    if (operator === 'empty') return rawText === '';
    if (operator === 'not_empty') return rawText !== '';
    if (operator === 'contains') return compareText ? normalizedText.includes(compareText) : false;
    if (operator === 'eq') return normalizedText === compareText;
    if (operator === 'ne') return normalizedText !== compareText;
    if (numericValue === null || compareNumber === null) return false;
    if (operator === 'gt') return numericValue > compareNumber;
    if (operator === 'gte') return numericValue >= compareNumber;
    if (operator === 'lt') return numericValue < compareNumber;
    if (operator === 'lte') return numericValue <= compareNumber;
    if (operator === 'between') {
        if (secondCompareNumber === null) return false;
        const min = Math.min(compareNumber, secondCompareNumber);
        const max = Math.max(compareNumber, secondCompareNumber);
        return numericValue >= min && numericValue <= max;
    }
    return false;
};

const resolveBackgroundStyle = (backgroundMode: ColumnStyleBackgroundMode, backgroundColor: string) => {
    if (!backgroundColor) return {};
    if (backgroundMode === 'solid') {
        return { backgroundColor, borderRadius: '8px', padding: '0.12rem 0.35rem' };
    }
    if (backgroundMode === 'pill') {
        return { backgroundColor, borderRadius: '999px', padding: '0.16rem 0.5rem' };
    }
    if (backgroundMode === 'outline') {
        return { border: `1px solid ${backgroundColor}`, borderRadius: '8px', padding: '0.12rem 0.35rem' };
    }
    return {
        background: `linear-gradient(180deg, ${backgroundColor}22, ${backgroundColor}14)`,
        borderRadius: '8px',
        padding: '0.12rem 0.35rem',
    };
};

export const getColumnStyleForValue = (
    value: unknown,
    rules?: ReportColumnStyleRule[],
    row?: Record<string, unknown>
) => {
    const matchedRule = (rules || []).find((rule) => evaluateStyleRule(value, rule, row));
    if (!matchedRule) return null;

    return {
        ...(matchedRule.text_color ? { color: matchedRule.text_color } : {}),
        ...(matchedRule.background_color ? resolveBackgroundStyle(matchedRule.background_mode || 'soft', matchedRule.background_color) : {}),
        fontWeight: matchedRule.background_mode === 'pill' || matchedRule.background_mode === 'solid' ? 700 : 600,
        display: 'inline-flex',
        alignItems: 'center',
        maxWidth: '100%',
    } as const;
};

const AGGREGATE_METHOD_LABEL: Record<ColumnAggregateMethod, string> = {
    none: '无',
    sum: '求和',
    avg: '平均',
    min: '最小值',
    max: '最大值',
    count: '计数',
    count_distinct: '去重计数',
};

export const getAggregateMethodLabel = (method: ColumnAggregateMethod) => AGGREGATE_METHOD_LABEL[method] || method;

export const computeReportAggregateSummaries = (
    rows: Record<string, unknown>[],
    columns: ReportColumnConfig[]
): ReportAggregateSummary[] => {
    return columns
        .filter((column) => column.visible && column.aggregate && column.aggregate !== 'none')
        .map((column) => {
            const values = rows.map((row) => row[column.key]);
            let aggregateValue: unknown = '-';

            if (column.aggregate === 'count') {
                aggregateValue = values.filter((value) => value !== null && value !== undefined && value !== '').length;
            } else if (column.aggregate === 'count_distinct') {
                aggregateValue = new Set(values.filter((value) => value !== null && value !== undefined && value !== '').map((value) => String(value))).size;
            } else {
                const numericValues = values
                    .map((value) => normalizeComparableNumber(value))
                    .filter((value): value is number => value !== null);

                if (numericValues.length) {
                    if (column.aggregate === 'sum') aggregateValue = numericValues.reduce((sum, value) => sum + value, 0);
                    if (column.aggregate === 'avg') aggregateValue = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
                    if (column.aggregate === 'min') aggregateValue = Math.min(...numericValues);
                    if (column.aggregate === 'max') aggregateValue = Math.max(...numericValues);
                }
            }

            return {
                key: column.key,
                label: `${column.label || column.key} · ${getAggregateMethodLabel(column.aggregate || 'none')}`,
                method: column.aggregate || 'none',
                value: formatValueByColumnConfig(aggregateValue, {
                    configuredType: column.type,
                    sample: aggregateValue,
                }),
            } satisfies ReportAggregateSummary;
        })
        .filter((item) => item.method !== 'none');
};

export const parseDatasetColumns = (dataset?: Dataset): QueryColumn[] => {
    if (!dataset?.last_columns_json) return [];
    const parsed = parseJson<unknown[]>(dataset.last_columns_json, []);
    return parsed
        .filter(
            (item): item is QueryColumn =>
                item !== null && typeof item === 'object' && 'name' in item && 'type' in item
        )
        .map((item) => ({
            name: String(item.name),
            type: String(item.type),
            sample: item.sample,
            nullable: typeof item.nullable === 'boolean' ? item.nullable : undefined,
            default: item.default == null ? null : String(item.default),
        }));
};

export const mergeReportColumnsWithDataset = (
    datasetColumns: QueryColumn[],
    existingColumns: ReportColumnConfig[] = []
): ReportColumnConfig[] => {
    const existingMap = new Map(existingColumns.map((column) => [column.key, normalizeReportColumnConfig(column)]));
    const merged = datasetColumns.map((column) => {
        const existing = existingMap.get(column.name);
        return createReportColumnConfig(column, existing || undefined);
    });

    existingColumns.forEach((column) => {
        if (!datasetColumns.some((item) => item.name === column.key)) {
            merged.push(normalizeReportColumnConfig(column));
        }
    });

    return merged;
};

export const normalizeFilter = (filter?: Partial<ReportFilter>): ReportFilter => ({
    key: filter?.key || '',
    label: filter?.label || '',
    type: filter?.type || 'text',
    placeholder: filter?.placeholder || '',
    default_value: filter?.default_value || '',
    dictionary_id: typeof filter?.dictionary_id === 'number' ? filter.dictionary_id : null,
    options: (filter?.options || []).map((option) => ({
        label: option?.label || '',
        value: option?.value || '',
    })),
    width: filter?.width || '',
});

export const sanitizeFilters = (filters: ReportFilter[]): ReportFilter[] =>
    filters
        .map((filter) => {
            const normalized = normalizeFilter(filter);
            const sanitized: ReportFilter = {
                key: normalized.key.trim(),
                label: (normalized.label || normalized.key).trim(),
                type: normalized.dictionary_id ? 'select' : normalized.type,
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
            if (typeof normalized.dictionary_id === 'number') {
                sanitized.dictionary_id = normalized.dictionary_id;
            }
            if (normalized.width?.trim()) {
                sanitized.width = normalized.width.trim();
            }

            return sanitized;
        })
        .filter((filter) => filter.key);

export const buildFilterDefaults = (filters: ReportFilter[]) => {
    const result: Record<string, string> = {};
    filters.forEach((filter) => {
        result[filter.key] = filter.default_value || '';
    });
    return result;
};

export const inferFiltersFromDataset = (dataset: Dataset | undefined): ReportFilter[] => {
    if (!dataset) return [];
    const params = parseJson<Record<string, unknown>>(dataset.params_json, {});
    const meta = params.__meta__ && typeof params.__meta__ === 'object' && !Array.isArray(params.__meta__)
        ? params.__meta__ as Record<string, { type?: string; options?: string[] }>
        : {};

    return Object.entries(params)
        .filter(([key]) => key !== '__meta__')
        .map(([key, value]) => {
            const config = meta[key] || {};
            const optionItems = Array.isArray(config.options) ? config.options.filter(Boolean) : [];
            const explicitType = config.type;
            const filterType =
                optionItems.length
                    ? 'select'
                    : explicitType === 'number'
                      ? 'number'
                      : explicitType === 'date'
                        ? 'date'
                        : explicitType === 'boolean'
                          ? 'select'
                          : typeof value === 'number'
                            ? 'number'
                            : String(key).toLowerCase().includes('date')
                              ? 'date'
                              : 'text';

            const options =
                explicitType === 'boolean'
                    ? [
                        { label: 'true', value: 'true' },
                        { label: 'false', value: 'false' },
                    ]
                    : optionItems.map((item) => ({ label: item, value: item }));

            return {
                key,
                label: key,
                type: filterType,
                default_value: value === null || value === undefined ? '' : String(value),
                ...(filterType === 'select' ? { options } : {}),
            } satisfies ReportFilter;
        });
};

export const buildCsvContent = (columns: string[], rows: Record<string, unknown>[]) => {
    const escapeCsvValue = (value: unknown) => {
        const text = formatCell(value);
        return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
    };

    return [
        columns.map((column) => escapeCsvValue(column)).join(','),
        ...rows.map((row) => columns.map((column) => escapeCsvValue(row[column])).join(',')),
    ].join('\r\n');
};

export const buildChartData = (rows: Record<string, unknown>[], chart?: ReportChartConfig) => {
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
