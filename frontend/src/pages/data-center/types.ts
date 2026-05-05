export type DataCenterTabKey = 'connections' | 'datasets' | 'reports' | 'dictionaries' | 'categories';

export type ReportCategory = {
    id: number;
    name: string;
    parent_id: number | null;
    sort_order: number;
    status: number;
    description: string | null;
    path?: string | null;
    children: ReportCategory[];
    created_at?: string;
    updated_at?: string | null;
};

export type Connection = {
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
    has_password?: boolean;
    created_at?: string;
    updated_at?: string | null;
};

export type Dataset = {
    id: number;
    connection_id: number;
    connection_name?: string | null;
    name: string;
    description?: string | null;
    sql_text: string;
    params_json?: string | null;
    row_limit: number;
    last_columns_json?: string | null;
    last_validated_at?: string | null;
    is_active: boolean;
    created_at?: string;
    updated_at?: string | null;
};

export type Report = {
    id: number;
    dataset_id: number;
    dataset_name?: string | null;
    name: string;
    description?: string | null;
    report_type: string;
    config_json?: string | null;
    category_id?: number | null;
    category_name?: string | null;
    is_active: boolean;
};

export type QueryColumn = {
    name: string;
    type: string;
    sample?: unknown;
    nullable?: boolean;
    default?: string | null;
};

export type QueryResult = {
    columns: QueryColumn[];
    rows: Record<string, unknown>[];
    numeric_summary: Record<string, number>;
    row_count: number;
    limit: number;
};

export type ConnectionMetadata = {
    connection_id: number;
    db_type: string;
    database_name: string;
    schema_name?: string | null;
    server_version?: string | null;
    current_schema?: string | null;
    available_schemas: string[];
    table_count: number;
    view_count: number;
};

export type TableEntry = {
    table_name: string;
    schema_name?: string | null;
    object_type?: string;
    columns: QueryColumn[];
};

export type DatasetValidation = {
    connection_id: number;
    sql_text: string;
    normalized_sql: string;
    extracted_params: string[];
    resolved_defaults: Record<string, unknown>;
    columns: QueryColumn[];
    preview_row_count: number;
    limit: number;
    warnings: string[];
};

export type FilterType = 'text' | 'number' | 'date' | 'select';

export type ReportFilter = {
    key: string;
    label: string;
    type: FilterType;
    placeholder?: string;
    default_value?: string;
    options?: Array<{ label: string; value: string }>;
    dictionary_id?: number | null;
    width?: string;
};

export type ColumnDisplayType = 'auto' | 'text' | 'number' | 'date' | 'datetime' | 'currency' | 'percent' | 'boolean';

export type ColumnAggregateMethod = 'none' | 'sum' | 'avg' | 'min' | 'max' | 'count' | 'count_distinct';

export type ColumnStyleConditionOperator =
    | 'eq'
    | 'ne'
    | 'contains'
    | 'gt'
    | 'gte'
    | 'lt'
    | 'lte'
    | 'between'
    | 'empty'
    | 'not_empty';

export type ColumnStyleBackgroundMode = 'solid' | 'soft' | 'pill' | 'outline';

export type ColumnStyleFontWeight = 'normal' | 'bold' | 'lighter' | 'bolder' | '100' | '200' | '300' | '400' | '500' | '600' | '700' | '800' | '900';

export type ColumnStyleFontStyle = 'normal' | 'italic' | 'oblique';

export type ColumnStyleTextDecoration = 'none' | 'underline' | 'line-through' | 'overline';

export type ColumnStyleTextAlign = 'left' | 'center' | 'right';

export type ReportColumnStyleRule = {
    id?: string;
    compare_field?: string;
    operator: ColumnStyleConditionOperator;
    value?: string;
    second_value?: string;
    text_color?: string;
    background_color?: string;
    background_mode?: ColumnStyleBackgroundMode;
    font_weight?: ColumnStyleFontWeight;
    font_style?: ColumnStyleFontStyle;
    font_size?: string;
    text_decoration?: ColumnStyleTextDecoration;
    text_align?: ColumnStyleTextAlign;
    border_color?: string;
    border_width?: string;
    border_radius?: string;
    opacity?: number;
    icon?: string;
};

export type ReportColumnConfig = {
    key: string;
    label: string;
    visible: boolean;
    type?: ColumnDisplayType;
    width?: string;
    description?: string;
    pinned?: 'none' | 'left';
    dictionary_id?: number | null;
    dictionary_display?: 'label' | 'value';
    aggregate?: ColumnAggregateMethod;
    style_rules?: ReportColumnStyleRule[];
    group?: string;
    group_order?: number;
    parent_group?: string;
    sort_order?: 'asc' | 'desc' | null;
};

export type ChartType = 'bar' | 'line' | 'pie';

export type TableBorderStyle = 'solid' | 'dashed' | 'dotted' | 'none';

export type TableStyleConfig = {
    border_style?: TableBorderStyle;
    border_color?: string;
    border_radius?: string;
    font_size?: string;
    row_height?: string;
    striped?: boolean;
    show_row_number?: boolean;
    header_background?: string;
    header_color?: string;
    header_font_size?: string;
    header_font_weight?: string;
    body_background?: string;
    body_color?: string;
    body_font_size?: string;
    footer_visible?: boolean;
    footer_background?: string;
    footer_color?: string;
    empty_text?: string;
    pagination_enabled?: boolean;
    page_size?: number;
    page_size_options?: number[];
};

export type ReportChartConfig = {
    enabled: boolean;
    chart_type: ChartType;
    category_field: string;
    value_field: string;
    series_field?: string;
    aggregate?: 'sum' | 'count';
};

export type ReportConfig = {
    visible_columns?: string[];
    columns?: ReportColumnConfig[];
    default_limit?: number;
    aggregate_scope?: 'returned' | 'filtered';
    filters?: ReportFilter[];
    chart?: ReportChartConfig;
    table_style?: TableStyleConfig;
};

export type DataDictionary = {
    id: number;
    key: string;
    name: string;
    source_type: 'static' | 'dataset' | 'table' | 'sql';
    description?: string | null;
    category?: string | null;
    is_active: boolean;
};

export type DataDictionaryItem = {
    key: string;
    label: string;
    value?: string | null;
    path?: string | null;
    raw?: Record<string, unknown> | null;
};

export type ReportAggregateSummary = {
    key: string;
    label: string;
    method: ColumnAggregateMethod;
    value: string;
};

export type ConnectionFormState = {
    name: string;
    db_type: string;
    host: string;
    port: string;
    database_name: string;
    schema_name: string;
    username: string;
    password: string;
    description: string;
    connection_options: string;
    is_active: boolean;
};

export type DatasetFormState = {
    connection_id: string;
    name: string;
    description: string;
    sql_text: string;
    params_json: string;
    row_limit: string;
    is_active: boolean;
};

export type ReportFormState = {
    dataset_id: string;
    name: string;
    description: string;
    report_type: string;
    category_id: string;
    visible_columns: string;
    column_configs_json: string;
    default_limit: string;
    aggregate_scope: 'returned' | 'filtered';
    filters_json: string;
    chart_enabled: boolean;
    chart_type: ChartType;
    category_field: string;
    value_field: string;
    aggregate: 'sum' | 'count';
    table_style_json: string;
    is_active: boolean;
};
