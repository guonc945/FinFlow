import { useEffect, useState, type Dispatch, type ReactNode, type SetStateAction } from 'react';
import { ChevronDown, Database, Eye, Plus, Save, SearchCheck, Trash2 } from 'lucide-react';
import Select from '../../../components/common/Select';
import DataTable from '../../../components/data/DataTable';
import JsonEditor from '../../../components/data/JsonEditor';
import SqlEditor from '../../../components/data/SqlEditor';
import { DATASET_PARAM_EXAMPLE } from '../config';
import FormModal from './FormModal';
import type {
    Connection,
    DatasetFormState,
    DatasetValidation,
    QueryResult,
} from '../types';

type DatasetModalProps = {
    open: boolean;
    editingDatasetId: number | null;
    datasetForm: DatasetFormState;
    setDatasetForm: Dispatch<SetStateAction<DatasetFormState>>;
    connections: Connection[];
    selectedConnectionForDataset?: Connection;
    datasetValidation: DatasetValidation | null;
    datasetValidationLoading: boolean;
    datasetResult: QueryResult | null;
    datasetPreviewLoading: boolean;
    datasetPreviewError: string | null;
    dynamicColumns: DatasetPreviewColumn[];
    onClose: () => void;
    onReset: () => void;
    onValidate: () => void | Promise<void>;
    onPreview: () => void | Promise<void>;
    onSave: () => void | Promise<void>;
};

type ParamRow = {
    id: string;
    key: string;
    value: string;
    paramType: 'text' | 'number' | 'boolean' | 'date' | 'json';
    options: string;
};

type ConfigTab = 'basic' | 'params';

type DatasetPreviewColumn = {
    key: string;
    title: ReactNode;
    render?: (value: unknown, record: Record<string, unknown>, index: number) => ReactNode;
    width?: number | string;
    fixed?: 'left' | 'right';
    className?: string;
    sortable?: boolean;
    hideable?: boolean;
    reorderable?: boolean;
    displayLabel?: string;
};

const createParamRow = (key = '', value = '', paramType: ParamRow['paramType'] = 'text', options = ''): ParamRow => ({
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    key,
    value,
    paramType,
    options,
});

const formatParamValue = (value: unknown) => {
    if (typeof value === 'string') return value;
    if (value === undefined) return '';
    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
};

const parseParamRows = (jsonText?: string | null) => {
    if (!jsonText?.trim()) {
        return { rows: [createParamRow()], error: '' };
    }
    try {
        const parsed = JSON.parse(jsonText);
        if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
            return { rows: [createParamRow()], error: '参数 JSON 需要是对象结构，例如 {"tenant_id": 1}。' };
        }
        const meta = parsed.__meta__ && typeof parsed.__meta__ === 'object' && !Array.isArray(parsed.__meta__)
            ? parsed.__meta__ as Record<string, { type?: ParamRow['paramType']; options?: string[] }>
            : {};
        const entries = Object.entries(parsed);
        return {
            rows: entries.length
                ? entries
                    .filter(([key]) => key !== '__meta__')
                    .map(([key, value]) =>
                    createParamRow(
                        key,
                        formatParamValue(value),
                        meta[key]?.type ||
                            (typeof value === 'number'
                                ? 'number'
                                : typeof value === 'boolean'
                                  ? 'boolean'
                                  : typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value)
                                    ? 'date'
                                    : value && typeof value === 'object'
                                      ? 'json'
                                      : 'text'),
                        Array.isArray(meta[key]?.options) ? meta[key].options.join(', ') : ''
                    )
                )
                : [createParamRow()],
            error: '',
        };
    } catch {
        return { rows: [createParamRow()], error: '高级 JSON 当前不是合法的对象格式，常用参数表单已暂停同步。' };
    }
};

const parseParamValue = (row: ParamRow) => {
    const value = row.value;
    const trimmed = value.trim();
    if (!trimmed) return '';
    if (row.paramType === 'number') {
        return Number(trimmed);
    }
    if (row.paramType === 'boolean') {
        return trimmed === 'true';
    }
    if (row.paramType === 'date') {
        return trimmed;
    }
    if (row.paramType === 'json') {
        return JSON.parse(trimmed);
    }
    const shouldTryJson =
        trimmed.startsWith('{') ||
        trimmed.startsWith('[') ||
        trimmed === 'true' ||
        trimmed === 'false' ||
        trimmed === 'null' ||
        /^-?\d+(\.\d+)?$/.test(trimmed) ||
        ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'")));

    if (!shouldTryJson) {
        return value;
    }

    try {
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
            return trimmed.slice(1, -1);
        }
        return JSON.parse(trimmed);
    } catch {
        return value;
    }
};

const getOptionItems = (value: string) =>
    value
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);

export default function DatasetModal({
    open,
    editingDatasetId,
    datasetForm,
    setDatasetForm,
    connections,
    selectedConnectionForDataset,
    datasetValidation,
    datasetValidationLoading,
    datasetResult,
    datasetPreviewLoading,
    datasetPreviewError,
    dynamicColumns,
    onClose,
    onReset,
    onValidate,
    onPreview,
    onSave,
}: DatasetModalProps) {
    const connectionLabel = selectedConnectionForDataset
        ? `${selectedConnectionForDataset.name} / ${selectedConnectionForDataset.db_type}`
        : '尚未选择外部连接';
    const [paramRows, setParamRows] = useState<ParamRow[]>([createParamRow()]);
    const [advancedOpen, setAdvancedOpen] = useState(false);
    const [paramJsonError, setParamJsonError] = useState('');
    const [activeTab, setActiveTab] = useState<ConfigTab>('basic');

    useEffect(() => {
        const { rows, error } = parseParamRows(datasetForm.params_json);
        setParamRows(rows);
        setParamJsonError(error);
    }, [datasetForm.params_json]);

    const syncRowsToJson = (nextRows: ParamRow[]) => {
        setParamRows(nextRows);
        const payload = nextRows.reduce<Record<string, unknown>>((acc, row) => {
            const key = row.key.trim();
            if (!key) return acc;
            try {
                acc[key] = parseParamValue(row);
            } catch {
                acc[key] = row.value;
            }
            return acc;
        }, {});
        const metaPayload = nextRows.reduce<Record<string, { type: ParamRow['paramType']; options?: string[] }>>((acc, row) => {
            const key = row.key.trim();
            if (!key) return acc;
            const options = getOptionItems(row.options);
            acc[key] = {
                type: row.paramType,
                ...(options.length ? { options } : {}),
            };
            return acc;
        }, {});
        setParamJsonError('');
        setDatasetForm((prev) => ({
            ...prev,
            params_json: JSON.stringify(
                Object.keys(metaPayload).length ? { ...payload, __meta__: metaPayload } : payload,
                null,
                2
            ),
        }));
    };

    const handleJsonEditorChange = (value: string) => {
        setDatasetForm((prev) => ({ ...prev, params_json: value }));
        const { rows, error } = parseParamRows(value);
        setParamRows(rows);
        setParamJsonError(error);
    };

    return (
        <FormModal
            open={open}
            title={editingDatasetId ? '编辑数据集模型' : '新建数据集模型'}
            subtitle="在一个窗口里完成连接绑定、SQL 建模、参数定义、只读校验和结果预览。"
            width="1400px"
            onClose={onClose}
            closeOnBackdrop={false}
        >
            <div className="dataset-modal-shell">
                {/* 顶部操作栏 */}
                <div className="dataset-modal-header-actions">
                    <div className="dataset-modal-header-left">
                        <div className="dataset-modal-title">
                            <Database size={18} />
                            <span>{editingDatasetId ? '编辑数据集模型' : '新建数据集模型'}</span>
                        </div>
                        <span className="connection-mode-pill">{connectionLabel}</span>
                    </div>
                    <div className="dataset-modal-header-right">
                        <label className="connection-switch-chip">
                            <input
                                type="checkbox"
                                checked={datasetForm.is_active}
                                onChange={(e) => setDatasetForm((prev) => ({ ...prev, is_active: e.target.checked }))}
                            />
                            <span>启用数据集</span>
                        </label>
                        <button className="btn-outline" type="button" onClick={onReset}>重置</button>
                        <button className="btn-outline" type="button" onClick={() => void onValidate()}>
                            <SearchCheck size={14} />
                            校验 SQL
                        </button>
                        <button className="btn-outline" type="button" onClick={() => void onPreview()}>
                            <Eye size={14} />
                            刷新预览
                        </button>
                        <button className="btn-primary" type="button" onClick={() => void onSave()}>
                            <Save size={14} />
                            保存
                        </button>
                    </div>
                </div>

                {/* 主体内容 - 上下布局 */}
                <div className="dataset-modal-body">
                    {/* 上半部分：左侧属性配置 + 右侧 SQL 与校验 */}
                    <div className="dataset-modal-main">
                        {/* 左侧：属性配置面板（TAB 组织） */}
                        <div className="dataset-modal-config-panel">
                            <div className="dataset-modal-tabs">
                                <button
                                    className={`dataset-modal-tab ${activeTab === 'basic' ? 'active' : ''}`}
                                    type="button"
                                    onClick={() => setActiveTab('basic')}
                                >
                                    基础信息
                                </button>
                                <button
                                    className={`dataset-modal-tab ${activeTab === 'params' ? 'active' : ''}`}
                                    type="button"
                                    onClick={() => setActiveTab('params')}
                                >
                                    参数模板
                                </button>
                            </div>

                            <div className="dataset-modal-tab-content">
                                {activeTab === 'basic' && (
                                    <div className="dataset-form-section">
                                        <div className="form-grid two">
                                            <label>
                                                <span>外部连接</span>
                                                <Select
                                                    value={datasetForm.connection_id}
                                                    onChange={(v) => setDatasetForm((prev) => ({ ...prev, connection_id: v }))}
                                                    options={[
                                                        { value: '', label: '请选择' },
                                                        ...connections.map((item) => ({ value: item.id, label: item.name })),
                                                    ]}
                                                />
                                            </label>
                                            <label>
                                                <span>预览行数</span>
                                                <input value={datasetForm.row_limit} onChange={(e) => setDatasetForm((prev) => ({ ...prev, row_limit: e.target.value }))} />
                                            </label>
                                        </div>
                                        <div className="form-grid two">
                                            <label><span>数据集名称</span><input value={datasetForm.name} onChange={(e) => setDatasetForm((prev) => ({ ...prev, name: e.target.value }))} /></label>
                                            <label><span>模型说明</span><input value={datasetForm.description} onChange={(e) => setDatasetForm((prev) => ({ ...prev, description: e.target.value }))} /></label>
                                        </div>
                                    </div>
                                )}

                                {activeTab === 'params' && (
                                    <div className="dataset-form-section params-tab-section">
                                        <div className="dataset-param-builder">
                                            <div className="field-head">
                                                <span>常用参数</span>
                                                <div className="reporting-inline-tools">
                                                    <button
                                                        className="inline-link-btn"
                                                        type="button"
                                                        onClick={() => syncRowsToJson([...paramRows, createParamRow()])}
                                                    >
                                                        <Plus size={14} />
                                                        新增参数
                                                    </button>
                                                    <button
                                                        className="inline-link-btn"
                                                        type="button"
                                                        onClick={() => {
                                                            const { rows } = parseParamRows(DATASET_PARAM_EXAMPLE);
                                                            setParamRows(rows);
                                                            setParamJsonError('');
                                                            setDatasetForm((prev) => ({ ...prev, params_json: DATASET_PARAM_EXAMPLE }));
                                                        }}
                                                    >
                                                        填充示例
                                                    </button>
                                                </div>
                                            </div>
                                            <div className="dataset-param-grid">
                                                {paramRows.map((row) => (
                                                    <div key={row.id} className="dataset-param-row">
                                                        <input
                                                            value={row.key}
                                                            placeholder="参数名，例如 tenant_id"
                                                            onChange={(e) =>
                                                                syncRowsToJson(
                                                                    paramRows.map((item) =>
                                                                        item.id === row.id ? { ...item, key: e.target.value } : item
                                                                    )
                                                                )
                                                            }
                                                        />
                                                        <select
                                                            value={row.paramType}
                                                            onChange={(e) =>
                                                                syncRowsToJson(
                                                                    paramRows.map((item) =>
                                                                        item.id === row.id
                                                                            ? { ...item, paramType: e.target.value as ParamRow['paramType'] }
                                                                            : item
                                                                    )
                                                                )
                                                            }
                                                        >
                                                            <option value="text">文本</option>
                                                            <option value="number">数字</option>
                                                            <option value="boolean">布尔</option>
                                                            <option value="date">日期</option>
                                                            <option value="json">JSON</option>
                                                        </select>
                                                        {row.paramType === 'boolean' ? (
                                                            <select
                                                                value={row.value}
                                                                onChange={(e) =>
                                                                    syncRowsToJson(
                                                                        paramRows.map((item) =>
                                                                            item.id === row.id ? { ...item, value: e.target.value } : item
                                                                        )
                                                                    )
                                                                }
                                                            >
                                                                <option value="">请选择</option>
                                                                <option value="true">true</option>
                                                                <option value="false">false</option>
                                                            </select>
                                                        ) : row.paramType === 'date' ? (
                                                            <input
                                                                type="date"
                                                                value={row.value}
                                                                onChange={(e) =>
                                                                    syncRowsToJson(
                                                                        paramRows.map((item) =>
                                                                            item.id === row.id ? { ...item, value: e.target.value } : item
                                                                        )
                                                                    )
                                                                }
                                                            />
                                                        ) : row.paramType === 'number' ? (
                                                            <input
                                                                type="number"
                                                                value={row.value}
                                                                placeholder="默认值，例如 1"
                                                                onChange={(e) =>
                                                                    syncRowsToJson(
                                                                        paramRows.map((item) =>
                                                                            item.id === row.id ? { ...item, value: e.target.value } : item
                                                                        )
                                                                    )
                                                                }
                                                            />
                                                        ) : row.paramType !== 'json' && getOptionItems(row.options).length ? (
                                                            <select
                                                                value={row.value}
                                                                onChange={(e) =>
                                                                    syncRowsToJson(
                                                                        paramRows.map((item) =>
                                                                            item.id === row.id ? { ...item, value: e.target.value } : item
                                                                        )
                                                                    )
                                                                }
                                                            >
                                                                <option value="">请选择默认值</option>
                                                                {getOptionItems(row.options).map((option) => (
                                                                    <option key={option} value={option}>{option}</option>
                                                                ))}
                                                            </select>
                                                        ) : (
                                                            <input
                                                                value={row.value}
                                                                placeholder={
                                                                    row.paramType === 'json'
                                                                        ? '{"code":"A01"}'
                                                                        : '默认值，例如 tenant_01'
                                                                }
                                                                onChange={(e) =>
                                                                    syncRowsToJson(
                                                                        paramRows.map((item) =>
                                                                            item.id === row.id ? { ...item, value: e.target.value } : item
                                                                        )
                                                                    )
                                                                }
                                                            />
                                                        )}
                                                        {(row.paramType === 'text' || row.paramType === 'date') ? (
                                                            <input
                                                                value={row.options}
                                                                className="dataset-param-options"
                                                                placeholder="可选项，逗号分隔，可留空"
                                                                onChange={(e) =>
                                                                    setParamRows((prev) =>
                                                                        prev.map((item) =>
                                                                            item.id === row.id ? { ...item, options: e.target.value } : item
                                                                        )
                                                                    )
                                                                }
                                                            />
                                                        ) : (
                                                            <div className="dataset-param-options-placeholder">
                                                                {row.paramType === 'boolean' ? '固定 true / false' : '结构化值'}
                                                            </div>
                                                        )}
                                                        <button
                                                            className="dataset-param-remove"
                                                            type="button"
                                                            onClick={() => {
                                                                const nextRows = paramRows.filter((item) => item.id !== row.id);
                                                                syncRowsToJson(nextRows.length ? nextRows : [createParamRow()]);
                                                            }}
                                                        >
                                                            <Trash2 size={14} />
                                                        </button>
                                                    </div>
                                                ))}
                                            </div>
                                            <div className="editor-help-text">
                                                值支持普通文本，也支持数字、布尔、数组、对象等 JSON 字面量。
                                            </div>
                                        </div>
                                        <div className="form-block reporting-editor-field">
                                            <button
                                                className={`connection-advanced-toggle ${advancedOpen ? 'open' : ''}`}
                                                type="button"
                                                onClick={() => setAdvancedOpen((prev) => !prev)}
                                            >
                                                <span>高级 JSON</span>
                                                <ChevronDown size={16} className={advancedOpen ? 'chevron-open' : ''} />
                                            </button>
                                            {advancedOpen ? (
                                                <>
                                                    <JsonEditor
                                                        value={datasetForm.params_json}
                                                        onChange={handleJsonEditorChange}
                                                        height="140px"
                                                        placeholder={DATASET_PARAM_EXAMPLE}
                                                    />
                                                    {paramJsonError ? (
                                                        <div className="preview-status warning">{paramJsonError}</div>
                                                    ) : (
                                                        <div className="editor-help-text">修改高级 JSON 后，常用参数表单会自动同步。</div>
                                                    )}
                                                </>
                                            ) : null}
                                        </div>

                                        {/* 参数使用说明整合到参数模板 TAB 中 */}
                                        <div className="param-guide-card compact">
                                            <div className="param-guide-title">参数使用说明</div>
                                            <div className="param-guide-list">
                                                <div>1. SQL 里先写命名参数，例如 <code>WHERE tenant_id = :tenant_id</code></div>
                                                <div>2. 参数 JSON 的键名必须与 SQL 里的命名参数一致</div>
                                                <div>3. 预览和校验都会优先使用这份 JSON 的默认值</div>
                                                <div>4. 系统全局变量仍然会先解析，再和参数 JSON 一起执行</div>
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* 右侧：SQL 建模 + 结构校验 */}
                        <div className="dataset-modal-sql-validation">
                            {/* SQL 建模 */}
                            <div className="dataset-form-card">
                                <div className="dataset-card-header">
                                    <div className="dataset-card-title">SQL 建模</div>
                                    <div className="dataset-card-subtitle">支持命名参数 <code>:param</code> 与系统全局变量</div>
                                </div>
                                <div className="dataset-card-content">
                                    <SqlEditor
                                        value={datasetForm.sql_text}
                                        onChange={(value) => setDatasetForm((prev) => ({ ...prev, sql_text: value }))}
                                        dialect={selectedConnectionForDataset?.db_type}
                                        onPreview={() => void onPreview()}
                                        previewLoading={datasetPreviewLoading}
                                        height="220px"
                                    />
                                    <div className="editor-help-text">
                                        建议先写明确的字段列表和过滤条件，避免 <code>SELECT *</code> 直接进入生产应用。
                                    </div>
                                </div>
                            </div>

                            {/* 结构校验 */}
                            <div className="dataset-form-card dataset-validation-card">
                                <div className="dataset-card-header">
                                    <div className="dataset-card-title">结构校验</div>
                                    <div className="dataset-card-subtitle">分析命名参数、返回列和只读查询安全性</div>
                                </div>
                                <div className="dataset-card-content">
                                    {datasetValidationLoading ? (
                                        <div className="preview-status loading">正在校验 SQL、安全性和参数定义...</div>
                                    ) : datasetValidation ? (
                                        <>
                                            <div className="validation-grid compact">
                                                <div className="meta-tile"><span>命名参数</span><strong>{datasetValidation.extracted_params.length}</strong></div>
                                                <div className="meta-tile"><span>返回列</span><strong>{datasetValidation.columns.length}</strong></div>
                                                <div className="meta-tile"><span>预览行数</span><strong>{datasetValidation.preview_row_count}</strong></div>
                                                <div className="meta-tile"><span>执行上限</span><strong>{datasetValidation.limit}</strong></div>
                                            </div>
                                            <div className="validation-block">
                                                <div className="validation-label">提取到的 SQL 参数</div>
                                                <div className="schema-columns">
                                                    {datasetValidation.extracted_params.map((item) => (
                                                        <span key={item} className="schema-chip">{item}</span>
                                                    ))}
                                                    {!datasetValidation.extracted_params.length && <span className="resource-meta">未检测到命名参数。</span>}
                                                </div>
                                            </div>
                                            <div className="validation-block">
                                                <div className="validation-label">输出列结构</div>
                                                <div className="schema-columns">
                                                    {datasetValidation.columns.map((column) => (
                                                        <span key={column.name} className="schema-chip">
                                                            {column.name}: {column.type}
                                                        </span>
                                                    ))}
                                                </div>
                                            </div>
                                            {datasetValidation.warnings.length ? (
                                                <div className="preview-status warning">
                                                    {datasetValidation.warnings.join('；')}
                                                </div>
                                            ) : (
                                                <div className="preview-status success">校验通过，当前 SQL 可作为只读数据集模型保存。</div>
                                            )}
                                        </>
                                    ) : (
                                        <div className="empty-box">校验会自动解析参数、输出列和默认值覆盖情况。</div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* 下半部分：预览结果 */}
                    <div className="dataset-modal-preview-section">
                        <div className="dataset-preview-header">
                            <div className="dataset-preview-title">
                                <Eye size={16} />
                                <span>预览结果</span>
                            </div>
                            <div className="dataset-preview-subtitle">
                                {datasetPreviewLoading
                                    ? '正在执行只读预览'
                                    : datasetResult
                                      ? `返回 ${datasetResult.row_count} 行 / 上限 ${datasetResult.limit}`
                                      : '点击"刷新预览"后在这里查看结果'}
                            </div>
                        </div>
                        <div className="dataset-preview-content">
                            {datasetPreviewError ? (
                                <div className="preview-status error">{datasetPreviewError}</div>
                            ) : datasetPreviewLoading ? (
                                <div className="preview-status loading">正在执行只读预览查询，请稍候...</div>
                            ) : datasetResult ? (
                                <DataTable columns={dynamicColumns} data={datasetResult.rows} tableId="reports-dataset-preview-table" />
                            ) : (
                                <div className="empty-box">选择连接并编写 SQL 后，这里会自动显示预览结果。</div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </FormModal>
    );
}
