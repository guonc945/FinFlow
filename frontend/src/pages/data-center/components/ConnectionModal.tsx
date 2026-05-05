import { useEffect, useMemo, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import {
    BadgeCheck,
    ChevronDown,
    ChevronUp,
    Database,
    FolderCog,
    Network,
    Save,
    SearchCheck,
    ShieldCheck,
    SlidersHorizontal,
} from 'lucide-react';
import FormModal from './FormModal';
import type { ConnectionFormState } from '../types';

type ConnectionTestMetadata = {
    db_type?: string;
    database_name?: string;
    server_version?: string | null;
};

type ConnectionModalProps = {
    open: boolean;
    editingConnectionId: number | null;
    connectionForm: ConnectionFormState;
    setConnectionForm: Dispatch<SetStateAction<ConnectionFormState>>;
    validationErrors: string[];
    testResult: { success: boolean; message: string; metadata?: ConnectionTestMetadata } | null;
    isSaving: boolean;
    isTesting: boolean;
    onClose: () => void;
    onReset: () => void;
    onTest: () => void;
    onSave: () => void | Promise<void>;
};

type DbTypeOption = {
    value: ConnectionFormState['db_type'];
    label: string;
    description: string;
    defaults: {
        port: string;
        schema_name: string;
        hostDisabled: boolean;
    };
};

type CommonConnectionOptions = {
    connect_timeout: string;
    sslmode: string;
    charset: string;
    application_name: string;
};

const DB_TYPE_OPTIONS: DbTypeOption[] = [
    {
        value: 'postgresql',
        label: 'PostgreSQL',
        description: '适合业务库、分析库和标准 Schema 结构。',
        defaults: { port: '5432', schema_name: 'public', hostDisabled: false },
    },
    {
        value: 'mysql',
        label: 'MySQL',
        description: '适合网站业务库、轻量数据服务和兼容生态。',
        defaults: { port: '3306', schema_name: '', hostDisabled: false },
    },
    {
        value: 'sqlite',
        label: 'SQLite',
        description: '适合本地文件型数据库和轻量单机场景。',
        defaults: { port: '', schema_name: '', hostDisabled: true },
    },
    {
        value: 'mssql',
        label: 'SQL Server',
        description: '适合 Windows / 企业内部数据仓与业务系统。',
        defaults: { port: '1433', schema_name: 'dbo', hostDisabled: false },
    },
];

function parseOptions(value: string): Record<string, unknown> {
    const text = value.trim();
    if (!text) return {};
    try {
        const parsed = JSON.parse(text);
        return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
    } catch {
        return {};
    }
}

function toText(value: unknown) {
    if (value === null || value === undefined) return '';
    return String(value);
}

export default function ConnectionModal({
    open,
    editingConnectionId,
    connectionForm,
    setConnectionForm,
    validationErrors,
    testResult,
    isSaving,
    isTesting,
    onClose,
    onReset,
    onTest,
    onSave,
}: ConnectionModalProps) {
    const [advancedOptionsOpen, setAdvancedOptionsOpen] = useState(false);
    const isSqlite = connectionForm.db_type === 'sqlite';
    const parsedOptions = useMemo(() => parseOptions(connectionForm.connection_options), [connectionForm.connection_options]);
    const currentDbOption = DB_TYPE_OPTIONS.find((item) => item.value === connectionForm.db_type) || DB_TYPE_OPTIONS[0];
    const commonOptions = useMemo<CommonConnectionOptions>(() => ({
        connect_timeout: toText(parsedOptions.connect_timeout),
        sslmode: toText(parsedOptions.sslmode),
        charset: toText(parsedOptions.charset),
        application_name: toText(parsedOptions.application_name),
    }), [parsedOptions]);
    const optionsHint = connectionForm.connection_options.trim()
        ? `${connectionForm.connection_options.trim().split('\n').length} 行 JSON 配置`
        : '未设置附加选项';
    const connectionModeLabel = isSqlite ? '文件型连接' : '网络型连接';

    useEffect(() => {
        if (!open) {
            setAdvancedOptionsOpen(false);
        }
    }, [open]);

    const applyDbType = (nextType: string) => {
        setConnectionForm((prev) => {
            const option = DB_TYPE_OPTIONS.find((item) => item.value === nextType) || DB_TYPE_OPTIONS[0];
            if (nextType === 'sqlite') {
                return {
                    ...prev,
                    db_type: nextType,
                    host: '',
                    port: '',
                    schema_name: '',
                    username: '',
                };
            }
            return {
                ...prev,
                db_type: nextType,
                port: option.defaults.port,
                schema_name: option.defaults.schema_name,
            };
        });
    };

    const updateCommonOption = (key: keyof CommonConnectionOptions, value: string) => {
        const nextOptions = { ...parsedOptions };
        if (value.trim()) {
            nextOptions[key] = key === 'connect_timeout' ? Number(value) : value;
        } else {
            delete nextOptions[key];
        }
        setConnectionForm((prev) => ({
            ...prev,
            connection_options: JSON.stringify(nextOptions, null, 2),
        }));
    };

    return (
        <FormModal
            open={open}
            title={editingConnectionId ? '编辑外部连接' : '新建外部连接'}
            subtitle="统一维护外部数据库连接、认证方式和接入参数。"
            width="960px"
            closeOnBackdrop={false}
            onClose={onClose}
        >
            <div className="connection-modal-shell">
                <div className="connection-modal-banner">
                    <div className="connection-modal-banner-copy">
                        <div className="connection-modal-banner-title">
                            <Database size={16} />
                            连接接入配置
                        </div>
                        <div className="resource-meta">
                            先选择数据库类型，再补齐定位、认证和连接参数。常用参数可以直接填写，高级配置仍支持 JSON。
                        </div>
                    </div>
                    <div className="connection-modal-badges">
                        <span className="connection-mode-pill">{connectionModeLabel}</span>
                        <label className="switch-chip connection-switch-chip">
                            <input
                                type="checkbox"
                                checked={connectionForm.is_active}
                                onChange={(e) => setConnectionForm((prev) => ({ ...prev, is_active: e.target.checked }))}
                            />
                            启用连接
                        </label>
                    </div>
                </div>

                <div className="editor-actions connection-editor-actions connection-editor-actions-top">
                    <button className="btn-outline" onClick={onTest} disabled={isTesting || isSaving}>
                        <SearchCheck size={14} />
                        {isTesting ? '测试中...' : '测试外部连接'}
                    </button>
                    <button className="btn-outline" onClick={onReset} disabled={isSaving || isTesting}>重置</button>
                    <button className="btn-primary" onClick={() => void onSave()} disabled={isSaving || isTesting}>
                        <Save size={14} />
                        {isSaving ? '保存中...' : '保存外部连接'}
                    </button>
                </div>

                <div className="connection-modal-main">
                    <div className="connection-form-pane">
                        <div className="connection-form-section">
                            <div className="connection-form-section-head">
                                <div className="connection-form-section-title">
                                    <BadgeCheck size={16} />
                                    基本信息
                                </div>
                                <div className="resource-meta">先确定连接名称、数据库类型和业务用途。</div>
                            </div>
                            <div className="form-grid two">
                                <label>
                                    <span>连接名称</span>
                                    <input
                                        value={connectionForm.name}
                                        placeholder="例如 财务分析库 / 运营主库"
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, name: e.target.value }))}
                                    />
                                </label>
                                <label>
                                    <span>用途说明</span>
                                    <input
                                        value={connectionForm.description}
                                        placeholder="简要说明这条连接服务于什么业务或数据场景"
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, description: e.target.value }))}
                                    />
                                </label>
                            </div>
                            <div className="connection-type-grid">
                                {DB_TYPE_OPTIONS.map((item) => (
                                    <button
                                        key={item.value}
                                        type="button"
                                        className={`connection-type-card ${connectionForm.db_type === item.value ? 'active' : ''}`}
                                        onClick={() => applyDbType(item.value)}
                                    >
                                        <div className="connection-type-card-head">
                                            <strong>{item.label}</strong>
                                            <span>{item.defaults.hostDisabled ? '文件型' : '网络型'}</span>
                                        </div>
                                        <div className="resource-meta">{item.description}</div>
                                        <div className="connection-type-card-meta">
                                            默认端口 {item.defaults.port || '无'} / 默认 Schema {item.defaults.schema_name || '无'}
                                        </div>
                                    </button>
                                ))}
                            </div>
                        </div>

                        <div className="connection-form-section">
                            <div className="connection-form-section-head">
                                <div className="connection-form-section-title">
                                    {isSqlite ? <FolderCog size={16} /> : <Network size={16} />}
                                    连接定位
                                </div>
                                <div className="resource-meta">
                                    {isSqlite ? 'SQLite 只需要数据库文件路径。' : '补齐主机、端口、数据库名和默认 Schema。'}
                                </div>
                            </div>
                            <div className="form-grid two">
                                <label>
                                    <span>Host</span>
                                    <input
                                        value={connectionForm.host}
                                        disabled={isSqlite}
                                        placeholder={isSqlite ? 'SQLite 不需要 Host' : '例如 127.0.0.1 / db.example.com'}
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, host: e.target.value }))}
                                    />
                                </label>
                                <label>
                                    <span>Port</span>
                                    <input
                                        value={connectionForm.port}
                                        disabled={isSqlite}
                                        placeholder={isSqlite ? 'SQLite 不需要端口' : `例如 ${currentDbOption.defaults.port || '5432'}`}
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, port: e.target.value }))}
                                    />
                                </label>
                                <label>
                                    <span>{isSqlite ? '数据库文件路径' : '数据库名'}</span>
                                    <input
                                        value={connectionForm.database_name}
                                        placeholder={isSqlite ? '例如 D:\\data\\finance.db' : '例如 finflow'}
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, database_name: e.target.value }))}
                                    />
                                </label>
                                <label>
                                    <span>默认 Schema</span>
                                    <input
                                        value={connectionForm.schema_name}
                                        disabled={isSqlite}
                                        placeholder={isSqlite ? 'SQLite 不需要 Schema' : '例如 public / dbo'}
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, schema_name: e.target.value }))}
                                    />
                                </label>
                            </div>
                        </div>

                        <div className="connection-form-section">
                            <div className="connection-form-section-head">
                                <div className="connection-form-section-title">
                                    <ShieldCheck size={16} />
                                    认证与连接参数
                                </div>
                                <div className="resource-meta">常用参数直接填写，高级场景再展开 JSON 补充。</div>
                            </div>
                            <div className="form-grid two">
                                <label>
                                    <span>用户名</span>
                                    <input
                                        value={connectionForm.username}
                                        disabled={isSqlite}
                                        placeholder={isSqlite ? 'SQLite 不需要用户名' : '可选'}
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, username: e.target.value }))}
                                    />
                                </label>
                                <label>
                                    <span>密码</span>
                                    <input
                                        type="password"
                                        value={connectionForm.password}
                                        placeholder={editingConnectionId ? '留空表示不修改当前密码' : '请输入连接密码'}
                                        onChange={(e) => setConnectionForm((prev) => ({ ...prev, password: e.target.value }))}
                                    />
                                </label>
                            </div>
                            <div className="form-grid two">
                                <label>
                                    <span>连接超时（秒）</span>
                                    <input
                                        value={commonOptions.connect_timeout}
                                        placeholder="例如 10"
                                        onChange={(e) => updateCommonOption('connect_timeout', e.target.value)}
                                    />
                                </label>
                                <label>
                                    <span>SSL 模式</span>
                                    <select
                                        value={commonOptions.sslmode}
                                        onChange={(e) => updateCommonOption('sslmode', e.target.value)}
                                    >
                                        <option value="">未设置</option>
                                        <option value="disable">disable</option>
                                        <option value="prefer">prefer</option>
                                        <option value="require">require</option>
                                        <option value="verify-ca">verify-ca</option>
                                        <option value="verify-full">verify-full</option>
                                    </select>
                                </label>
                                <label>
                                    <span>字符集</span>
                                    <input
                                        value={commonOptions.charset}
                                        placeholder="例如 utf8mb4 / utf-8"
                                        onChange={(e) => updateCommonOption('charset', e.target.value)}
                                    />
                                </label>
                                <label>
                                    <span>应用标识</span>
                                    <input
                                        value={commonOptions.application_name}
                                        placeholder="例如 finflow-data-center"
                                        onChange={(e) => updateCommonOption('application_name', e.target.value)}
                                    />
                                </label>
                            </div>
                            <div className="connection-advanced-block">
                                <button
                                    type="button"
                                    className="connection-advanced-toggle"
                                    onClick={() => setAdvancedOptionsOpen((prev) => !prev)}
                                >
                                    <span>高级 JSON 配置</span>
                                    {advancedOptionsOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                                </button>
                                {advancedOptionsOpen ? (
                                    <label className="form-block reporting-editor-field">
                                        <div className="field-head">
                                            <span>连接选项 JSON</span>
                                            <span className="resource-meta">适合补充驱动级参数、证书路径或未覆盖的连接项</span>
                                        </div>
                                        <textarea
                                            value={connectionForm.connection_options}
                                            onChange={(e) => setConnectionForm((prev) => ({ ...prev, connection_options: e.target.value }))}
                                        />
                                    </label>
                                ) : (
                                    <div className="helper-text">
                                        当前 {optionsHint}。如需更细的驱动参数，可展开后直接编辑完整 JSON。
                                    </div>
                                )}
                            </div>
                        </div>
                        <div className="connection-form-section connection-tips-card">
                            <div className="connection-form-section-title">
                                <SlidersHorizontal size={16} />
                                填写建议
                            </div>
                            <div className="journey-list compact-empty">
                                <div className="checklist-item">
                                    <span className={`status-dot ${connectionForm.name.trim() ? 'ready' : ''}`} />
                                    先给连接起一个业务上容易识别的名称。
                                </div>
                                <div className="checklist-item">
                                    <span className={`status-dot ${connectionForm.database_name.trim() ? 'ready' : ''}`} />
                                    {isSqlite ? 'SQLite 请填写服务器上实际可访问的数据库文件路径。' : '网络型数据库请确认数据库名与默认 Schema。'}
                                </div>
                                <div className="checklist-item">
                                    <span className={`status-dot ${isSqlite || connectionForm.host.trim() ? 'ready' : ''}`} />
                                    非 SQLite 连接至少要填写 Host，端口建议保持为驱动默认值。
                                </div>
                                <div className="checklist-item">
                                    <span className={`status-dot ${testResult?.success ? 'ready' : ''}`} />
                                    保存前先测试连接，能更早发现账号、权限或网络问题。
                                </div>
                            </div>
                        </div>

                        {validationErrors.length ? (
                            <div className="preview-status error">
                                {validationErrors.map((item, index) => (
                                    <div key={`${item}-${index}`}>{item}</div>
                                ))}
                            </div>
                        ) : null}

                        {testResult ? (
                            <div className={`preview-status ${testResult.success ? 'success' : 'error'}`}>
                                <div>{testResult.message}</div>
                                {testResult.metadata ? (
                                    <div className="resource-meta">
                                        {testResult.metadata.db_type} / {testResult.metadata.database_name}
                                        {testResult.metadata.server_version ? ` / ${testResult.metadata.server_version}` : ''}
                                    </div>
                                ) : null}
                            </div>
                        ) : (
                            <div className="preview-status loading">
                                SQLite 只需要数据库文件路径；其他数据库建议补齐 Host、Port、数据库名和账号后再测试。
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </FormModal>
    );
}
