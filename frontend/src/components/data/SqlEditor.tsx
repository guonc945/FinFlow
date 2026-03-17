import React, { useMemo, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { MSSQL, MySQL, PostgreSQL, SQLite, sql } from '@codemirror/lang-sql';
import type { Extension } from '@codemirror/state';
import { oneDark } from '@codemirror/theme-one-dark';
import { EditorView } from '@codemirror/view';
import { Check, Copy, Hash, Maximize2, Minimize2, RefreshCw, WrapText } from 'lucide-react';
import VariablePicker from '../../pages/settings/VariablePicker';
import './SqlEditor.css';

interface SqlEditorProps {
    value: string;
    onChange: (value: string) => void;
    placeholder?: string;
    height?: string;
    readOnly?: boolean;
    dialect?: string;
    onPreview?: () => void;
    previewLoading?: boolean;
}

const DIALECT_LABELS: Record<string, string> = {
    postgresql: 'PostgreSQL',
    postgres: 'PostgreSQL',
    pgsql: 'PostgreSQL',
    mysql: 'MySQL',
    mariadb: 'MariaDB',
    sqlite: 'SQLite',
    mssql: 'SQL Server',
    sqlserver: 'SQL Server',
};

const resolveDialect = (dialect?: string) => {
    const normalized = (dialect || 'postgresql').trim().toLowerCase();

    if (normalized === 'mysql' || normalized === 'mariadb') {
        return MySQL;
    }
    if (normalized === 'sqlite') {
        return SQLite;
    }
    if (normalized === 'mssql' || normalized === 'sqlserver') {
        return MSSQL;
    }
    return PostgreSQL;
};

const SqlEditor: React.FC<SqlEditorProps> = ({
    value,
    onChange,
    placeholder = 'SELECT *\nFROM your_table\nWHERE created_at >= {CURRENT_DATE}',
    height = '320px',
    readOnly = false,
    dialect = 'postgresql',
    onPreview,
    previewLoading = false,
}) => {
    const [isExpanded, setIsExpanded] = useState(false);
    const [copySuccess, setCopySuccess] = useState(false);
    const [lineWrap, setLineWrap] = useState(false);
    const [editorView, setEditorView] = useState<EditorView | null>(null);
    const [isVariablePickerOpen, setIsVariablePickerOpen] = useState(false);

    const dialectLabel = DIALECT_LABELS[(dialect || '').toLowerCase()] || 'SQL';
    const extensions = useMemo(() => {
        const items: Extension[] = [sql({ dialect: resolveDialect(dialect), upperCaseKeywords: true })];
        if (lineWrap) {
            items.push(EditorView.lineWrapping);
        }
        return items;
    }, [dialect, lineWrap]);

    const handleCopy = async () => {
        await navigator.clipboard.writeText(value);
        setCopySuccess(true);
        window.setTimeout(() => setCopySuccess(false), 1800);
    };

    const handleVariableSelect = (variable: any) => {
        if (!editorView) {
            return;
        }

        const insertText = variable?.insert_text || (variable?.key ? `{${variable.key}}` : String(variable || ''));
        const { from, to } = editorView.state.selection.main;
        const transaction = editorView.state.update({
            changes: { from, to, insert: insertText },
            selection: { anchor: from + insertText.length },
        });

        editorView.dispatch(transaction);
        editorView.focus();
    };

    return (
        <div className={`sql-editor-container ${isExpanded ? 'is-expanded' : ''}`}>
            <div className="sql-editor-toolbar">
                <div className="sql-editor-status">
                    <span className="status-badge dialect">{dialectLabel}</span>
                    <span className="sql-editor-hint">支持高亮、折叠、补全与全局变量插入</span>
                </div>
                <div className="sql-editor-actions">
                    {onPreview && (
                        <button
                            className="sql-action-btn primary"
                            onClick={onPreview}
                            disabled={previewLoading || readOnly}
                            title="执行预览查询"
                            type="button"
                        >
                            <RefreshCw size={14} className={previewLoading ? 'is-spinning' : ''} />
                            {previewLoading ? '预览中' : '预览'}
                        </button>
                    )}
                    <button
                        className="sql-action-btn"
                        onClick={() => setIsVariablePickerOpen(true)}
                        title="调用系统全局变量窗口"
                        disabled={readOnly}
                        type="button"
                    >
                        <Hash size={14} />
                        变量
                    </button>
                    <button
                        className={`sql-action-btn ${lineWrap ? 'is-active' : ''}`}
                        onClick={() => setLineWrap((prev) => !prev)}
                        title="切换自动换行"
                        type="button"
                    >
                        <WrapText size={14} />
                        换行
                    </button>
                    <button className="sql-action-btn" onClick={() => void handleCopy()} title="复制 SQL" type="button">
                        {copySuccess ? <Check size={14} /> : <Copy size={14} />}
                        复制
                    </button>
                    <button
                        className="sql-action-btn"
                        onClick={() => setIsExpanded((prev) => !prev)}
                        title={isExpanded ? '退出全屏' : '全屏编辑'}
                        type="button"
                    >
                        {isExpanded ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
                    </button>
                </div>
            </div>

            <div className="sql-editor-wrapper">
                <CodeMirror
                    value={value}
                    height={isExpanded ? 'calc(100vh - 170px)' : height}
                    theme={oneDark}
                    extensions={extensions}
                    onChange={onChange}
                    onCreateEditor={(view) => setEditorView(view)}
                    readOnly={readOnly}
                    placeholder={placeholder}
                    basicSetup={{
                        lineNumbers: true,
                        highlightActiveLine: true,
                        highlightActiveLineGutter: true,
                        bracketMatching: true,
                        closeBrackets: true,
                        autocompletion: true,
                        foldGutter: true,
                        dropCursor: true,
                        allowMultipleSelections: true,
                        indentOnInput: true,
                    }}
                />
            </div>

            <VariablePicker
                isOpen={isVariablePickerOpen}
                onClose={() => setIsVariablePickerOpen(false)}
                onSelect={handleVariableSelect}
            />
        </div>
    );
};

export default SqlEditor;
