import React, { useMemo, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { MSSQL, MySQL, PostgreSQL, SQLite, sql } from '@codemirror/lang-sql';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { EditorView } from '@codemirror/view';
import { tags } from '@lezer/highlight';
import { Check, Copy, Hash, Maximize2, Minimize2, RefreshCw, AlignLeft } from 'lucide-react';
import { format } from 'sql-formatter';
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
    showCopy?: boolean;
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

type SupportedFormatterDialect = 'postgresql' | 'mysql' | 'sqlite' | 'transactsql' | 'sql';
type VariableOption = {
    insert_text?: string;
    key?: string;
};

const sqlHighlightStyle = HighlightStyle.define([
    { tag: [tags.keyword, tags.controlKeyword, tags.operatorKeyword], color: '#7c3aed', fontWeight: '700' },
    { tag: [tags.string, tags.special(tags.string)], color: '#047857' },
    { tag: [tags.number, tags.integer, tags.float], color: '#b45309' },
    { tag: [tags.bool, tags.null], color: '#dc2626', fontWeight: '600' },
    { tag: [tags.comment, tags.lineComment, tags.blockComment], color: '#94a3b8', fontStyle: 'italic' },
    { tag: [tags.name, tags.propertyName, tags.attributeName, tags.labelName], color: '#0f766e' },
    { tag: [tags.definition(tags.name), tags.standard(tags.name)], color: '#0369a1' },
    { tag: [tags.paren, tags.squareBracket, tags.brace], color: '#475569' },
    { tag: [tags.punctuation, tags.separator], color: '#64748b' },
    { tag: tags.operator, color: '#1d4ed8' },
]);

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

const resolveFormatterDialect = (dialect?: string): SupportedFormatterDialect => {
    const normalized = (dialect || 'postgresql').trim().toLowerCase();
    if (normalized === 'mysql' || normalized === 'mariadb') {
        return 'mysql';
    }
    if (normalized === 'sqlite') {
        return 'sqlite';
    }
    if (normalized === 'mssql' || normalized === 'sqlserver') {
        return 'transactsql';
    }
    if (normalized === 'postgresql' || normalized === 'postgres' || normalized === 'pgsql') {
        return 'postgresql';
    }
    return 'sql';
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
    showCopy = true,
}) => {
    const [isExpanded, setIsExpanded] = useState(false);
    const [copySuccess, setCopySuccess] = useState(false);
    const [editorView, setEditorView] = useState<EditorView | null>(null);
    const [isVariablePickerOpen, setIsVariablePickerOpen] = useState(false);
    const [formatError, setFormatError] = useState<string | null>(null);

    const dialectLabel = DIALECT_LABELS[(dialect || '').toLowerCase()] || 'SQL';
    const extensions = useMemo(() => {
        const dialectConfig = resolveDialect(dialect);
        return [
            sql({ dialect: dialectConfig }),
            syntaxHighlighting(sqlHighlightStyle),
        ];
    }, [dialect]);

    const handleCopy = async () => {
        await navigator.clipboard.writeText(value);
        setCopySuccess(true);
        window.setTimeout(() => setCopySuccess(false), 1800);
    };

    const handleVariableSelect = (variable: VariableOption | string) => {
        if (!editorView) {
            return;
        }

        const insertText = typeof variable === 'string'
            ? variable
            : variable.insert_text || (variable.key ? `{${variable.key}}` : '');
        const { from, to } = editorView.state.selection.main;
        const transaction = editorView.state.update({
            changes: { from, to, insert: insertText },
            selection: { anchor: from + insertText.length },
        });

        editorView.dispatch(transaction);
        editorView.focus();
    };

    const handleFormat = () => {
        if (!value.trim()) {
            setFormatError('SQL 内容为空，无法格式化');
            setTimeout(() => setFormatError(null), 2000);
            return;
        }

        try {
            const formatted = format(value, {
                language: resolveFormatterDialect(dialect),
                tabWidth: 4,
                useTabs: false,
                keywordCase: 'upper',
                linesBetweenQueries: 2,
            });
            onChange(formatted);
            setFormatError(null);
        } catch (error) {
            setFormatError(error instanceof Error ? error.message : '格式化失败，请检查 SQL 语法');
            setTimeout(() => setFormatError(null), 3000);
        }
    };

    return (
        <div className={`sql-editor-container ${isExpanded ? 'is-expanded' : ''}`}>
            <div className="sql-editor-toolbar">
                <div className="sql-editor-status">
                    <span className="status-badge dialect">{dialectLabel}</span>
                    <span className="sql-editor-hint">支持高亮、折叠、补全与全局变量插入</span>
                    {formatError && <span className="sql-format-error">{formatError}</span>}
                </div>
                <div className="sql-editor-actions">
                    <button
                        className="sql-action-btn"
                        onClick={handleFormat}
                        title="格式化 SQL"
                        disabled={readOnly}
                        type="button"
                    >
                        <AlignLeft size={14} />
                        格式化
                    </button>
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
                    {showCopy && (
                        <button className="sql-action-btn" onClick={() => void handleCopy()} title="复制 SQL" type="button">
                            {copySuccess ? <Check size={14} /> : <Copy size={14} />}
                            复制
                        </button>
                    )}
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
                    extensions={extensions}
                    onChange={onChange}
                    onCreateEditor={(view) => setEditorView(view)}
                    readOnly={readOnly}
                    placeholder={placeholder}
                    theme="light"
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
