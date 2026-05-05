import { useMemo, useState, useCallback } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { json } from '@codemirror/lang-json';
import { oneDark } from '@codemirror/theme-one-dark';
import { EditorView } from '@codemirror/view';
import { Check, X, Maximize2, Minimize2, Copy, FileCode, AlertCircle, Hash } from 'lucide-react';
import VariablePicker from '../../pages/settings/VariablePicker';
import './JsonEditor.css';

interface JsonEditorProps {
    value: string;
    onChange: (value: string) => void;
    placeholder?: string;
    height?: string;
    readOnly?: boolean;
}

interface VariablePickerSelection {
    insert_text?: string;
    key?: string;
}

const getErrorMessage = (error: unknown, fallback: string) => {
    if (error instanceof Error && error.message) {
        return error.message;
    }
    return fallback;
};

const JsonEditor = ({
    value,
    onChange,
    placeholder = '请输入 JSON...',
    height = '300px',
    readOnly = false
}: JsonEditorProps) => {
    const [isExpanded, setIsExpanded] = useState(false);
    const [copySuccess, setCopySuccess] = useState(false);
    const [editorView, setEditorView] = useState<EditorView | null>(null);
    const [isVariablePickerOpen, setIsVariablePickerOpen] = useState(false);

    const { isValid, errorMsg } = useMemo(() => {
        if (!value || value.trim() === '') {
            return { isValid: true, errorMsg: null as string | null };
        }

        try {
            JSON.parse(value);
            return { isValid: true, errorMsg: null as string | null };
        } catch (error) {
            return {
                isValid: false,
                errorMsg: getErrorMessage(error, 'JSON 格式无效'),
            };
        }
    }, [value]);

    const handleFormat = useCallback(() => {
        try {
            const parsed = JSON.parse(value);
            const formatted = JSON.stringify(parsed, null, 2);
            onChange(formatted);
        } catch {
            // Validation state already shows the parse error, so we only keep the editor value unchanged here.
        }
    }, [value, onChange]);

    const handleCopy = useCallback(() => {
        navigator.clipboard.writeText(value);
        setCopySuccess(true);
        setTimeout(() => setCopySuccess(false), 2000);
    }, [value]);

    const handleTextChange = useCallback((val: string) => {
        onChange(val);
    }, [onChange]);

    const handleVariableSelect = (variable: VariablePickerSelection | string) => {
        if (editorView) {
            const varTag = typeof variable === 'string'
                ? variable
                : variable.insert_text || (variable.key ? `{${variable.key}}` : '');
            const { from, to } = editorView.state.selection.main;
            const transaction = editorView.state.update({
                changes: { from, to, insert: varTag },
                selection: { anchor: from + varTag.length }
            });
            editorView.dispatch(transaction);
            editorView.focus();
        }
    };

    return (
        <div className={`json-editor-container ${!isValid ? 'has-error' : ''} ${isExpanded ? 'is-expanded' : ''}`}>
            <div className="json-editor-toolbar">
                <div className="json-editor-status">
                    {isValid ? (
                        <span className="status-badge valid">
                            <Check size={12} /> 有效 JSON
                        </span>
                    ) : (
                        <span className="status-badge invalid" title={errorMsg || ''}>
                            <X size={12} /> 无效 JSON
                        </span>
                    )}
                </div>
                <div className="json-editor-actions">
                    <button
                        className="json-action-btn"
                        onClick={() => setIsVariablePickerOpen(true)}
                        title="插入全局变量"
                    >
                        <Hash size={14} /> 变量
                    </button>
                    <button
                        className="json-action-btn"
                        onClick={handleFormat}
                        title="格式化 JSON"
                        disabled={!value || !isValid}
                    >
                        <FileCode size={14} /> 格式化
                    </button>
                    <button
                        className="json-action-btn"
                        onClick={handleCopy}
                        title="复制内容"
                    >
                        {copySuccess ? <Check size={14} /> : <Copy size={14} />} 复制
                    </button>
                    <button
                        className="json-action-btn"
                        onClick={() => setIsExpanded(!isExpanded)}
                        title={isExpanded ? "退出全屏" : "全屏编辑"}
                    >
                        {isExpanded ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
                    </button>
                </div>
            </div>

            <div className="json-editor-wrapper">
                <CodeMirror
                    value={value}
                    height={isExpanded ? 'calc(100vh - 150px)' : height}
                    theme={oneDark}
                    extensions={[json()]}
                    onChange={handleTextChange}
                    onCreateEditor={(view) => setEditorView(view)}
                    readOnly={readOnly}
                    placeholder={placeholder}
                    basicSetup={{
                        lineNumbers: true,
                        highlightActiveLine: true,
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

            {!isValid && errorMsg && (
                <div className="json-editor-error-footer">
                    <AlertCircle size={14} />
                    <span>{errorMsg}</span>
                </div>
            )}

            <VariablePicker
                isOpen={isVariablePickerOpen}
                onClose={() => setIsVariablePickerOpen(false)}
                onSelect={handleVariableSelect}
            />
        </div>
    );
};

export default JsonEditor;
