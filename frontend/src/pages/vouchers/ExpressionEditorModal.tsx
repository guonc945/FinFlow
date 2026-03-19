import { useEffect, useRef, useState } from 'react';
import { Database, Hash, Maximize2, Save, X } from 'lucide-react';
import VariablePicker from '../settings/VariablePicker';
import SourceFieldPickerModal from './SourceFieldPickerModal';
import type { VoucherFieldModule } from '../../types';

interface ExpressionEditorModalProps {
    open: boolean;
    title: string;
    value: string;
    onClose: () => void;
    onSave: (value: string) => void;
    fieldModules?: VoucherFieldModule[] | null;
    useBraces?: boolean;
    placeholder?: string;
}

const ExpressionEditorModal = ({
    open,
    title,
    value,
    onClose,
    onSave,
    fieldModules,
    useBraces = true,
    placeholder,
}: ExpressionEditorModalProps) => {
    const [draft, setDraft] = useState(value || '');
    const [isVariablePickerOpen, setIsVariablePickerOpen] = useState(false);
    const [isSourceFieldPickerOpen, setIsSourceFieldPickerOpen] = useState(false);
    const textareaRef = useRef<HTMLTextAreaElement>(null);

    useEffect(() => {
        if (open) {
            setDraft(value || '');
        }
    }, [open, value]);

    const insertAtCursor = (insertText: string) => {
        const textarea = textareaRef.current;
        const currentText = draft || '';

        if (!textarea) {
            setDraft(`${currentText}${insertText}`);
            return;
        }

        const start = textarea.selectionStart ?? currentText.length;
        const end = textarea.selectionEnd ?? currentText.length;
        const nextText = `${currentText.slice(0, start)}${insertText}${currentText.slice(end)}`;
        setDraft(nextText);

        requestAnimationFrame(() => {
            textarea.focus();
            const cursor = start + insertText.length;
            textarea.setSelectionRange(cursor, cursor);
        });
    };

    if (!open) {
        return null;
    }

    return (
        <>
            <div className="expression-editor-overlay" onClick={onClose}>
                <div className="expression-editor-modal" onClick={e => e.stopPropagation()}>
                    <header className="expression-editor-header">
                        <div>
                            <div className="expression-editor-title-row">
                                <Maximize2 size={18} />
                                <h3>{title}</h3>
                            </div>
                            <p>在大编辑器中编写公式，保存后会直接回填到当前字段。</p>
                        </div>
                        <button type="button" className="expression-editor-close" onClick={onClose}>
                            <X size={18} />
                        </button>
                    </header>

                    <div className="expression-editor-toolbar">
                        <button type="button" className="expression-editor-toolbtn" onClick={() => setIsVariablePickerOpen(true)}>
                            <Hash size={14} />
                            插入变量/函数
                        </button>
                        {fieldModules && fieldModules.length > 0 && (
                            <button type="button" className="expression-editor-toolbtn" onClick={() => setIsSourceFieldPickerOpen(true)}>
                                <Database size={14} />
                                插入数据源字段
                            </button>
                        )}
                        <div className="expression-editor-meta">{draft.length} chars</div>
                    </div>

                    <div className="expression-editor-body">
                        <textarea
                            ref={textareaRef}
                            className="expression-editor-textarea"
                            value={draft}
                            onChange={e => setDraft(e.target.value)}
                            placeholder={placeholder || '请输入公式内容'}
                            autoFocus
                        />
                    </div>

                    <footer className="expression-editor-footer">
                        <button type="button" className="cancel-btn" onClick={onClose}>
                            取消
                        </button>
                        <button
                            type="button"
                            className="save-btn"
                            onClick={() => {
                                onSave(draft);
                                onClose();
                            }}
                        >
                            <Save size={16} />
                            保存到当前字段
                        </button>
                    </footer>
                </div>
            </div>

            <VariablePicker
                isOpen={isVariablePickerOpen}
                onClose={() => setIsVariablePickerOpen(false)}
                includeFunctions
                onSelect={(item) => insertAtCursor(item.insert_text || `{${item.key}}`)}
            />

            <SourceFieldPickerModal
                open={isSourceFieldPickerOpen}
                onClose={() => setIsSourceFieldPickerOpen(false)}
                modules={fieldModules || []}
                onPick={(field, ctx) => {
                    const key = (ctx?.module_id && ctx?.source_id)
                        ? `${ctx.module_id}.${ctx.source_id}.${field.value}`
                        : (ctx?.source_type ? `${ctx.source_type}.${field.value}` : field.value);
                    insertAtCursor(useBraces ? `{${key}}` : key);
                    setIsSourceFieldPickerOpen(false);
                }}
            />
        </>
    );
};

export default ExpressionEditorModal;
