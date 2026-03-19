import { useState } from 'react';
import { Maximize2 } from 'lucide-react';
import type { VoucherFieldModule } from '../../types';
import ExpressionEditorModal from './ExpressionEditorModal';

interface ExpressionInputWithActionsProps {
    value: string;
    onChange: (val: string) => void;
    fieldModules?: VoucherFieldModule[] | null;
    useBraces?: boolean;
    size?: 'normal' | 'mini';
    placeholder?: string;
    className?: string;
    editorTitle?: string;
}

const ExpressionInputWithActions = ({
    value,
    onChange,
    fieldModules,
    useBraces = true,
    size = 'normal',
    placeholder,
    className,
    editorTitle = '编辑公式',
}: ExpressionInputWithActionsProps) => {
    const [editorOpen, setEditorOpen] = useState(false);

    return (
        <>
            <div className={`expression-input-group ${size === 'mini' ? 'mini' : ''}`}>
                <div className={`input-with-action ${size === 'mini' ? 'mini' : ''}`}>
                    <input
                        type="text"
                        value={value}
                        readOnly
                        onClick={() => setEditorOpen(true)}
                        placeholder={placeholder}
                        className={`${className || ''} expression-preview-input`.trim()}
                        title="打开公式编辑器"
                    />
                    <button type="button" onClick={() => setEditorOpen(true)} title="打开公式编辑器">
                        <Maximize2 size={size === 'mini' ? 12 : 14} />
                    </button>
                </div>
            </div>
            <ExpressionEditorModal
                open={editorOpen}
                title={editorTitle}
                value={value}
                onClose={() => setEditorOpen(false)}
                onSave={onChange}
                fieldModules={fieldModules}
                useBraces={useBraces}
                placeholder={placeholder}
            />
        </>
    );
};

export default ExpressionInputWithActions;
