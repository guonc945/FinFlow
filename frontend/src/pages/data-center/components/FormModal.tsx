import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { Maximize2, Minimize2, X } from 'lucide-react';

type FormModalProps = {
    open: boolean;
    title: string;
    subtitle?: string;
    width?: string;
    closeOnBackdrop?: boolean;
    onClose: () => void;
    children: ReactNode;
};

const FOCUSABLE_SELECTOR = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
].join(', ');

export default function FormModal({
    open,
    title,
    subtitle,
    width = '1200px',
    closeOnBackdrop = true,
    onClose,
    children,
}: FormModalProps) {
    const [isMaximized, setIsMaximized] = useState(false);
    const modalRef = useRef<HTMLDivElement>(null);

    // 打开时禁止背景页面滚动
    useEffect(() => {
        if (!open) return;
        const originalOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.body.style.overflow = originalOverflow;
        };
    }, [open]);

    // ESC 键关闭
    useEffect(() => {
        if (!open) return;
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                e.preventDefault();
                onClose();
            }
        };
        document.addEventListener('keydown', handleKeyDown);
        return () => document.removeEventListener('keydown', handleKeyDown);
    }, [open, onClose]);

    // 焦点捕获（Focus Trap）：确保 Tab 键不会跳出模态窗口
    const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
        if (e.key !== 'Tab') return;
        const container = modalRef.current;
        if (!container) return;

        const focusable = container.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR);
        if (!focusable.length) return;

        const first = focusable[0];
        const last = focusable[focusable.length - 1];

        if (e.shiftKey) {
            if (document.activeElement === first) {
                e.preventDefault();
                last.focus();
            }
        } else {
            if (document.activeElement === last) {
                e.preventDefault();
                first.focus();
            }
        }
    }, []);

    // 打开时自动聚焦到模态窗口
    useEffect(() => {
        if (!open) return;
        // 延迟一帧确保 DOM 已渲染
        const timer = requestAnimationFrame(() => {
            const container = modalRef.current;
            if (!container) return;
            const firstFocusable = container.querySelector<HTMLElement>(FOCUSABLE_SELECTOR);
            if (firstFocusable) {
                firstFocusable.focus();
            } else {
                container.focus();
            }
        });
        return () => cancelAnimationFrame(timer);
    }, [open]);

    if (!open) return null;

    return createPortal(
        <div
            className={`reporting-modal-backdrop ${isMaximized ? 'modal-maximized' : ''}`}
            onClick={closeOnBackdrop ? onClose : undefined}
        >
            <div
                ref={modalRef}
                className={`reporting-modal ${isMaximized ? 'reporting-modal-maximized' : ''}`}
                style={isMaximized ? {} : { maxWidth: width }}
                tabIndex={-1}
                onClick={(event) => event.stopPropagation()}
                onKeyDown={handleKeyDown}
            >
                <div className="reporting-modal-head">
                    <div>
                        <h3>{title}</h3>
                        {subtitle ? <div className="resource-meta">{subtitle}</div> : null}
                    </div>
                    <div className="reporting-modal-head-actions">
                        <button
                            className="ghost-btn"
                            type="button"
                            onClick={() => setIsMaximized(!isMaximized)}
                            title={isMaximized ? '还原' : '最大化'}
                        >
                            {isMaximized ? <Minimize2 size={16} /> : <Maximize2 size={16} />}
                        </button>
                        <button className="ghost-btn" type="button" onClick={onClose}>
                            <X size={16} />
                        </button>
                    </div>
                </div>
                <div className="reporting-modal-body">{children}</div>
            </div>
        </div>,
        document.body
    );
}
