import type { ReactNode } from 'react';
import { X } from 'lucide-react';

export type ConfirmModalVariant = 'primary' | 'danger';

interface ConfirmModalProps {
    isOpen: boolean;
    title: string;
    message: string;
    confirmText?: string;
    cancelText?: string;
    loading?: boolean;
    variant?: ConfirmModalVariant;
    children?: ReactNode;
    onConfirm: () => void;
    onCancel: () => void;
}

const variantStyles: Record<
    ConfirmModalVariant,
    { bg: string; hoverBg: string; shadow: string; hoverShadow: string }
> = {
    primary: {
        bg: 'linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%)',
        hoverBg: 'linear-gradient(135deg, #1d4ed8 0%, #1e40af 100%)',
        shadow: '0 10px 18px rgba(37, 99, 235, 0.22)',
        hoverShadow: '0 14px 24px rgba(37, 99, 235, 0.28)',
    },
    danger: {
        bg: 'linear-gradient(135deg, #f97316 0%, #ea580c 100%)',
        hoverBg: 'linear-gradient(135deg, #ea580c 0%, #c2410c 100%)',
        shadow: '0 10px 18px rgba(249, 115, 22, 0.20)',
        hoverShadow: '0 14px 24px rgba(249, 115, 22, 0.26)',
    },
};

const ConfirmModal = ({
    isOpen,
    title,
    message,
    confirmText = '确定',
    cancelText = '取消',
    loading = false,
    variant = 'primary',
    children,
    onConfirm,
    onCancel,
}: ConfirmModalProps) => {
    if (!isOpen) return null;

    const confirmStyle = variantStyles[variant] ?? variantStyles.primary;

    return (
        <div
            onClick={() => {
                if (!loading) onCancel();
            }}
            style={{
                position: 'fixed',
                inset: 0,
                zIndex: 1200,
                backgroundColor: 'rgba(15, 23, 42, 0.5)',
                backdropFilter: 'blur(6px)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '1.25rem',
            }}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: '#fff',
                    borderRadius: '1rem',
                    width: '520px',
                    maxWidth: '92vw',
                    boxShadow: '0 25px 50px -12px rgba(0,0,0,0.25)',
                    border: '1px solid #e2e8f0',
                    overflow: 'hidden',
                }}
            >
                <div
                    style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        padding: '1rem 1.25rem',
                        borderBottom: '1px solid #f1f5f9',
                        background: 'linear-gradient(135deg, #1e293b 0%, #334155 100%)',
                        color: '#fff',
                    }}
                >
                    <h3 style={{ margin: 0, fontSize: '1rem', fontWeight: 700 }}>{title}</h3>
                    <button
                        onClick={onCancel}
                        disabled={loading}
                        style={{
                            background: 'rgba(255,255,255,0.15)',
                            border: 'none',
                            borderRadius: '0.5rem',
                            color: '#fff',
                            cursor: loading ? 'not-allowed' : 'pointer',
                            padding: '0.375rem',
                            display: 'flex',
                            opacity: loading ? 0.6 : 1,
                        }}
                        title={loading ? '处理中...' : '关闭'}
                    >
                        <X size={18} />
                    </button>
                </div>

                <div style={{ padding: '1.25rem', color: '#0f172a' }}>
                    <div style={{ whiteSpace: 'pre-line', fontSize: '0.9rem', lineHeight: 1.6 }}>{message}</div>
                    {children ? <div style={{ marginTop: '0.85rem' }}>{children}</div> : null}
                </div>

                <div
                    style={{
                        display: 'flex',
                        justifyContent: 'flex-end',
                        gap: '0.75rem',
                        padding: '1rem 1.25rem',
                        borderTop: '1px solid #f1f5f9',
                        background: '#f8fafc',
                    }}
                >
                    <button
                        onClick={onCancel}
                        disabled={loading}
                        style={{
                            height: '38px',
                            padding: '0 0.95rem',
                            borderRadius: '0.75rem',
                            border: '1px solid #e2e8f0',
                            background: '#ffffff',
                            color: '#334155',
                            fontSize: '0.85rem',
                            fontWeight: 600,
                            cursor: loading ? 'not-allowed' : 'pointer',
                            opacity: loading ? 0.65 : 1,
                            transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
                        }}
                        onMouseEnter={(e) => {
                            if (loading) return;
                            (e.currentTarget as HTMLButtonElement).style.background = '#f8fafc';
                            (e.currentTarget as HTMLButtonElement).style.borderColor = '#cbd5e1';
                        }}
                        onMouseLeave={(e) => {
                            if (loading) return;
                            (e.currentTarget as HTMLButtonElement).style.background = '#ffffff';
                            (e.currentTarget as HTMLButtonElement).style.borderColor = '#e2e8f0';
                        }}
                        title={loading ? '处理中...' : cancelText}
                    >
                        {cancelText}
                    </button>
                    <button
                        onClick={onConfirm}
                        disabled={loading}
                        style={{
                            height: '38px',
                            padding: '0 1.05rem',
                            borderRadius: '0.75rem',
                            background: confirmStyle.bg,
                            border: '1px solid rgba(15, 23, 42, 0.08)',
                            color: '#fff',
                            fontSize: '0.85rem',
                            fontWeight: 700,
                            opacity: loading ? 0.85 : 1,
                            cursor: loading ? 'not-allowed' : 'pointer',
                            boxShadow: confirmStyle.shadow,
                            transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
                        }}
                        onMouseEnter={(e) => {
                            if (loading) return;
                            (e.currentTarget as HTMLButtonElement).style.background = confirmStyle.hoverBg;
                            (e.currentTarget as HTMLButtonElement).style.boxShadow = confirmStyle.hoverShadow;
                            (e.currentTarget as HTMLButtonElement).style.transform = 'translateY(-1px)';
                        }}
                        onMouseLeave={(e) => {
                            if (loading) return;
                            (e.currentTarget as HTMLButtonElement).style.background = confirmStyle.bg;
                            (e.currentTarget as HTMLButtonElement).style.boxShadow = confirmStyle.shadow;
                            (e.currentTarget as HTMLButtonElement).style.transform = 'translateY(0)';
                        }}
                        title={loading ? '处理中...' : confirmText}
                    >
                        {loading ? '处理中...' : confirmText}
                    </button>
                </div>
            </div>
        </div>
    );
};

export default ConfirmModal;
