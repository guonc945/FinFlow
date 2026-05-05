import { useCallback, useState } from 'react';
import { CheckCircle, AlertCircle, X, Info } from 'lucide-react';
import './Toast.css';

export type ToastType = 'success' | 'error' | 'info';

interface ToastMessage {
    id: string;
    type: ToastType;
    message: string;
    description?: string;
}

export const useToast = () => {
    const [toasts, setToasts] = useState<ToastMessage[]>([]);

    const removeToast = useCallback((id: string) => {
        setToasts(prev => prev.filter(t => t.id !== id));
    }, []);

    const showToast = useCallback((type: ToastType, message: string, description?: string) => {
        const id = Math.random().toString(36).substring(7);
        setToasts(prev => [...prev, { id, type, message, description }]);
        window.setTimeout(() => removeToast(id), 4000);
    }, [removeToast]);

    return { toasts, showToast, removeToast };
};

export const ToastContainer = ({ toasts, removeToast }: { toasts: ToastMessage[], removeToast: (id: string) => void }) => {
    return (
        <div className="toast-container-fixed">
            {toasts.map(toast => (
                <div key={toast.id} className={`toast-card toast-${toast.type}`}>
                    <div className="toast-icon">
                        {toast.type === 'success' && <CheckCircle size={20} />}
                        {toast.type === 'error' && <AlertCircle size={20} />}
                        {toast.type === 'info' && <Info size={20} />}
                    </div>
                    <div className="toast-content">
                        <h4 className="toast-title">{toast.message}</h4>
                        {toast.description && <p className="toast-desc">{toast.description}</p>}
                    </div>
                    <button onClick={() => removeToast(toast.id)} className="toast-close">
                        <X size={16} />
                    </button>
                </div>
            ))}
        </div>
    );
};
