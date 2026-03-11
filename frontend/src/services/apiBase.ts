export const resolveApiBaseUrl = () => {
    const raw = (import.meta.env.VITE_API_BASE_URL as string | undefined) || '';
    const envBase = raw.trim();

    // Explicit mode (recommended for production builds)
    if (envBase && envBase !== 'auto') {
        return envBase.replace(/\/+$/, '');
    }

    // Auto mode (dev convenience):
    // - Opened via http://localhost:5273 -> API http://localhost:8100/api
    // - Opened via http://192.168.x.x:5273 -> API http://192.168.x.x:8100/api
    const port = ((import.meta.env.VITE_API_PORT as string | undefined) || '8100').trim();

    // In case this module is ever imported in a non-browser environment.
    if (typeof window === 'undefined') {
        return `http://localhost:${port}/api`;
    }

    const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
    const host = window.location.hostname || 'localhost';
    return `${protocol}//${host}:${port}/api`;
};

export const API_BASE_URL = resolveApiBaseUrl();

