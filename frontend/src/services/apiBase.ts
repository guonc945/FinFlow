import { isSecureContextForAuth } from '../utils/authStorage';

const isLoopbackHost = (host: string) => host === 'localhost' || host === '127.0.0.1' || host === '::1';

export const resolveApiBaseUrl = () => {
    // 优先使用环境变量 VITE_API_BASE_URL
    const envBase = (import.meta.env.VITE_API_BASE_URL as string | undefined) || '';
    
    // 如果环境变量设置了且不是 auto，直接使用
    if (envBase && envBase !== 'auto') {
        return envBase.replace(/\/+$/, '');
    }

    // 备用：使用 VITE_API_PORT 构建地址
    const port = ((import.meta.env.VITE_API_PORT as string | undefined) || '8100').trim();
    
    // 非浏览器环境
    if (typeof window === 'undefined') {
        return `http://127.0.0.1:${port}/api`;
    }

    // 浏览器环境：使用当前主机名 + API 端口
    const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
    const host = window.location.hostname || '127.0.0.1';
    
    // 如果当前访问的是域名，使用域名；如果是 IP，使用 IP
    return `${protocol}//${host}:${port}/api`;
};

export const API_BASE_URL = resolveApiBaseUrl();

export const assertSecureApiAccess = () => {
    if (typeof window === 'undefined') return;
    if (isSecureContextForAuth()) return;

    try {
        const apiUrl = new URL(API_BASE_URL, window.location.origin);
        if (apiUrl.protocol === 'https:' || isLoopbackHost(apiUrl.hostname)) {
            return;
        }
    } catch {
        // Fall through to throw with a user-facing message.
    }

    throw new Error('检测到当前认证请求将通过非 HTTPS 连接发送。请改用 HTTPS 部署访问系统。');
};

// 调试日志（仅在开发环境显示）
if (import.meta.env.DEV) {
    console.info('[API] Base URL resolved');
}
