const TOKEN_KEY = 'token';
const USER_KEY = 'user';

const canUseStorage = () => typeof window !== 'undefined';

const migrateLegacyAuthValue = (key: string): string | null => {
    if (!canUseStorage()) return null;

    const sessionValue = window.sessionStorage.getItem(key);
    if (sessionValue) {
        return sessionValue;
    }

    const legacyValue = window.localStorage.getItem(key);
    if (legacyValue) {
        window.sessionStorage.setItem(key, legacyValue);
        window.localStorage.removeItem(key);
    }
    return legacyValue;
};

export const getAuthToken = (): string | null => migrateLegacyAuthValue(TOKEN_KEY);

export const setAuthToken = (token: string) => {
    if (!canUseStorage()) return;
    window.sessionStorage.setItem(TOKEN_KEY, token);
    window.localStorage.removeItem(TOKEN_KEY);
};

export const getAuthUser = <T = unknown>(): T | null => {
    const raw = migrateLegacyAuthValue(USER_KEY);
    if (!raw) return null;

    try {
        return JSON.parse(raw) as T;
    } catch {
        return null;
    }
};

export const setAuthUser = (user: unknown) => {
    if (!canUseStorage()) return;
    window.sessionStorage.setItem(USER_KEY, JSON.stringify(user));
    window.localStorage.removeItem(USER_KEY);
};

export const clearAuthSession = () => {
    if (!canUseStorage()) return;
    window.sessionStorage.removeItem(TOKEN_KEY);
    window.sessionStorage.removeItem(USER_KEY);
    window.localStorage.removeItem(TOKEN_KEY);
    window.localStorage.removeItem(USER_KEY);
};

export const isSecureContextForAuth = (): boolean => {
    if (typeof window === 'undefined') return true;
    const { protocol, hostname } = window.location;
    if (protocol === 'https:') return true;

    return hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '::1';
};
