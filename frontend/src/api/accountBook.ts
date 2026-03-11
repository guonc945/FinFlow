import axios from 'axios';
import type { PaginatedAccountBooks } from '../types/accountBook';

export const getAccountBooks = async (skip: number = 0, limit: number = 100, search?: string) => {
    const params = new URLSearchParams({ skip: skip.toString(), limit: limit.toString() });
    if (search) {
        params.append('search', search);
    }
    const response = await axios.get<PaginatedAccountBooks>(`${import.meta.env.VITE_API_BASE_URL}/finance/kd-account-books?${params.toString()}`);
    return response.data;
};

export const syncAccountBooks = async () => {
    const response = await axios.post<{ message: string }>(`${import.meta.env.VITE_API_BASE_URL}/finance/kd-account-books/sync`, {});
    return response.data;
};
