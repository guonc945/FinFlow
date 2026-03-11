const API_BASE_URL = 'http://localhost:8000/api';

async function request(url, options = {}) {
    const response = await fetch(`${API_BASE_URL}${url}`, {
        headers: {
            'Content-Type': 'application/json',
            ...options.headers
        },
        ...options
    });
    
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    return response.json();
}

const api = {
    getStats: async () => {
        return await request('/stats');
    },
    
    getChargeItems: async (params = {}) => {
        const query = new URLSearchParams(params).toString();
        return await request(`/charge-items?${query}`);
    },
    
    getProjects: async (params = {}) => {
        const query = new URLSearchParams(params).toString();
        return await request(`/projects?${query}`);
    },
    
    getBills: async (params = {}) => {
        const query = new URLSearchParams(params).toString();
        return await request(`/bills?${query}`);
    },
    
    getBillById: async (id) => {
        return await request(`/bills/${id}`);
    },
    
    getRecentBills: async (limit = 5) => {
        return await request(`/bills?limit=${limit}&sort=created_at&order=desc`);
    },
    
    getIncomeTrend: async (period = 'month') => {
        return await request(`/reports/income-trend?period=${period}`);
    },
    
    getChargeItemRanking: async (limit = 10) => {
        return await request(`/reports/charge-items-ranking?limit=${limit}`);
    }
};