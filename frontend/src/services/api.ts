import axios from 'axios';
import type { CashJournal, DepositRecord, PushResult, VoucherPreview, ChargeItem, House, Project, Resident, BillVoucherPushStatus, ReceiptBill, VoucherFieldModule } from '../types';

import { API_BASE_URL } from './apiBase';

// Axios Interceptor for Auth
axios.interceptors.request.use((config) => {
    const token = localStorage.getItem('token');
    if (token) {
        config.headers.Authorization = `Bearer ${token}`;
    }

    // 注入当前账套上下文
    const accountBookNumber = localStorage.getItem('active_account_book_number');
    if (accountBookNumber) {
        config.headers['X-Account-Book-Number'] = encodeURIComponent(accountBookNumber);
    }
    const accountBookName = localStorage.getItem('active_account_book_name');
    if (accountBookName) {
        config.headers['X-Account-Book-Name'] = encodeURIComponent(accountBookName);
    }

    return config;
});

axios.interceptors.response.use(
    (response) => response,
    (error) => {
        if (error.response?.status === 401 || error.response?.status === 403) {
            localStorage.removeItem('token');
            localStorage.removeItem('user');
            if (!window.location.pathname.startsWith('/login')) {
                window.location.href = '/login';
            }
        }
        return Promise.reject(error);
    }
);

export const login = async (username: string, password: string) => {
    const response = await axios.post(`${API_BASE_URL}/auth/login`, { username, password });
    return response.data;
};

export const getJournals = async (): Promise<CashJournal[]> => {
    const response = await axios.get<CashJournal[]>(`${API_BASE_URL}/journals`);
    return response.data;
};

export const previewVoucher = async (flowId: string): Promise<VoucherPreview> => {
    const response = await axios.post<VoucherPreview>(`${API_BASE_URL}/journals/${flowId}/preview`);
    return response.data;
};

export const pushToKingdee = async (flowId: string): Promise<PushResult> => {
    const response = await axios.post<PushResult>(`${API_BASE_URL}/journals/${flowId}/push`);
    return response.data;
};

// Organization API
export const getOrganizations = async () => {
    const response = await axios.get(`${API_BASE_URL}/organizations`);
    return response.data;
};

export const getOrganizationsTree = async () => {
    const response = await axios.get(`${API_BASE_URL}/organizations/tree`);
    return response.data;
};

export const createOrganization = async (data: {
    name: string;
    code?: string;
    parent_id?: number;
    description?: string;
}) => {
    const response = await axios.post(`${API_BASE_URL}/organizations`, data);
    return response.data;
};

export const updateOrganization = async (id: number, data: {
    name?: string;
    code?: string;
    parent_id?: number;
    status?: number;
    description?: string;
}) => {
    const response = await axios.put(`${API_BASE_URL}/organizations/${id}`, data);
    return response.data;
};

export const deleteOrganization = async (id: number) => {
    const response = await axios.delete(`${API_BASE_URL}/organizations/${id}`);
    return response.data;
};

// User API
export const getUsers = async (orgId?: number) => {
    const params = orgId ? { org_id: orgId } : {};
    const response = await axios.get(`${API_BASE_URL}/users`, { params });
    return response.data;
};

export const createUser = async (data: {
    username: string;
    password: string;
    email?: string;
    phone?: string;
    real_name?: string;
    org_id?: number;
}) => {
    const response = await axios.post(`${API_BASE_URL}/users`, data);
    return response.data;
};

export const updateUser = async (id: number, data: {
    username?: string;
    email?: string | null;
    phone?: string | null;
    real_name?: string | null;
    org_id?: number;
    status?: number;
    password?: string;
}) => {
    const response = await axios.put(`${API_BASE_URL}/users/${id}`, data);
    return response.data;
};

export const deleteUser = async (id: number) => {
    const response = await axios.delete(`${API_BASE_URL}/users/${id}`);
    return response.data;
};

export const getMe = async () => {
    const response = await axios.get(`${API_BASE_URL}/users/me`);
    return response.data;
};

export const getUserById = async (userId: number) => {
    const response = await axios.get(`${API_BASE_URL}/users/${userId}`);
    return response.data;
};

// Bills Sync
export const syncBills = async (communityIds?: number[]) => {
    const response = await axios.post(`${API_BASE_URL}/bills/sync`, { community_ids: communityIds });
    return response.data; // Returns { task_id, ... }
};

export const getSyncStatus = async (taskId: string) => {
    const response = await axios.get(`${API_BASE_URL}/bills/sync/status/${taskId}`);
    return response.data;
};

// Receipt Bills Sync
export const syncReceiptBills = async (communityIds?: number[]) => {
    const response = await axios.post(`${API_BASE_URL}/receipt-bills/sync`, { community_ids: communityIds });
    return response.data;
};

export const getReceiptBillSyncStatus = async (taskId: string) => {
    const response = await axios.get(`${API_BASE_URL}/receipt-bills/sync/status/${taskId}`);
    return response.data;
};

// Deposit Records Sync
export const syncDepositRecords = async (communityIds?: number[]) => {
    const response = await axios.post(`${API_BASE_URL}/deposit-records/sync`, { community_ids: communityIds });
    return response.data;
};

export const getDepositRecordSyncStatus = async (taskId: string) => {
    const response = await axios.get(`${API_BASE_URL}/deposit-records/sync/status/${taskId}`);
    return response.data;
};

// Projects Sync
export const syncProjects = async () => {
    const response = await axios.post(`${API_BASE_URL}/projects/sync`);
    return response.data;
};

export const getProjects = async (params?: { skip?: number; limit?: number; current_account_book_only?: boolean }) => {
    const response = await axios.get(`${API_BASE_URL}/projects`, { params });
    return response.data;
};

export const updateProject = async (projId: string, data: Partial<Project>) => {
    const response = await axios.put(`${API_BASE_URL}/projects/${projId}`, data);
    return response.data;
};

// Dashboard & Stats - Removed Globally



export const getBills = async (params?: {
    search?: string;
    community_ids?: string;
    status?: string;
    charge_items?: string;
    customer_name?: string;
    bill_id?: string;
    receipt_id?: string;
    house_name?: string;
    start_date?: string;
    end_date?: string;
    in_month_start?: string;
    in_month_end?: string;
    pay_date_start?: string;
    pay_date_end?: string;
    pay_time_start?: string;
    pay_time_end?: string;
    deal_log_id?: number;
    skip?: number;
    limit?: number
}) => {
    const response = await axios.get(`${API_BASE_URL}/bills`, { params });
    return response.data;
};

export const exportBills = async (params?: {
    search?: string;
    community_ids?: string;
    status?: string;
    charge_items?: string;
    customer_name?: string;
    bill_id?: string;
    receipt_id?: string;
    house_name?: string;
    start_date?: string;
    end_date?: string;
    in_month_start?: string;
    in_month_end?: string;
    pay_date_start?: string;
    pay_date_end?: string;
    pay_time_start?: string;
    pay_time_end?: string;
    deal_log_id?: number;
}) => {
    const response = await axios.get(`${API_BASE_URL}/bills/export`, {
        params,
        responseType: 'blob',
    });

    const disposition = response.headers['content-disposition'] as string | undefined;
    const filenameStarMatch = disposition?.match(/filename\*=UTF-8''([^;]+)/i);
    const filenameMatch = disposition?.match(/filename=([^;]+)/i);
    const rawFilename = filenameStarMatch?.[1] || filenameMatch?.[1]?.replace(/"/g, '');
    const filename = rawFilename ? decodeURIComponent(rawFilename) : `bills_export_${Date.now()}.csv`;

    return {
        blob: response.data as Blob,
        filename,
    };
};

export const getBillChargeItems = async () => {
    const response = await axios.get(`${API_BASE_URL}/bills/charge-items`);
    return response.data as { value: string, label: string }[];
};

export const getVoucherFieldModules = async () => {
    const response = await axios.get<{ modules: VoucherFieldModule[] }>(`${API_BASE_URL}/vouchers/source-modules`);
    return response.data;
};

export const getVoucherTemplateCategoriesTree = async () => {
    const response = await axios.get(`${API_BASE_URL}/vouchers/template-categories/tree`);
    return response.data;
};

export const createVoucherTemplateCategory = async (data: {
    name: string;
    parent_id?: number | null;
    sort_order?: number;
    status?: number;
    description?: string | null;
}) => {
    const response = await axios.post(`${API_BASE_URL}/vouchers/template-categories`, data);
    return response.data;
};

export const updateVoucherTemplateCategory = async (id: number, data: {
    name?: string;
    parent_id?: number | null;
    sort_order?: number;
    status?: number;
    description?: string | null;
}) => {
    const response = await axios.put(`${API_BASE_URL}/vouchers/template-categories/${id}`, data);
    return response.data;
};

export const deleteVoucherTemplateCategory = async (id: number) => {
    const response = await axios.delete(`${API_BASE_URL}/vouchers/template-categories/${id}`);
    return response.data;
};

export const getReceiptBills = async (params?: {
    search?: string;
    community_ids?: string;
    deal_date_start?: string;
    deal_date_end?: string;
    skip?: number;
    limit?: number;
}) => {
    const response = await axios.get(`${API_BASE_URL}/receipt-bills`, { params });
    return response.data as { total: number; total_income_amount: number; items: ReceiptBill[] };
};

export const getReceiptBill = async (receiptBillId: number, communityId: number) => {
    const response = await axios.get(`${API_BASE_URL}/receipt-bills/${receiptBillId}`, {
        params: { community_id: communityId }
    });
    return response.data;
};

export const getDepositRecords = async (params?: {
    search?: string;
    community_ids?: string;
    operate_type?: number;
    operate_date_start?: string;
    operate_date_end?: string;
    pay_date_start?: string;
    pay_date_end?: string;
    has_refund_receipt?: boolean;
    skip?: number;
    limit?: number;
}) => {
    const response = await axios.get(`${API_BASE_URL}/deposit-records`, { params });
    return response.data as { total: number; total_amount: number; items: DepositRecord[] };
};

// Reports
export const getIncomeTrend = async (period: string = 'month') => {
    const response = await axios.get(`${API_BASE_URL}/reports/income-trend`, { params: { period } });
    return response.data;
};

export const getChargeItemsRanking = async (limit: number = 10) => {
    const response = await axios.get(`${API_BASE_URL}/reports/charge-items-ranking`, { params: { limit } });
    return response.data;
};

export const getReportingConnections = async () => {
    const response = await axios.get(`${API_BASE_URL}/reporting/db-connections`);
    return response.data;
};

export const createReportingConnection = async (data: any) => {
    const response = await axios.post(`${API_BASE_URL}/reporting/db-connections`, data);
    return response.data;
};

export const updateReportingConnection = async (id: number, data: any) => {
    const response = await axios.put(`${API_BASE_URL}/reporting/db-connections/${id}`, data);
    return response.data;
};

export const deleteReportingConnection = async (id: number) => {
    const response = await axios.delete(`${API_BASE_URL}/reporting/db-connections/${id}`);
    return response.data;
};

export const testReportingConnection = async (data: any) => {
    const response = await axios.post(`${API_BASE_URL}/reporting/db-connections/test`, data);
    return response.data;
};

export const getReportingConnectionTables = async (id: number, schemaName?: string) => {
    const response = await axios.get(`${API_BASE_URL}/reporting/db-connections/${id}/tables`, {
        params: schemaName ? { schema_name: schemaName } : undefined
    });
    return response.data;
};

export const getReportingDatasets = async () => {
    const response = await axios.get(`${API_BASE_URL}/reporting/datasets`);
    return response.data;
};

export const createReportingDataset = async (data: any) => {
    const response = await axios.post(`${API_BASE_URL}/reporting/datasets`, data);
    return response.data;
};

export const updateReportingDataset = async (id: number, data: any) => {
    const response = await axios.put(`${API_BASE_URL}/reporting/datasets/${id}`, data);
    return response.data;
};

export const deleteReportingDataset = async (id: number) => {
    const response = await axios.delete(`${API_BASE_URL}/reporting/datasets/${id}`);
    return response.data;
};

export const previewReportingDataset = async (id: number, data: { params?: Record<string, any>; limit?: number }) => {
    const response = await axios.post(`${API_BASE_URL}/reporting/datasets/${id}/preview`, data);
    return response.data;
};

export const previewReportingDatasetDraft = async (data: {
    connection_id: number;
    sql_text: string;
    params_json?: string | null;
    row_limit?: number;
    params?: Record<string, any>;
    limit?: number;
}) => {
    const response = await axios.post(`${API_BASE_URL}/reporting/datasets/preview-draft`, data);
    return response.data;
};

export const getReportingReports = async () => {
    const response = await axios.get(`${API_BASE_URL}/reporting/reports`);
    return response.data;
};

export const createReportingReport = async (data: any) => {
    const response = await axios.post(`${API_BASE_URL}/reporting/reports`, data);
    return response.data;
};

export const updateReportingReport = async (id: number, data: any) => {
    const response = await axios.put(`${API_BASE_URL}/reporting/reports/${id}`, data);
    return response.data;
};

export const deleteReportingReport = async (id: number) => {
    const response = await axios.delete(`${API_BASE_URL}/reporting/reports/${id}`);
    return response.data;
};

export const runReportingReport = async (id: number, data: { params?: Record<string, any>; limit?: number }) => {
    const response = await axios.post(`${API_BASE_URL}/reporting/reports/${id}/run`, data);
    return response.data;
};

export const getChargeItems = async () => {
    const response = await axios.get(`${API_BASE_URL}/charge-items`);
    return response.data;
};

export const syncChargeItems = async (communityIds?: number[]) => {
    const response = await axios.post(`${API_BASE_URL}/charge-items/sync`, { community_ids: communityIds });
    return response.data;
};

export const updateChargeItem = async (itemId: number, data: Partial<ChargeItem>) => {
    const response = await axios.put(`${API_BASE_URL}/charge-items/${itemId}`, data);
    return response.data;
};

// House API
export const getHouses = async (query?: { communityId?: string; search?: string; skip?: number; limit?: number }) => {
    const params = query ? { community_id: query.communityId, search: query.search, skip: query.skip, limit: query.limit } : {};
    const response = await axios.get(`${API_BASE_URL}/houses`, { params });
    return response.data;
};

export const updateHouse = async (houseId: number, data: Partial<House>) => {
    const response = await axios.put(`${API_BASE_URL}/houses/${houseId}`, data);
    return response.data;
};

export const syncHouses = async (communityIds: string[]) => {
    const response = await axios.post(`${API_BASE_URL}/houses/sync`, {
        community_ids: communityIds
    });
    return response.data;
};

export const getKingdeeHouses = async (query: { skip?: number; limit?: number; search?: string } = {}) => {
    const response = await axios.get(`${API_BASE_URL}/finance/kd-houses`, { params: query });
    return response.data;
};

export const getKingdeeProjects = async (query: { skip?: number; limit?: number; search?: string } = {}) => {
    const response = await axios.get(`${API_BASE_URL}/finance/auxiliary-data`, {
        params: { ...query, categories: '管理项目' }
    });
    return response.data;
};

export const getKingdeeCustomers = async (query: { skip?: number; limit?: number; search?: string } = {}) => {
    const response = await axios.get(`${API_BASE_URL}/finance/customers`, { params: query });
    return response.data;
};

// Resident API
export const getResidents = async (params?: { community_id?: string; search?: string; skip?: number; limit?: number }) => {
    const response = await axios.get(`${API_BASE_URL}/residents`, { params });
    return response.data;
};

export const syncResidents = async (communityIds: string[]) => {
    const response = await axios.post(`${API_BASE_URL}/residents/sync`, {
        community_ids: communityIds
    });
    return response.data;
};

export const updateResident = async (residentId: number, data: Partial<Resident>) => {
    const response = await axios.put(`${API_BASE_URL}/residents/${residentId}`, data);
    return response.data;
};

// Park API
export const getParks = async (query?: { communityId?: string; search?: string; skip?: number; limit?: number }) => {
    const params = query ? { community_id: query.communityId, search: query.search, skip: query.skip, limit: query.limit } : {};
    const response = await axios.get(`${API_BASE_URL}/parks`, { params });
    return response.data;
};

export const syncParks = async (communityIds: string[]) => {
    const response = await axios.post(`${API_BASE_URL}/parks/sync`, {
        community_ids: communityIds
    });
    return response.data;
};

export const updatePark = async (parkId: number, data: Partial<{ kingdee_house_id: string }>) => {
    const response = await axios.put(`${API_BASE_URL}/parks/${parkId}`, data);
    return response.data;
};

export const previewVoucherForBill = async (billId: number, communityId?: number) => {
    const response = await axios.post(`${API_BASE_URL}/vouchers/preview-bill/${billId}`, null, {
        params: communityId !== undefined ? { community_id: communityId } : undefined
    });
    return response.data;
};

export const previewBatchVoucherForBills = async (bills: Array<{ bill_id: number; community_id: number }>) => {
    const response = await axios.post(`${API_BASE_URL}/vouchers/preview-bills`, { bills });
    return response.data;
};

export const previewVoucherForReceipt = async (receiptBillId: number, communityId: number) => {
    const response = await axios.post(`${API_BASE_URL}/vouchers/preview-receipt/${receiptBillId}`, null, {
        params: { community_id: communityId }
    });
    return response.data;
};

export const previewBatchVoucherForReceipts = async (
    receipts: Array<{ receipt_bill_id: number; community_id: number }>
) => {
    const response = await axios.post(`${API_BASE_URL}/vouchers/preview-receipts`, { receipts });
    return response.data;
};

export const pushVoucherToKingdee = async (
    kingdeeJson: any,
    bills: Array<{ bill_id: number; community_id: number }> = [],
    apiId?: number,
    forcePush: boolean = false
) => {
    const response = await axios.post(`${API_BASE_URL}/vouchers/push`, {
        kingdee_json: kingdeeJson,
        api_id: apiId,
        bills,
        force_push: forcePush
    });
    return response.data as PushResult & { tracked_bills?: BillVoucherPushStatus[] };
};

export const resetBillVoucherBinding = async (
    bills: Array<{ bill_id: number; community_id: number }>,
    reason?: string
) => {
    const response = await axios.post(`${API_BASE_URL}/bills/voucher/reset`, {
        bills,
        reason
    });
    return response.data as { success: boolean; push_batch_no?: string };
};

export const queryVoucherById = async (voucherId: string) => {
    const response = await axios.post(`${API_BASE_URL}/vouchers/query`, {
        voucher_id: voucherId,
        page_no: 1,
        page_size: 1
    });
    return response.data as { success: boolean; exists: boolean };
};

// 银行账户
export const getBankAccounts = async (query?: { search?: string; skip?: number; limit?: number }) => {
    const response = await axios.get(`${API_BASE_URL}/finance/kd-bank-accounts`, { params: query });
    return response.data;
};

// 账套
export const getAccountBooks = async (query?: { search?: string; skip?: number; limit?: number }) => {
    const response = await axios.get(`${API_BASE_URL}/finance/kd-account-books`, { params: query });
    return response.data;
};
