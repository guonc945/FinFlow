export interface AccountBook {
    id: string;
    number?: string;
    name: string;
    org_number?: string;
    org_name?: string;
    accountingsys_number?: string;
    accountingsys_name?: string;
    booknature?: string;
    accounttable_name?: string;
    basecurrency_name?: string;
    status?: string;
    enable?: string;
    created_at: string;
    updated_at?: string;
}

export interface PaginatedAccountBooks {
    items: AccountBook[];
    total: number;
}
