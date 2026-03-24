export interface CashJournal {
    id: number;
    flow_id: string;
    amount: number;
    direction: string;
    status: 'pending' | 'pushed' | 'failed';
    voucher_id?: string;
    error_msg?: string;
    created_at: string;
}

export interface PushResult {
    success: boolean;
    voucher_id?: string;
    voucher_number?: string;
    message?: string;
    push_batch_no?: string;
    tracked_bills?: BillVoucherPushStatus[];
}

export interface BillVoucherPushStatus {
    bill_id: number;
    community_id: number;
    push_status: 'not_pushed' | 'pushing' | 'success' | 'failed';
    push_status_label: string;
    push_batch_no?: string | null;
    voucher_number?: string | null;
    voucher_id?: string | null;
    pushed_at?: string | null;
    message?: string | null;
    account_book_number?: string | null;
}

export interface VoucherEntry {
    line_no: number;
    dr_cr: string;
    account_code: string;
    amount: number;
    summary: string;
    aux_items?: Record<string, string>;
}

export interface VoucherPreview {
    entries: VoucherEntry[];
    total_debit: number;
    total_credit: number;
    is_balanced: boolean;
}

export interface Bill {
    id: string;
    community_id?: number;
    community_name: string;
    charge_item_name: string;
    asset_name: string;
    full_house_name: string;
    in_month: string;
    amount: number;
    customer_name?: string;

    pay_status_str: string;
    pay_time: number | null;
    receive_date?: string | null;
    deal_log_id?: number | null;
    created_at: string;
    push_status: 'not_pushed' | 'pushing' | 'success' | 'failed';
    push_status_label: string;
    push_batch_no?: string | null;
    voucher_number?: string | null;
    voucher_id?: string | null;
    pushed_at?: string | null;
    message?: string | null;
    account_book_number?: string | null;
}

export interface VoucherSourceFieldOption {
    label: string;
    value: string;
    group?: string;
}

export interface VoucherFieldSource {
    id: string;
    label: string;
    source_type: string;
    root_enabled?: boolean;
    note?: string;
    fields: VoucherSourceFieldOption[];
}

export interface VoucherFieldModule {
    id: string;
    label: string;
    note?: string;
    sources: VoucherFieldSource[];
}

export interface VoucherRelationOption {
    resolver: string;
    label: string;
    root_source: string;
    target_source: string;
}

export interface VoucherSourceMetadataResponse {
    modules: VoucherFieldModule[];
    relations: VoucherRelationOption[];
}

export interface PushStatusSummary {
    total: number;
    not_pushed: number;
    pushing: number;
    success: number;
    failed: number;
}

export interface TableColumnPreference {
    table_id: string;
    hidden: string[];
    order: string[];
    updated_at?: string | null;
}

export interface ReceiptBillDrilldownSection {
    relation_key: string;
    source_type: 'bills' | 'deposit_records' | 'prepayment_records' | string;
    label: string;
    count: number;
    items?: Array<Bill | DepositRecord | PrepaymentRecord>;
}

export interface ReceiptBillDepositRefundLinkSummary {
    matched: boolean;
    link_count: number;
    link_type: 'actual_refund' | 'transfer_to_prepayment' | 'mixed' | 'unmatched' | string;
    link_type_label?: string | null;
    match_rule?: string | null;
    match_confidence?: number | null;
}

export interface ReceiptBill {
    id: string;
    community_id: number;
    community_name: string;
    receipt_id?: string | null;
    asset_name?: string | null;
    payee?: string | null;
    payer_name?: string | null;
    income_amount: number;
    amount: number;
    bill_amount: number;
    discount_amount: number;
    late_money_amount: number;
    deposit_amount: number;
    pay_channel_str?: string | null;
    deal_time?: number | null;
    deal_date?: string | null;
    deal_type?: number | null;
    deal_type_label?: string | null;
    related_bill_count?: number;
    related_bill_push_summary?: PushStatusSummary;
    push_status?: 'unbound' | 'not_pushed' | 'pushing' | 'success' | 'failed' | 'partial';
    push_status_label?: string;
    push_batch_no?: string | null;
    voucher_number?: string | null;
    voucher_id?: string | null;
    pushed_at?: string | null;
    message?: string | null;
    account_book_number?: string | null;
    drilldown_enabled?: boolean;
    drilldown_source?: string | null;
    drilldown_count?: number;
    drilldown_summary?: string | null;
    drilldown_sections?: ReceiptBillDrilldownSection[];
    supports_bill_push_ops?: boolean;
}

export interface DepositRecord {
    id: string;
    community_id?: number | null;
    community_name?: string | null;
    house_id?: number | null;
    house_name?: string | null;
    resident_name?: string | null;
    amount: number;
    operate_type?: number | null;
    operate_type_label?: string | null;
    operator?: number | null;
    operator_name?: string | null;
    operate_time?: number | null;
    operate_date?: string | null;
    cash_pledge_name?: string | null;
    remark?: string | null;
    pay_time?: number | null;
    pay_date?: string | null;
    payment_id?: number | null;
    has_refund_receipt?: boolean;
    refund_receipt_id?: number | null;
    pay_channel_str?: string | null;
    created_at: string;
    updated_at?: string | null;
}

export interface PrepaymentRecord {
    id: string;
    community_id?: number | null;
    community_name?: string | null;
    account_id?: number | null;
    building_id?: number | null;
    unit_id?: number | null;
    house_id?: number | null;
    house_name?: string | null;
    resident_name?: string | null;
    amount: number;
    balance_after_change?: number | null;
    operate_type?: number | null;
    operate_type_label?: string | null;
    pay_channel_id?: number | null;
    pay_channel_str?: string | null;
    operator?: number | null;
    operator_name?: string | null;
    operate_time?: number | null;
    operate_date?: string | null;
    source_updated_time?: string | null;
    remark?: string | null;
    deposit_order_id?: number | null;
    pay_time?: number | null;
    pay_date?: string | null;
    category_id?: number | null;
    category_name?: string | null;
    status?: number | null;
    payment_id?: number | null;
    has_refund_receipt?: boolean;
    refund_receipt_id?: number | null;
    created_at: string;
    updated_at?: string | null;
}

export interface ReceiptBillDetail extends ReceiptBill {
    asset_type?: number | null;
    asset_id?: number | null;
    pay_channel?: number | null;
    pay_channel_list?: string | null;
    remark?: string | null;
    fk_id?: number | null;
    receipt_version?: number | null;
    invoice_number?: string | null;
    invoice_urls?: string | null;
    invoice_status?: number | null;
    open_invoice?: number | null;
    bind_users_raw?: string | null;
    users?: Array<{ user_id?: number | null; user_name?: string | null; phone?: string | null }>;
    related_bills?: Bill[];
    related_deposit_collect?: DepositRecord[];
    related_deposit_refund?: DepositRecord[];
    related_prepayment_recharge?: PrepaymentRecord[];
    related_prepayment_refund?: PrepaymentRecord[];
    deposit_refund_links?: Array<Record<string, any>>;
    deposit_refund_link_summary?: ReceiptBillDepositRefundLinkSummary | null;
}

export interface ChargeItem {
    item_id: number;
    communityid: string;
    item_name: string;
    current_account_subject_id?: string;
    profit_loss_subject_id?: string;
    current_account_subject?: AccountingSubject;
    profit_loss_subject?: AccountingSubject;
    created_at: string;
}

export interface KingdeeProject {
    id: string;
    number: string;
    name: string;
    group_name?: string;
}

export interface BankAccountBrief {
    id: string;
    name: string;
    bankaccountnumber: string;
    bank_name?: string;
}

export interface KingdeeAccountBookBrief {
    id: string;
    number: string;
    name: string;
}

export interface Project {
    proj_id: string;
    proj_name: string;
    kingdee_project_id?: string;
    kingdee_project?: KingdeeProject;
    default_receive_bank_id?: string;
    default_receive_bank?: BankAccountBrief;
    default_pay_bank_id?: string;
    default_pay_bank?: BankAccountBrief;
    kingdee_account_book_id?: string;
    kingdee_account_book?: KingdeeAccountBookBrief;
    created_at: string;
}

export interface Organization {
    id: number;
    name: string;
    code?: string;
    parent_id?: number;
    level: number;
    sort_order: number;
    status: number;
    description?: string;
    created_at: string;
    updated_at?: string;
    children?: Organization[];
}

export interface User {
    id: number;
    username: string;
    email?: string;
    phone?: string;
    real_name?: string;
    org_id?: number;
    org_name?: string;
    status: number;
    avatar?: string;
    last_login?: string;
    created_at: string;
    updated_at?: string;
    role?: 'admin' | 'user' | string;
    menu_keys?: string[];
    api_keys?: string[];
    account_book_ids?: string[];
    account_books?: { id: string; name: string }[];
}

export interface MenuPermissionMenuItem {
    key: string;
    label: string;
    section: string;
    group?: string | null;
    description?: string | null;
    admin_only: boolean;
    required: boolean;
    default_enabled: boolean;
}

export interface ApiPermissionItem {
    key: string;
    label: string;
    section: string;
    group?: string | null;
    description?: string | null;
    admin_only: boolean;
    default_enabled: boolean;
}

export interface MenuPermissionRoleState {
    role: string;
    label: string;
    description?: string | null;
    editable: boolean;
    menu_keys: string[];
    api_keys: string[];
}

export interface MenuPermissionOverview {
    menus: MenuPermissionMenuItem[];
    apis: ApiPermissionItem[];
    roles: MenuPermissionRoleState[];
}


export interface AccountingSubject {
    id: string;
    number: string;
    name: string;
    fullname: string;
    level: number;
    is_leaf: boolean;
    long_number: string;
    direction: string;
    is_active: boolean;
    is_cash: boolean;
    is_bank: boolean;
    is_cash_equivalent: boolean;
    check_items: string; // JSON string
    account_type_number?: string;
}

export interface Customer {
    id: string;
    number: string;
    name: string;
    status: string;
    enable: string;
    type: string;
    linkman?: string;
    bizpartner_phone?: string;
    bizpartner_address?: string;
    societycreditcode?: string;
    org_name?: string;
    createorg_name?: string;
    updated_at?: string;
}

export interface KingdeeHouse {
    id: string;
    number?: string;
    wtw8_number?: string;
    name: string;
    tzqslx?: string;
    splx?: string;
    createorg_name?: string;
    createorg_number?: string;
    created_at?: string;
    updated_at?: string;
}

export interface HouseUserItem {
    id: number;
    house_fk: number;
    origin_id?: number;
    item_id: number;
    name?: string;
    item_type?: number;
    licence?: string;
    park_name?: string;
    owner_name?: string;
    owner_phone?: string;
    charge_item_info?: string;
    start_time?: number;
    end_time?: number;
    community_name?: string;
    natural_period?: number;
    period_type?: number;
    period_num?: number;
    created_at: string;
}

export interface HouseParkBrief {
    id: number;
    park_id: string;
    name: string;
    park_type_name?: string;
    state?: number;
    user_name?: string;
    house_name?: string;
    house_id?: string;
    house_fk?: number;
    created_at: string;
}

export interface House {
    id: number;
    house_id: string;
    community_id: string;
    community_name?: string;
    house_name: string;
    building_id?: number;
    building_name?: string;
    unit_id?: number;
    unit_name?: string;
    layer?: number;
    building_size?: number;
    usable_size?: number;
    floor_name?: string;
    area?: number;
    user_num?: number;
    charge_num?: number;
    park_num?: number;
    car_num?: number;
    combina_name?: string;
    create_uid?: number;
    disable?: boolean;
    expand?: string;
    expand_info?: string;
    tag_list?: string;
    attachment_list?: string;
    house_type_name?: string;
    house_status_name?: string;
    user_list?: HouseUserItem[];
    park_list?: HouseParkBrief[];
    kingdee_house_id?: string;
    kingdee_house?: KingdeeHouse;
    created_at: string;
}

export interface Resident {
    id: number;
    resident_id: string;
    community_id: string;
    community_name?: string;
    name: string;
    phone?: string;
    houses?: string;
    labels?: string;
    kingdee_customer_id?: string;
    kingdee_customer?: Customer;
    created_at: string;
}

export interface Park {
    id: number;
    park_id: string;
    community_id: string;
    community_name?: string;
    name: string;
    park_type_name?: string;
    state?: number;
    user_name?: string;
    house_name?: string;
    house_id?: string;
    house_fk?: number;
    kingdee_house_id?: string;
    kingdee_house?: KingdeeHouse;
    created_at: string;
}

export interface SyncScheduleTargetMeta {
    code: string;
    label: string;
    system: 'mark' | 'kingdee' | string;
    requires_community_ids: boolean;
}

export interface SyncScheduleMeta {
    targets: SyncScheduleTargetMeta[];
    schedule_types: Array<{ value: 'interval' | 'daily' | 'weekly'; label: string }>;
    weekdays: Array<{ value: string; label: string }>;
    default_timezone: string;
}

export interface SyncSchedule {
    id: number;
    name: string;
    description?: string | null;
    target_codes: string[];
    community_ids: number[];
    account_book_number?: string | null;
    account_book_name?: string | null;
    schedule_type: 'interval' | 'daily' | 'weekly';
    interval_minutes?: number | null;
    daily_time?: string | null;
    weekly_days: string[];
    timezone: string;
    enabled: boolean;
    is_running: boolean;
    current_execution_id?: number | null;
    last_run_at?: string | null;
    last_status?: 'success' | 'failed' | 'partial' | 'running' | string | null;
    last_message?: string | null;
    next_run_at?: string | null;
    created_by?: number | null;
    updated_by?: number | null;
    created_at: string;
    updated_at?: string | null;
    creator_name?: string | null;
    updater_name?: string | null;
}

export interface SyncScheduleExecutionTargetResult {
    code: string;
    status: 'success' | 'failed' | string;
    message?: string;
    logs?: Array<{ type?: string; message?: string; time?: string }>;
    task_id?: string | null;
    traceback?: string;
}

export interface SyncScheduleExecution {
    id: number;
    schedule_id: number;
    schedule_name?: string;
    trigger_type: 'manual' | 'auto' | string;
    triggered_by?: number | null;
    triggered_by_name?: string | null;
    status: 'running' | 'success' | 'failed' | 'partial' | string;
    started_at: string;
    finished_at?: string | null;
    total_targets: number;
    success_targets: number;
    failed_targets: number;
    summary?: string | null;
    error_message?: string | null;
    result_payload: SyncScheduleExecutionTargetResult[];
    created_at: string;
    updated_at?: string | null;
}
