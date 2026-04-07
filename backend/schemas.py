# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field
from typing import Optional, List, Any, Literal, Dict
from datetime import date, datetime
from decimal import Decimal

# Project Schemas
class ProjectUpdate(BaseModel):
    kingdee_project_id: Optional[str] = None
    default_receive_bank_id: Optional[str] = None
    default_pay_bank_id: Optional[str] = None
    kingdee_account_book_id: Optional[str] = None

# OA Callback Schema
class OACallback(BaseModel):
    flow_id: str
    business_type: str
    applicant_id: str
    applicant_name: str
    department_code: str
    total_amount: Decimal
    approved_at: datetime
    form_data: dict

# Journal Response Schema
class CashJournalResponse(BaseModel):
    id: int
    flow_id: str
    amount: Decimal
    direction: Optional[str]
    status: str
    voucher_id: Optional[str]
    error_msg: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True

# Voucher Preview Schema
class VoucherEntry(BaseModel):
    line_no: int
    dr_cr: str
    account_code: str
    amount: Decimal
    summary: str
    aux_items: Optional[dict]

class VoucherPreview(BaseModel):
    entries: List[VoucherEntry]
    total_debit: Decimal
    total_credit: Decimal
    is_balanced: bool

class PushResult(BaseModel):
    success: bool
    voucher_id: Optional[str]
    message: Optional[str]


# Organization Schemas
class OrganizationBase(BaseModel):
    name: str
    code: Optional[str] = None
    parent_id: Optional[int] = None
    level: Optional[int] = 1
    sort_order: Optional[int] = 0
    status: Optional[int] = 1
    description: Optional[str] = None


class OrganizationCreate(OrganizationBase):
    pass


class OrganizationUpdate(BaseModel):
    name: Optional[str] = None
    code: Optional[str] = None
    parent_id: Optional[int] = None
    level: Optional[int] = None
    sort_order: Optional[int] = None
    status: Optional[int] = None
    description: Optional[str] = None


class OrganizationResponse(OrganizationBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class OrganizationTree(OrganizationResponse):
    children: List["OrganizationTree"] = []


# User Schemas
class UserBase(BaseModel):
    username: str
    email: Optional[str] = None
    phone: Optional[str] = None
    real_name: Optional[str] = None
    org_id: Optional[int] = None
    status: Optional[int] = 1
    role: Optional[str] = "user"


class UserCreate(UserBase):
    password: str
    account_book_ids: Optional[List[str]] = None


class UserUpdate(BaseModel):
    username: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    real_name: Optional[str] = None
    org_id: Optional[int] = None
    status: Optional[int] = None
    role: Optional[str] = None
    password: Optional[str] = None
    account_book_ids: Optional[List[str]] = None


class UserResponse(UserBase):
    id: int
    avatar: Optional[str] = None
    last_login: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    org_name: Optional[str] = None
    account_book_ids: Optional[List[str]] = None
    role: Optional[str] = "user"

    class Config:
        from_attributes = True


class UserTableColumnPreferenceUpdate(BaseModel):
    hidden: List[str] = Field(default_factory=list)
    order: List[str] = Field(default_factory=list)


class UserTableColumnPreferenceResponse(BaseModel):
    table_id: str
    hidden: List[str] = Field(default_factory=list)
    order: List[str] = Field(default_factory=list)
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class MenuPermissionMenuItem(BaseModel):
    key: str
    label: str
    section: str
    group: Optional[str] = None
    description: Optional[str] = None
    admin_only: bool = False
    required: bool = False
    default_enabled: bool = False


class ApiPermissionItem(BaseModel):
    key: str
    label: str
    section: str
    group: Optional[str] = None
    description: Optional[str] = None
    admin_only: bool = False
    default_enabled: bool = False


class MenuPermissionRoleState(BaseModel):
    role: str
    label: str
    description: Optional[str] = None
    editable: bool = True
    menu_keys: List[str] = Field(default_factory=list)
    api_keys: List[str] = Field(default_factory=list)


class MenuPermissionOverviewResponse(BaseModel):
    menus: List[MenuPermissionMenuItem] = Field(default_factory=list)
    apis: List[ApiPermissionItem] = Field(default_factory=list)
    roles: List[MenuPermissionRoleState] = Field(default_factory=list)


class MenuPermissionRoleUpdate(BaseModel):
    menu_keys: List[str] = Field(default_factory=list)
    api_keys: List[str] = Field(default_factory=list)


class SyncScheduleBase(BaseModel):
    name: str
    description: Optional[str] = None
    target_codes: List[str] = Field(default_factory=list)
    community_ids: List[int] = Field(default_factory=list)
    account_book_number: Optional[str] = None
    account_book_name: Optional[str] = None
    schedule_type: Literal["interval", "daily", "weekly"]
    interval_minutes: Optional[int] = Field(None, ge=5, le=10080)
    daily_time: Optional[str] = None
    weekly_days: List[str] = Field(default_factory=list)
    timezone: str = "Asia/Shanghai"
    enabled: bool = True


class SyncScheduleCreate(SyncScheduleBase):
    pass


class SyncScheduleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    target_codes: Optional[List[str]] = None
    community_ids: Optional[List[int]] = None
    account_book_number: Optional[str] = None
    account_book_name: Optional[str] = None
    schedule_type: Optional[Literal["interval", "daily", "weekly"]] = None
    interval_minutes: Optional[int] = Field(None, ge=5, le=10080)
    daily_time: Optional[str] = None
    weekly_days: Optional[List[str]] = None
    timezone: Optional[str] = None
    enabled: Optional[bool] = None


class SyncScheduleResponse(SyncScheduleBase):
    id: int
    is_running: bool
    current_execution_id: Optional[int] = None
    last_run_at: Optional[datetime] = None
    last_status: Optional[str] = None
    last_message: Optional[str] = None
    next_run_at: Optional[datetime] = None
    created_by: Optional[int] = None
    updated_by: Optional[int] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    creator_name: Optional[str] = None
    updater_name: Optional[str] = None


class SyncScheduleExecutionResponse(BaseModel):
    id: int
    schedule_id: int
    trigger_type: str
    triggered_by: Optional[int] = None
    triggered_by_name: Optional[str] = None
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    total_targets: int = 0
    success_targets: int = 0
    failed_targets: int = 0
    summary: Optional[str] = None
    error_message: Optional[str] = None
    result_payload: List[Dict[str, Any]] = Field(default_factory=list)
    created_at: datetime
    updated_at: Optional[datetime] = None


# Bill Sync Request Schema
class BillSyncRequest(BaseModel):
    community_ids: Optional[List[int]] = None


class ReceiptBillSyncRequest(BaseModel):
    community_ids: Optional[List[int]] = None


class DepositRecordSyncRequest(BaseModel):
    community_ids: Optional[List[int]] = None


class PrepaymentRecordSyncRequest(BaseModel):
    community_ids: Optional[List[int]] = None


class DepositRecordResponse(BaseModel):
    id: int
    community_id: Optional[int] = None
    community_name: Optional[str] = None
    house_id: Optional[int] = None
    house_name: Optional[str] = None
    amount: Optional[Decimal] = None
    operate_type: Optional[int] = None
    operator: Optional[int] = None
    operator_name: Optional[str] = None
    operate_time: Optional[int] = None
    operate_date: Optional[date] = None
    cash_pledge_name: Optional[str] = None
    remark: Optional[str] = None
    pay_time: Optional[int] = None
    pay_date: Optional[date] = None
    payment_id: Optional[int] = None
    has_refund_receipt: Optional[bool] = None
    refund_receipt_id: Optional[int] = None
    pay_channel_str: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class PrepaymentRecordResponse(BaseModel):
    id: int
    community_id: Optional[int] = None
    community_name: Optional[str] = None
    account_id: Optional[int] = None
    building_id: Optional[int] = None
    unit_id: Optional[int] = None
    house_id: Optional[int] = None
    house_name: Optional[str] = None
    amount: Optional[Decimal] = None
    balance_after_change: Optional[Decimal] = None
    operate_type: Optional[int] = None
    operate_type_label: Optional[str] = None
    pay_channel_id: Optional[int] = None
    pay_channel_str: Optional[str] = None
    operator: Optional[int] = None
    operator_name: Optional[str] = None
    operate_time: Optional[int] = None
    operate_date: Optional[date] = None
    source_updated_time: Optional[datetime] = None
    remark: Optional[str] = None
    deposit_order_id: Optional[int] = None
    pay_time: Optional[int] = None
    pay_date: Optional[date] = None
    category_id: Optional[int] = None
    category_name: Optional[str] = None
    status: Optional[int] = None
    payment_id: Optional[int] = None
    has_refund_receipt: Optional[bool] = None
    refund_receipt_id: Optional[int] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BillPreviewRef(BaseModel):
    bill_id: int
    community_id: int


class BatchVoucherPreviewRequest(BaseModel):
    bills: List[BillPreviewRef] = Field(default_factory=list)


class ReceiptBillPreviewRef(BaseModel):
    receipt_bill_id: int
    community_id: int


class BatchReceiptVoucherPreviewRequest(BaseModel):
    receipts: List[ReceiptBillPreviewRef] = Field(default_factory=list)


class VoucherPushRequest(BaseModel):
    kingdee_json: Dict[str, Any]
    api_id: Optional[int] = None
    bills: List[BillPreviewRef] = Field(default_factory=list)
    force_push: bool = False


class BillVoucherResetRequest(BaseModel):
    bills: List[BillPreviewRef] = Field(default_factory=list)
    reason: Optional[str] = None


class VoucherQueryRequest(BaseModel):
    voucher_id: str
    page_no: int = 1
    page_size: int = 10

# External Service Schemas
class ExternalServiceBase(BaseModel):
    service_name: str
    display_name: Optional[str] = None
    app_id: Optional[str] = None
    app_secret: Optional[str] = None
    auth_url: Optional[str] = None
    base_url: Optional[str] = None
    is_active: Optional[bool] = True
    auth_type: Optional[str] = "oauth2"
    auth_method: Optional[str] = "POST"
    auth_headers: Optional[str] = None
    auth_body: Optional[str] = None
    refresh_token: Optional[str] = None




class ExternalServiceCreate(ExternalServiceBase):
    pass

class ExternalServiceUpdate(BaseModel):
    display_name: Optional[str] = None
    app_id: Optional[str] = None
    app_secret: Optional[str] = None
    auth_url: Optional[str] = None
    base_url: Optional[str] = None
    is_active: Optional[bool] = None
    auth_type: Optional[str] = None
    auth_method: Optional[str] = None
    auth_headers: Optional[str] = None
    auth_body: Optional[str] = None
    refresh_token: Optional[str] = None




class ExternalServiceResponse(ExternalServiceBase):
    id: int
    expires_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

# External API Schemas
class ExternalApiBase(BaseModel):
    service_id: int
    name: str
    method: Optional[str] = "POST"
    url_path: str
    description: Optional[str] = None
    is_active: Optional[bool] = True
    request_headers: Optional[str] = None
    request_body: Optional[str] = None
    response_example: Optional[str] = None
    notes: Optional[str] = None
    category: Optional[str] = None

class ExternalApiCreate(ExternalApiBase):
    pass

class ExternalApiUpdate(BaseModel):
    name: Optional[str] = None
    method: Optional[str] = None
    url_path: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    request_headers: Optional[str] = None
    request_body: Optional[str] = None
    response_example: Optional[str] = None
    notes: Optional[str] = None
    category: Optional[str] = None

class ExternalApiResponse(ExternalApiBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class ExternalServiceWithApis(ExternalServiceResponse):
    apis: List[ExternalApiResponse] = []


class ReportingDbConnectionBase(BaseModel):
    name: str
    description: Optional[str] = None
    db_type: str = "sqlserver"
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: str
    schema_name: Optional[str] = None
    username: Optional[str] = None
    connection_options: Optional[str] = None
    is_active: Optional[bool] = True


class ReportingDbConnectionCreate(ReportingDbConnectionBase):
    password: Optional[str] = None


class ReportingDbConnectionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    db_type: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    connection_options: Optional[str] = None
    is_active: Optional[bool] = None


class ReportingDbConnectionResponse(ReportingDbConnectionBase):
    id: int
    has_password: bool = False
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ReportingDatasetBase(BaseModel):
    connection_id: int
    name: str
    description: Optional[str] = None
    sql_text: str
    params_json: Optional[str] = None
    row_limit: Optional[int] = Field(500, ge=1, le=5000)
    is_active: Optional[bool] = True


class ReportingDatasetCreate(ReportingDatasetBase):
    pass


class ReportingDatasetUpdate(BaseModel):
    connection_id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    sql_text: Optional[str] = None
    params_json: Optional[str] = None
    row_limit: Optional[int] = Field(None, ge=1, le=5000)
    is_active: Optional[bool] = None


class ReportingDatasetResponse(ReportingDatasetBase):
    id: int
    connection_name: Optional[str] = None
    last_columns_json: Optional[str] = None
    last_validated_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ReportingReportBase(BaseModel):
    dataset_id: int
    name: str
    description: Optional[str] = None
    report_type: str = "table"
    config_json: Optional[str] = None
    is_active: Optional[bool] = True


class ReportingReportCreate(ReportingReportBase):
    pass


class ReportingReportUpdate(BaseModel):
    dataset_id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    report_type: Optional[str] = None
    config_json: Optional[str] = None
    is_active: Optional[bool] = None


class ReportingReportResponse(ReportingReportBase):
    id: int
    dataset_name: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ReportingDatasetPreviewRequest(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict)
    limit: Optional[int] = Field(None, ge=1, le=5000)


class ReportingDatasetDraftPreviewRequest(BaseModel):
    connection_id: int
    sql_text: str
    params_json: Optional[str] = None
    row_limit: Optional[int] = Field(500, ge=1, le=5000)
    params: Dict[str, Any] = Field(default_factory=dict)
    limit: Optional[int] = Field(None, ge=1, le=5000)


class ReportingReportRunRequest(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict)
    limit: Optional[int] = Field(None, ge=1, le=5000)


# Global Variable Schemas
class GlobalVariableBase(BaseModel):
    key: str
    value: str
    description: Optional[str] = None
    category: Optional[str] = "common"
    is_secret: Optional[bool] = False

class GlobalVariableCreate(GlobalVariableBase):
    pass

class GlobalVariableUpdate(BaseModel):
    key: Optional[str] = None
    value: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    is_secret: Optional[bool] = None

class GlobalVariableResponse(GlobalVariableBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ExpressionFunctionResponse(BaseModel):
    key: str
    category: str
    description: str
    syntax: str
    example: str
    insert_text: str

# Voucher Template Schemas
class VoucherEntryRuleBase(BaseModel):
    line_no: int = Field(..., ge=1)
    dr_cr: Literal["D", "C"]
    account_code: str
    display_condition_expr: Optional[str] = None
    amount_expr: str
    summary_expr: str
    currency_expr: Optional[str] = "'CNY'"
    localrate_expr: Optional[str] = "1"
    aux_items: Optional[str] = None
    main_cf_assgrp: Optional[str] = None

class VoucherEntryRuleCreate(VoucherEntryRuleBase):
    pass

class VoucherEntryRuleResponse(VoucherEntryRuleBase):
    rule_id: int
    template_id: str

    class Config:
        from_attributes = True

class VoucherTemplateBase(BaseModel):
    template_id: str
    template_name: str
    business_type: str
    description: Optional[str] = None
    active: Optional[bool] = True
    priority: Optional[int] = Field(100, ge=0)
    category_id: Optional[int] = None
    source_module: Optional[str] = None
    source_type: Optional[str] = None
    trigger_condition: Optional[str] = None
    book_number_expr: Optional[str] = "'BU-35256'"
    vouchertype_number_expr: Optional[str] = "'0001'"
    attachment_expr: Optional[str] = "0"
    bizdate_expr: Optional[str] = "{CURRENT_DATE}"
    bookeddate_expr: Optional[str] = "{CURRENT_DATE}"

class VoucherTemplateCreate(VoucherTemplateBase):
    rules: List[VoucherEntryRuleCreate] = Field(default_factory=list)

class VoucherTemplateUpdate(BaseModel):
    template_name: Optional[str] = None
    business_type: Optional[str] = None
    description: Optional[str] = None
    active: Optional[bool] = None
    priority: Optional[int] = Field(None, ge=0)
    category_id: Optional[int] = None
    book_number_expr: Optional[str] = None
    vouchertype_number_expr: Optional[str] = None
    attachment_expr: Optional[str] = None
    bizdate_expr: Optional[str] = None
    bookeddate_expr: Optional[str] = None
    source_module: Optional[str] = None
    source_type: Optional[str] = None
    trigger_condition: Optional[str] = None
    rules: Optional[List[VoucherEntryRuleCreate]] = None

class VoucherTemplateResponse(VoucherTemplateBase):
    category_path: Optional[str] = None
    rules: List[VoucherEntryRuleResponse] = Field(default_factory=list)

    class Config:
        from_attributes = True


class VoucherTemplateCategoryBase(BaseModel):
    name: str
    parent_id: Optional[int] = None
    sort_order: Optional[int] = 0
    status: Optional[int] = 1
    description: Optional[str] = None


class VoucherTemplateCategoryCreate(VoucherTemplateCategoryBase):
    pass


class VoucherTemplateCategoryUpdate(BaseModel):
    name: Optional[str] = None
    parent_id: Optional[int] = None
    sort_order: Optional[int] = None
    status: Optional[int] = None
    description: Optional[str] = None


class VoucherTemplateCategoryResponse(VoucherTemplateCategoryBase):
    id: int
    path: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    children: List["VoucherTemplateCategoryResponse"] = []

    class Config:
        from_attributes = True


class AccountingSubjectBase(BaseModel):
    id: str
    number: str
    name: str
    fullname: Optional[str] = None
    level: Optional[int] = None
    is_leaf: Optional[bool] = None
    direction: Optional[str] = None
    is_active: Optional[bool] = True
    long_number: Optional[str] = None
    is_cash: Optional[bool] = False
    is_bank: Optional[bool] = False
    is_cash_equivalent: Optional[bool] = False
    account_type_number: Optional[str] = None
    acct_currency: Optional[str] = None
    
    ac_check: Optional[bool] = False
    is_qty: Optional[bool] = False
    currency_entry: Optional[str] = None
    
    check_items: Optional[str] = None
    raw_data: Optional[str] = None

class AccountingSubjectCreate(AccountingSubjectBase):
    pass

class AccountingSubjectResponse(AccountingSubjectBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class AccountingSubjectSyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedAccountingSubjectResponse(BaseModel):
    items: List[AccountingSubjectResponse]
    total: int


# Customer Schemas
class CustomerBase(BaseModel):
    id: str
    number: str
    name: str
    status: Optional[str] = None
    enable: Optional[str] = None
    type: Optional[str] = None
    linkman: Optional[str] = None
    bizpartner_phone: Optional[str] = None
    bizpartner_address: Optional[str] = None
    societycreditcode: Optional[str] = None
    org_name: Optional[str] = None
    createorg_name: Optional[str] = None
    
    entry_bank: Optional[str] = None
    entry_linkman: Optional[str] = None
    raw_data: Optional[str] = None

class CustomerResponse(CustomerBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class CustomerSyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedCustomerResponse(BaseModel):
    items: List[CustomerResponse]
    total: int

# Supplier Schemas
class SupplierBase(BaseModel):
    id: str
    number: str
    name: str
    status: Optional[str] = None
    enable: Optional[str] = None
    type: Optional[str] = None
    createorg_number: Optional[str] = None
    supplier_status_name: Optional[str] = None
    raw_data: Optional[str] = None

class SupplierResponse(SupplierBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class SupplierSyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedSupplierResponse(BaseModel):
    items: List[SupplierResponse]
    total: int

# TaxRate Schemas
class TaxRateBase(BaseModel):
    id: str
    number: str
    name: str
    enable: Optional[str] = None
    enable_title: Optional[str] = None
    status: Optional[str] = None
    source_created_time: Optional[str] = None
    source_modified_time: Optional[str] = None
    raw_data: Optional[str] = None

class TaxRateResponse(TaxRateBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class TaxRateSyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedTaxRateResponse(BaseModel):
    items: List[TaxRateResponse]
    total: int

# KingdeeHouse Schemas
class KingdeeHouseBase(BaseModel):
    id: str
    number: Optional[str] = None
    wtw8_number: Optional[str] = None
    name: str
    tzqslx: Optional[str] = None
    splx: Optional[str] = None
    createorg_name: Optional[str] = None
    createorg_number: Optional[str] = None
    raw_data: Optional[str] = None

class KingdeeHouseResponse(KingdeeHouseBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class KingdeeHouseSyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedKingdeeHouseResponse(BaseModel):
    items: List[KingdeeHouseResponse]
    total: int

# KingdeeAccountBook Schemas
class KingdeeAccountBookBase(BaseModel):
    id: str
    number: Optional[str] = None
    name: str
    org_number: Optional[str] = None
    org_name: Optional[str] = None
    accountingsys_number: Optional[str] = None
    accountingsys_name: Optional[str] = None
    booknature: Optional[str] = None
    accounttable_name: Optional[str] = None
    basecurrency_name: Optional[str] = None
    status: Optional[str] = None
    enable: Optional[str] = None
    raw_data: Optional[str] = None

class KingdeeAccountBookResponse(KingdeeAccountBookBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class KingdeeAccountBookSyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedKingdeeAccountBookResponse(BaseModel):
    items: List[KingdeeAccountBookResponse]
    total: int

# House Schemas
class HouseUserItemResponse(BaseModel):
    id: int
    house_fk: int
    origin_id: Optional[int] = None
    item_id: int
    name: Optional[str] = None
    item_type: Optional[int] = None
    licence: Optional[str] = None
    park_name: Optional[str] = None
    owner_name: Optional[str] = None
    owner_phone: Optional[str] = None
    charge_item_info: Optional[str] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    community_name: Optional[str] = None
    natural_period: Optional[int] = None
    period_type: Optional[int] = None
    period_num: Optional[int] = None
    created_at: datetime

    class Config:
        from_attributes = True


class HouseParkBriefResponse(BaseModel):
    """房屋下的车位列表（从 parks 表反查）"""

    id: int
    park_id: str
    name: str
    park_type_name: Optional[str] = None
    state: Optional[int] = None
    user_name: Optional[str] = None
    house_name: Optional[str] = None
    house_id: Optional[str] = None
    house_fk: Optional[int] = None
    created_at: datetime

    class Config:
        from_attributes = True


class HouseResponse(BaseModel):
    id: int
    house_id: str
    community_id: str
    community_name: Optional[str] = None
    house_name: str
    building_id: Optional[int] = None
    building_name: Optional[str] = None
    unit_id: Optional[int] = None
    unit_name: Optional[str] = None
    layer: Optional[int] = None
    building_size: Optional[Decimal] = None
    usable_size: Optional[Decimal] = None
    floor_name: Optional[str] = None
    area: Optional[Decimal] = None
    user_num: Optional[int] = None
    charge_num: Optional[int] = None
    park_num: Optional[int] = None
    car_num: Optional[int] = None
    combina_name: Optional[str] = None
    create_uid: Optional[int] = None
    disable: Optional[bool] = None
    expand: Optional[str] = None
    expand_info: Optional[str] = None
    tag_list: Optional[str] = None
    attachment_list: Optional[str] = None
    house_type_name: Optional[str] = None
    house_status_name: Optional[str] = None
    user_list: Optional[List[HouseUserItemResponse]] = None
    park_list: Optional[List[HouseParkBriefResponse]] = None
    kingdee_house_id: Optional[str] = None
    kingdee_house: Optional[KingdeeHouseResponse] = None
    created_at: datetime

    class Config:
        from_attributes = True

class HouseUpdate(BaseModel):
    kingdee_house_id: Optional[str] = None

class HouseSyncRequest(BaseModel):
    community_ids: List[Any]

class ResidentResponse(BaseModel):
    id: int
    resident_id: str
    community_id: str
    community_name: Optional[str] = None
    name: str
    phone: Optional[str] = None
    houses: Optional[str] = None
    labels: Optional[str] = None
    kingdee_customer_id: Optional[str] = None
    kingdee_customer: Optional[CustomerResponse] = None
    created_at: datetime

    class Config:
        from_attributes = True

class ResidentUpdate(BaseModel):
    kingdee_customer_id: Optional[str] = None

class ResidentSyncRequest(BaseModel):
    community_ids: List[Any]

class ParkResponse(BaseModel):
    id: int
    park_id: str
    community_id: str
    community_name: Optional[str] = None
    name: str
    park_type_name: Optional[str] = None
    state: Optional[int] = None
    user_name: Optional[str] = None
    house_name: Optional[str] = None
    house_id: Optional[str] = None
    house_fk: Optional[int] = None
    kingdee_house_id: Optional[str] = None
    kingdee_house: Optional[KingdeeHouseResponse] = None
    created_at: datetime

    class Config:
        from_attributes = True

class ParkUpdate(BaseModel):
    kingdee_house_id: Optional[str] = None

class ParkSyncRequest(BaseModel):
    community_ids: List[Any]


# AuxiliaryData Schemas
class AuxiliaryDataBase(BaseModel):
    id: str
    number: str
    name: str
    issyspreset: Optional[bool] = None
    ctrlstrategy: Optional[str] = None
    enable: Optional[str] = None
    group_number: Optional[str] = None
    group_name: Optional[str] = None
    parent_number: Optional[str] = None
    parent_name: Optional[str] = None
    createorg_number: Optional[str] = None
    createorg_name: Optional[str] = None
    raw_data: Optional[str] = None

class AuxiliaryDataResponse(AuxiliaryDataBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class AuxiliaryDataSyncRequest(BaseModel):
    override_config: Optional[dict] = None
    categories: Optional[List[str]] = None

class PaginatedAuxiliaryDataResponse(BaseModel):
    items: List[AuxiliaryDataResponse]
    total: int

# AuxiliaryDataCategory Schemas
class AuxiliaryDataCategoryBase(BaseModel):
    id: str
    number: str
    name: str
    fissyspreset: Optional[bool] = None
    description: Optional[str] = None
    ctrlstrategy: Optional[str] = None
    createorg_name: Optional[str] = None
    createorg_number: Optional[str] = None
    createorg_id: Optional[str] = None
    raw_data: Optional[str] = None

class AuxiliaryDataCategoryResponse(AuxiliaryDataCategoryBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class AuxiliaryDataCategorySyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedAuxiliaryDataCategoryResponse(BaseModel):
    items: List[AuxiliaryDataCategoryResponse]
    total: int

# ChargeItem Schemas
class ChargeItemBase(BaseModel):
    item_id: int
    communityid: str
    item_name: str
    charge_type: Optional[int] = None
    charge_type_str: Optional[str] = None
    category_id: Optional[int] = None
    category_name: Optional[str] = None
    period_type_str: Optional[str] = None
    remark: Optional[str] = None
    kingdee_tax_rate_id: Optional[str] = None

class ChargeItemResponse(ChargeItemBase):
    created_at: datetime
    kingdee_tax_rate: Optional[TaxRateResponse] = None

    class Config:
        from_attributes = True

class ChargeItemUpdate(BaseModel):
    kingdee_tax_rate_id: Optional[str] = None

# KingdeeBankAccount Schemas
class KingdeeBankAccountBase(BaseModel):
    id: str
    bankaccountnumber: Optional[str] = None
    name: Optional[str] = None
    acctname: Optional[str] = None
    company_number: Optional[str] = None
    company_name: Optional[str] = None
    openorg_number: Optional[str] = None
    openorg_name: Optional[str] = None
    defaultcurrency_number: Optional[str] = None
    defaultcurrency_name: Optional[str] = None
    accttype: Optional[str] = None
    acctstyle: Optional[str] = None
    finorgtype: Optional[str] = None
    banktype_number: Optional[str] = None
    banktype_name: Optional[str] = None
    bank_number: Optional[str] = None
    bank_name: Optional[str] = None
    acctproperty_number: Optional[str] = None
    acctproperty_name: Optional[str] = None
    status: Optional[str] = None
    acctstatus: Optional[str] = None
    isdefaultrec: Optional[bool] = False
    isdefaultpay: Optional[bool] = False
    comment: Optional[str] = None
    raw_data: Optional[str] = None

class KingdeeBankAccountResponse(KingdeeBankAccountBase):
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class KingdeeBankAccountSyncRequest(BaseModel):
    override_config: Optional[dict] = None

class PaginatedKingdeeBankAccountResponse(BaseModel):
    items: List[KingdeeBankAccountResponse]
    total: int

class MarkiConfigRequest(BaseModel):
    app_id: str
    app_secret: str
