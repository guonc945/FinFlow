import { useState, useEffect, useRef, forwardRef, useImperativeHandle } from 'react';
import {
    Layers, FileText, Settings, Hash, Info, X, Sliders, ArrowUp, ArrowDown, AlertTriangle,
    Plus, Save, Trash2, ChevronLeft, Database, Copy
} from 'lucide-react';
import axios from 'axios';
import VariablePicker from '../settings/VariablePicker';
import ConditionBuilder from './ConditionBuilder';
import AccountSelector from './AccountSelector';
import type { AccountingSubject, VoucherFieldModule, VoucherSourceFieldOption } from '../../types';
import SourceFieldPickerModal from './SourceFieldPickerModal';
import './VoucherTemplates.css';

import { API_BASE_URL } from '../../services/apiBase';

const API_BASE = API_BASE_URL;

type SourceFieldOption = {
    label: string;
    value: string;
    group?: string;
};

const buildDefaultVoucherFieldModules = (billsFields: SourceFieldOption[], receiptBillFields: SourceFieldOption[]): VoucherFieldModule[] => {
    return [
        {
            id: 'marki',
            label: '马克系统',
            sources: [
                {
                    id: 'bills',
                    label: '运营账单',
                    source_type: 'bills',
                    fields: billsFields as unknown as VoucherSourceFieldOption[],
                }
                ,
                {
                    id: 'receipt_bills',
                    label: '收款账单',
                    source_type: 'receipt_bills',
                    fields: receiptBillFields as unknown as VoucherSourceFieldOption[],
                }
            ]
        },
        {
            id: 'oa',
            label: 'OA系统',
            note: '暂未接入，缺省处理',
            sources: [
                {
                    id: 'oa_default',
                    label: '缺省',
                    source_type: 'oa',
                    fields: [],
                }
            ]
        }
    ];
};



const FALLBACK_BILL_SOURCE_FIELDS: SourceFieldOption[] = [
    // 金额信息
    { label: '账单金额 (amount)', value: 'amount', group: '金额信息' },
    { label: '应付金额 (bill_amount)', value: 'bill_amount', group: '金额信息' },
    { label: '折扣金额 (discount_amount)', value: 'discount_amount', group: '金额信息' },
    { label: '滞纳金 (late_money_amount)', value: 'late_money_amount', group: '金额信息' },
    { label: '押金 (deposit_amount)', value: 'deposit_amount', group: '金额信息' },

    // 基础信息
    { label: '收费项目名称 (charge_item_name)', value: 'charge_item_name', group: '基础信息' },
    { label: '收费项目分类 (category_name)', value: 'category_name', group: '基础信息' },
    { label: '资产名称 (asset_name)', value: 'asset_name', group: '基础信息' },
    { label: '资产类型 (asset_type_str)', value: 'asset_type_str', group: '基础信息' },
    { label: '房屋全名 (full_house_name)', value: 'full_house_name', group: '基础信息' },
    { label: '车位名称 (park_name)', value: 'park_name', group: '基础信息' },

    // 支付与状态
    { label: '所属月份 (in_month)', value: 'in_month', group: '支付与状态' },
    { label: '支付状态 (pay_status_str)', value: 'pay_status_str', group: '支付与状态' },
    { label: '支付方式 (pay_type_str)', value: 'pay_type_str', group: '支付与状态' },
    { label: '账单类型 (bill_type_str)', value: 'bill_type_str', group: '支付与状态' },
    { label: '收据号 (receipt_id)', value: 'receipt_id', group: '支付与状态' },

    // 其他
    { label: '备注 (remark)', value: 'remark', group: '其他' },
    { label: '子商户名称 (sub_mch_name)', value: 'sub_mch_name', group: '其他' },
    { label: '账单ID (id)', value: 'id', group: '其他' },

    // 金蝶关联字段（通过已建立的档案映射自动取值）
    { label: '金蝶房号编码 (kd_house_number)', value: 'kd_house_number', group: '金蝶关联' },
    { label: '金蝶房号名称 (kd_house_name)', value: 'kd_house_name', group: '金蝶关联' },
    { label: '车位映射房号编码 (kd_park_house_number)', value: 'kd_park_house_number', group: '金蝶关联' },
    { label: '车位映射房号名称 (kd_park_house_name)', value: 'kd_park_house_name', group: '金蝶关联' },
    { label: '金蝶客户编码 (kd_customer_number)', value: 'kd_customer_number', group: '金蝶关联' },
    { label: '金蝶客户名称 (kd_customer_name)', value: 'kd_customer_name', group: '金蝶关联' },
    { label: '金蝶项目编码 (kd_project_number)', value: 'kd_project_number', group: '金蝶关联' },
    { label: '金蝶项目名称 (kd_project_name)', value: 'kd_project_name', group: '金蝶关联' },

    // 自动解析的银行账户
    { label: '收款银行账户编码 (kd_receive_bank_number)', value: 'kd_receive_bank_number', group: '银行账户' },
    { label: '收款银行账户名称 (kd_receive_bank_name)', value: 'kd_receive_bank_name', group: '银行账户' },
    { label: '付款银行账户编码 (kd_pay_bank_number)', value: 'kd_pay_bank_number', group: '银行账户' },
    { label: '付款银行账户名称 (kd_pay_bank_name)', value: 'kd_pay_bank_name', group: '银行账户' },
];

const FALLBACK_RECEIPT_BILL_SOURCE_FIELDS: SourceFieldOption[] = [
    { label: '收款明细ID (id)', value: 'id', group: '关联ID' },
    { label: '园区ID (community_id)', value: 'community_id', group: '关联ID' },
    { label: '园区名称 (community_name)', value: 'community_name', group: '运行时字段' },
    { label: '付款人 (payer_name)', value: 'payer_name', group: '运行时字段' },
    { label: '收款人 (payee)', value: 'payee', group: '基础信息' },
    { label: '资产名称 (asset_name)', value: 'asset_name', group: '资产信息' },
    { label: '实收金额 (income_amount)', value: 'income_amount', group: '金额信息' },
    { label: '收款金额 (amount)', value: 'amount', group: '金额信息' },
    { label: '账单金额 (bill_amount)', value: 'bill_amount', group: '金额信息' },
    { label: '折扣金额 (discount_amount)', value: 'discount_amount', group: '金额信息' },
    { label: '滞纳金 (late_money_amount)', value: 'late_money_amount', group: '金额信息' },
    { label: '押金 (deposit_amount)', value: 'deposit_amount', group: '金额信息' },
    { label: '支付方式 (pay_channel_str)', value: 'pay_channel_str', group: '支付信息' },
    { label: '收据号 (receipt_id)', value: 'receipt_id', group: '支付信息' },
    { label: '交易时间 (deal_time)', value: 'deal_time', group: '交易时间' },
    { label: '交易日期 (deal_date)', value: 'deal_date', group: '交易时间' },
];



// Helpers moved outside component to be stable
const parseJsonToRows = (jsonStr: string | null | undefined) => {
    if (!jsonStr) return [];
    try {
        const obj = JSON.parse(jsonStr);
        return Object.entries(obj).map(([key, val]) => {
            const valObj = val as any;
            const innerEntries = Object.entries(valObj);
            if (innerEntries.length > 0) {
                return { key, prop: innerEntries[0][0], value: String(innerEntries[0][1]) };
            }
            return { key, prop: 'number', value: '' };
        });
    } catch {
        return [];
    }
};

const rowsToJson = (rows: Array<{ key: string, prop: string, value: string }>) => {
    const obj: Record<string, any> = {};
    rows.forEach(row => {
        if (row.key.trim()) {
            obj[row.key.trim()] = { [row.prop.trim() || 'number']: row.value.trim() };
        }
    });
    return JSON.stringify(obj, null, 2);
};



const extractSaveErrorMessages = (err: any): string[] => {
    const detail = err?.response?.data?.detail;

    if (detail && typeof detail === 'object') {
        if (Array.isArray(detail.errors)) {
            const parsed = detail.errors
                .map((m: any) => String(m || '').trim())
                .filter((m: string) => m.length > 0);
            if (parsed.length > 0) return parsed;
        }
        if (typeof detail.message === 'string' && detail.message.trim()) {
            return [detail.message.trim()];
        }
    }

    if (Array.isArray(detail)) {
        const parsed = detail
            .map((m: any) => String(m || '').trim())
            .filter((m: string) => m.length > 0);
        if (parsed.length > 0) return parsed;
    }

    if (typeof detail === 'string' && detail.trim()) {
        return [detail.trim()];
    }

    if (typeof err?.message === 'string' && err.message.trim()) {
        return [err.message.trim()];
    }

    return ['保存失败，请稍后重试'];
};

const normalizeSourceFields = (raw: any): SourceFieldOption[] => {
    if (!Array.isArray(raw)) return [];
    const normalized: SourceFieldOption[] = [];
    raw.forEach((item: any) => {
        if (typeof item === 'string') {
            const v = item.trim();
            if (v) normalized.push({ label: v, value: v, group: '账单字段' });
            return;
        }
        const value = String(item?.value ?? item?.field ?? '').trim();
        if (!value) return;
        const label = String(item?.label ?? value).trim() || value;
        const group = String(item?.group ?? '').trim() || '账单字段';
        normalized.push({ label, value, group });
    });
    return normalized;
};

const mergeSourceFields = (fallback: SourceFieldOption[], dynamic: SourceFieldOption[]): SourceFieldOption[] => {
    const map = new Map<string, SourceFieldOption>();
    fallback.forEach(item => map.set(item.value, item));
    dynamic.forEach(item => {
        if (map.has(item.value)) return;
        map.set(item.value, item);
    });
    return Array.from(map.values());
};



const ExpressionInput = forwardRef<
    { insert: (text: string) => void },
    { value: string, onChange: (val: string) => void, onFocus?: (ins: (text: string) => void) => void, placeholder?: string, className?: string }
>(({ value, onChange, onFocus, placeholder, className }, ref) => {
    const inputRef = useRef<HTMLInputElement>(null);
    const onChangeRef = useRef(onChange);

    useEffect(() => {
        onChangeRef.current = onChange;
    });

    const insert = (text: string) => {
        const input = inputRef.current;
        if (input) {
            const start = input.selectionStart || 0;
            const end = input.selectionEnd || 0;
            const oldVal = input.value;
            const newVal = oldVal.substring(0, start) + text + oldVal.substring(end);
            onChangeRef.current(newVal);
            setTimeout(() => {
                input.focus();
                input.setSelectionRange(start + text.length, start + text.length);
            }, 0);
        } else {
            onChangeRef.current(value + text);
        }
    };

    useImperativeHandle(ref, () => ({
        insert
    }));

    return (
        <input
            ref={inputRef}
            type="text"
            value={value}
            onChange={e => onChange(e.target.value)}
            onFocus={() => onFocus?.(insert)}
            placeholder={placeholder}
            className={className}
        />
    );
});

const ExpressionInputWithActions = ({
    value,
    onChange,
    onGlobalFocus,
    onOpenPicker,
    fieldModules,
    useBraces = true,
    size = 'normal',
    placeholder,
    className
}: {
    value: string,
    onChange: (val: string) => void,
    onGlobalFocus: (ins: (text: string) => void) => void,
    onOpenPicker: () => void,
    fieldModules?: VoucherFieldModule[] | null,
    useBraces?: boolean,
    size?: 'normal' | 'mini',
    placeholder?: string,
    className?: string
}) => {
    const expRef = useRef<{ insert: (t: string) => void }>(null);
    const [fieldPickerOpen, setFieldPickerOpen] = useState(false);

    return (
        <div className={`expression-input-group ${size === 'mini' ? 'mini' : ''}`}>
            <div className={`input-with-action ${size === 'mini' ? 'mini' : ''}`}>
                <ExpressionInput
                    ref={expRef}
                    value={value}
                    onChange={onChange}
                    onFocus={onGlobalFocus}
                    placeholder={placeholder}
                    className={className}
                />
                <button onClick={() => {
                    if (expRef.current) onGlobalFocus(expRef.current.insert);
                    onOpenPicker();
                }}>
                    <Hash size={size === 'mini' ? 12 : 14} />
                </button>
                {fieldModules && fieldModules.length > 0 && (
                    <div className="source-field-combo" title="选择数据源字段">
                        <button
                            type="button"
                            className="source-field-trigger"
                            onClick={() => setFieldPickerOpen(true)}
                            title="选择数据源字段"
                        >
                            <Database size={size === 'mini' ? 12 : 14} />
                        </button>
                        <SourceFieldPickerModal
                            open={fieldPickerOpen}
                            onClose={() => setFieldPickerOpen(false)}
                            modules={fieldModules}
                            onPick={(f, ctx) => {
                                const key = (ctx?.module_id && ctx?.source_id)
                                    ? `${ctx.module_id}.${ctx.source_id}.${f.value}`
                                    : (ctx?.source_type ? `${ctx.source_type}.${f.value}` : f.value);
                                const text = useBraces ? `{${key}}` : key;
                                if (expRef.current) {
                                    expRef.current.insert(text);
                                    onGlobalFocus(expRef.current.insert);
                                } else {
                                    onChange(value + text);
                                }
                                setFieldPickerOpen(false);
                            }}
                        />
                    </div>
                )}
            </div>
        </div>
    );
};

const DimensionFormEditor = ({
    value,
    onChange,
    onFocusField,
    onOpenPicker,
    fieldModules,
    requiredKeys
}: {
    value: string | null | undefined,
    onChange: (json: string) => void,
    onFocusField: (insert: (text: string) => void) => void,
    onOpenPicker: () => void,
    fieldModules?: VoucherFieldModule[] | null,
    requiredKeys?: string[]
}) => {
    const [rows, setRows] = useState(parseJsonToRows(value));

    useEffect(() => {
        const newRowsFromProps = parseJsonToRows(value);
        const currentJson = rowsToJson(rows);
        const newJson = rowsToJson(newRowsFromProps);
        if (currentJson !== newJson) {
            setRows(newRowsFromProps);
        }
    }, [value]);

    const handleRowChange = (idx: number, field: 'key' | 'value' | 'prop', val: string) => {
        const newRows = [...rows];
        newRows[idx] = { ...newRows[idx], [field]: val };
        setRows(newRows);
        onChange(rowsToJson(newRows));
    };

    const addRow = () => {
        const newRows = [...rows, { key: '', prop: 'number', value: '' }];
        setRows(newRows);
    };

    const removeRow = (idx: number) => {
        const newRows = rows.filter((_, i) => i !== idx);
        setRows(newRows);
        onChange(rowsToJson(newRows));
    };

    return (
        <div className="dimension-form">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <button onClick={addRow} className="add-dim-btn" style={{ width: 'auto', padding: '0.4rem 0.8rem' }}>
                    <Plus size={14} /> 添加维度行
                </button>
                <span style={{ fontSize: '0.75rem', color: '#94a3b8' }}>{rows.length} 个配置项</span>
            </div>
            <table className="dimension-table">
                <thead>
                    <tr>
                        <th style={{ width: '30%' }}>核算维度</th>
                        <th style={{ width: '20%' }}>属性</th>
                        <th style={{ width: '40%' }}>值</th>
                        <th style={{ width: '10%' }}></th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row, idx) => (
                        <tr key={idx}>
                            <td>
                                <input
                                    className="dim-input"
                                    placeholder="例如：客户"
                                    value={row.key}
                                    onChange={e => handleRowChange(idx, 'key', e.target.value)}
                                />
                            </td>
                            <td>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                    <input
                                        className="dim-input"
                                        placeholder="number"
                                        value={row.prop}
                                        onChange={e => handleRowChange(idx, 'prop', e.target.value)}
                                    />
                                    {requiredKeys && requiredKeys.includes(row.key) && (
                                        <span style={{ color: 'red', fontWeight: 'bold', fontSize: '1.2em' }} title="此维度为必填项">*</span>
                                    )}
                                </div>
                            </td>
                            <td>
                                <ExpressionInputWithActions
                                    size="mini"
                                    placeholder="{variable}"
                                    value={row.value}
                                    onChange={val => handleRowChange(idx, 'value', val)}
                                    onGlobalFocus={onFocusField}
                                    onOpenPicker={onOpenPicker}
                                    fieldModules={fieldModules || buildDefaultVoucherFieldModules(FALLBACK_BILL_SOURCE_FIELDS, FALLBACK_RECEIPT_BILL_SOURCE_FIELDS)}
                                />
                            </td>
                            <td>
                                <button onClick={() => removeRow(idx)} className="delete-dim-btn">
                                    <Trash2 size={14} />
                                </button>
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};

interface VoucherEntryRule {
    rule_id?: number | null;
    line_no: number;
    dr_cr: 'D' | 'C';
    account_code: string;
    amount_expr: string;
    summary_expr: string;
    currency_expr: string;
    localrate_expr: string;
    aux_items?: string | null;
    main_cf_assgrp?: string | null;
}

interface VoucherTemplate {
    template_id: string;
    template_name: string;
    business_type: string;
    description: string;
    active: boolean;
    priority: number;
    source_module?: string;
    source_type?: string;
    trigger_condition?: string; // JSON string
    book_number_expr: string;
    vouchertype_number_expr: string;
    attachment_expr: string;
    bizdate_expr: string;
    bookeddate_expr: string;
    rules: VoucherEntryRule[];
}

const getUniqueCopiedTemplateId = (sourceId: string, templates: VoucherTemplate[]) => {
    const existingIds = new Set(
        templates.map(t => (t.template_id || '').trim().toLowerCase()).filter(Boolean)
    );
    const baseId = (sourceId || 'template').trim().replace(/\s+/g, '_');
    const copyBase = `${baseId}_copy`;

    if (!existingIds.has(copyBase.toLowerCase())) return copyBase;

    let index = 2;
    while (existingIds.has(`${copyBase}_${index}`.toLowerCase())) {
        index += 1;
    }
    return `${copyBase}_${index}`;
};

const filterModulesBySourceType = (modules: VoucherFieldModule[], sourceType: string | null | undefined) => {
    const st = String(sourceType || '').trim().toLowerCase();
    if (!st) return modules;
    const next: VoucherFieldModule[] = [];
    modules.forEach(m => {
        const sources = (m.sources || []).filter(s => String(s?.source_type || '').toLowerCase() === st);
        if (sources.length > 0) next.push({ ...m, sources });
    });
    return next.length > 0 ? next : modules;
};

const filterModulesByModuleId = (modules: VoucherFieldModule[], moduleId: string | null | undefined) => {
    const mid = String(moduleId || '').trim();
    if (!mid) return modules;
    const found = modules.find(m => String(m?.id) === mid);
    return found ? [found] : modules;
};

const inferModuleIdFromSourceType = (modules: VoucherFieldModule[], sourceType: string | null | undefined) => {
    const st = String(sourceType || '').trim().toLowerCase();
    if (!st) return 'marki';
    for (const m of modules || []) {
        for (const s of m.sources || []) {
            if (String(s?.source_type || '').trim().toLowerCase() === st) return String(m.id);
        }
    }
    return 'marki';
};

// Empty/NULL source_type is treated as bills by backend matching & validation.
const getEffectiveSourceType = (sourceType: string | null | undefined) => {
    const st = String(sourceType || '').trim();
    return st ? st : 'bills';
};

const getUniqueCopiedTemplateName = (sourceName: string, templates: VoucherTemplate[]) => {
    const existingNames = new Set(
        templates.map(t => (t.template_name || '').trim()).filter(Boolean)
    );
    const baseName = (sourceName || '未命名模板').trim();
    const copyBase = `${baseName}（副本）`;

    if (!existingNames.has(copyBase)) return copyBase;

    let index = 2;
    while (existingNames.has(`${baseName}（副本${index}）`)) {
        index += 1;
    }
    return `${baseName}（副本${index}）`;
};



const VoucherTemplates = () => {
    const [templates, setTemplates] = useState<VoucherTemplate[]>([]);
    const [isEditing, setIsEditing] = useState(false);
    const [currentTemplate, setCurrentTemplate] = useState<VoucherTemplate | null>(null);
    const [editingTemplateId, setEditingTemplateId] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);

    // Variable Picker State
    const [isVariablePickerOpen, setIsVariablePickerOpen] = useState(false);
    const [activeTab, setActiveTab] = useState<'basic' | 'condition' | 'subject' | 'rules'>('basic');
    const [lastFocusedField, setLastFocusedField] = useState<{
        insert: (text: string) => void
    } | null>(null);

    // Detail Modal State
    const [detailModalOpen, setDetailModalOpen] = useState(false);
    const [currentRuleIndex, setCurrentRuleIndex] = useState<number | null>(null);
    const [detailTab, setDetailTab] = useState<'assgrp' | 'maincf'>('assgrp');

    const [subjects, setSubjects] = useState<AccountingSubject[]>([]);
    const [saveErrors, setSaveErrors] = useState<string[]>([]);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [billSourceFields, setBillSourceFields] = useState<SourceFieldOption[]>(FALLBACK_BILL_SOURCE_FIELDS);
    const [receiptBillSourceFields, setReceiptBillSourceFields] = useState<SourceFieldOption[]>(FALLBACK_RECEIPT_BILL_SOURCE_FIELDS);
    const [voucherFieldModules, setVoucherFieldModules] = useState<VoucherFieldModule[]>(
        buildDefaultVoucherFieldModules(FALLBACK_BILL_SOURCE_FIELDS, FALLBACK_RECEIPT_BILL_SOURCE_FIELDS)
    );

    useEffect(() => {
        fetchSubjects();
        fetchTemplates();
        fetchVoucherFieldModules();
    }, []);

    const fetchSubjects = async () => {
        try {
            const res = await axios.get(`${API_BASE}/finance/accounting-subjects`, {
                params: {
                    skip: 0,
                    limit: 5000
                }
            });
            if (res.data && Array.isArray(res.data.items)) {
                setSubjects(res.data.items);
            }
        } catch (err) {
            console.error('Failed to fetch subjects:', err);
        }
    };

    const fetchTemplates = async () => {
        setIsLoading(true);
        setLoadError(null);
        try {
            const res = await axios.get(`${API_BASE}/vouchers/templates`);
            const normalized = (res.data || []).map((t: any) => ({
                template_id: t.template_id || '',
                template_name: t.template_name || '',
                business_type: t.business_type || '',
                description: t.description || '',
                active: t.active !== false,
                priority: Number.isFinite(Number(t.priority)) ? Number(t.priority) : 100,
                source_module: (t?.source_module || '').trim() || inferModuleIdFromSourceType(voucherFieldModules, t.source_type),
                source_type: (() => {
                    const raw = String(t?.source_type || '').trim();
                    if (raw) return raw;
                    const mid = (String(t?.source_module || '').trim() || inferModuleIdFromSourceType(voucherFieldModules, t.source_type));
                    const mod = voucherFieldModules.find(m => String(m?.id) === mid) || voucherFieldModules[0];
                    const fallback = String(mod?.sources?.[0]?.source_type || '').trim();
                    return fallback || 'bills';
                })(),
                trigger_condition: t.trigger_condition || '',
                book_number_expr: t.book_number_expr || "'BU-35256'",
                vouchertype_number_expr: t.vouchertype_number_expr || "'0001'",
                attachment_expr: t.attachment_expr || "0",
                bizdate_expr: t.bizdate_expr || "{CURRENT_DATE}",
                bookeddate_expr: t.bookeddate_expr || "{CURRENT_DATE}",
                rules: Array.isArray(t.rules) ? t.rules : []
            })) as VoucherTemplate[];
            setTemplates(normalized);
        } catch (err: any) {
            console.error('Failed to fetch templates:', err);
            const detail = err?.response?.data?.detail;
            const message = typeof detail === 'string'
                ? detail
                : (detail?.message || err?.message || '无法加载模板列表');
            setLoadError(String(message));
            setTemplates([]);
        } finally {
            setIsLoading(false);
        }
    };

    const fetchVoucherFieldModules = async () => {
        try {
            const res = await axios.get(`${API_BASE}/vouchers/source-modules`);
            const modules = res?.data?.modules as VoucherFieldModule[] | undefined;
            if (Array.isArray(modules) && modules.length > 0) {
                setVoucherFieldModules(modules);

                // Keep the legacy flat list in sync for existing logic (filters/labels etc).
                const marki = modules.find(m => String(m?.id) === 'marki') || modules[0];
                const billsSource = (marki?.sources || []).find(s => String(s?.id) === 'bills' || String(s?.source_type) === 'bills');
                const receiptBillsSource = (marki?.sources || []).find(s => String(s?.id) === 'receipt_bills' || String(s?.source_type) === 'receipt_bills');

                const billsFields = normalizeSourceFields(billsSource?.fields ?? []);
                if (billsFields.length > 0) {
                    setBillSourceFields(mergeSourceFields(FALLBACK_BILL_SOURCE_FIELDS, billsFields));
                }

                const receiptFields = normalizeSourceFields(receiptBillsSource?.fields ?? []);
                if (receiptFields.length > 0) {
                    setReceiptBillSourceFields(mergeSourceFields(FALLBACK_RECEIPT_BILL_SOURCE_FIELDS, receiptFields));
                }
                return;
            }
        } catch (err) {
            console.warn('Failed to fetch voucher source modules, falling back to source-fields.', err);
        }

        // Fallback: existing endpoint, then wrap into modules for advanced picker UI.
        await fetchBillSourceFields();
    };

    const fetchBillSourceFields = async () => {
        try {
            const res = await axios.get(`${API_BASE}/vouchers/source-fields`, {
                params: { source_type: 'bills' }
            });
            const dynamicFields = normalizeSourceFields(res?.data?.fields ?? res?.data ?? []);
            if (dynamicFields.length === 0) return;
            const merged = mergeSourceFields(FALLBACK_BILL_SOURCE_FIELDS, dynamicFields);
            setBillSourceFields(merged);
            setVoucherFieldModules(buildDefaultVoucherFieldModules(merged, receiptBillSourceFields || FALLBACK_RECEIPT_BILL_SOURCE_FIELDS));
        } catch (err) {
            console.warn('Failed to fetch dynamic bill source fields, fallback to built-in list.', err);
        }
    };

    const normalizeTemplateFieldBindings = (template: VoucherTemplate) => {
        const effectiveSourceType = getEffectiveSourceType(template.source_type);
        const moduleId = String(template.source_module || '').trim() || inferModuleIdFromSourceType(voucherFieldModules, effectiveSourceType);
        const module = (voucherFieldModules || []).find(m => String(m?.id) === moduleId) || (voucherFieldModules || [])[0];

        type SourceCtx = { module_id: string; source_id: string; source_type: string; fieldKeys: Set<string> };
        const ctxByPrefix = new Map<string, SourceCtx>();

        (module?.sources || []).forEach(s => {
            const sid = String(s?.id || '').trim();
            const st = String(s?.source_type || '').trim();
            const fieldKeys = new Set(
                (s?.fields || [])
                    .map((f: any) => String(f?.value || '').trim())
                    .filter(Boolean)
            );
            const ctx: SourceCtx = { module_id: String(module?.id || 'marki'), source_id: sid, source_type: st, fieldKeys };
            if (sid) ctxByPrefix.set(sid.toLowerCase(), ctx);
            if (st) ctxByPrefix.set(st.toLowerCase(), ctx);
        });

        const defaultCtx =
            ctxByPrefix.get(String(effectiveSourceType || '').trim().toLowerCase()) ||
            (module?.sources?.[0] ? ctxByPrefix.get(String(module.sources[0].id || '').toLowerCase()) : undefined);

        const buildTargetKey = (ctx: SourceCtx | undefined, baseKey: string) => {
            const b = String(baseKey || '').trim();
            if (!ctx || !b) return '';
            if (!ctx.module_id || !ctx.source_id) return '';
            return `${ctx.module_id}.${ctx.source_id}.${b}`;
        };

        let replaced = 0;

        const replaceInText = (text: string | null | undefined) => {
            const rawText = text == null ? '' : String(text);
            if (!rawText) return rawText;
            return rawText.replace(/\{([^{}]+)\}/g, (match, key) => {
                const rawKey = String(key || '').trim();
                if (!rawKey) return match;
                const parts = rawKey.split('.').filter(Boolean);
                if (parts.length >= 3) return match;

                const [prefix, base] =
                    parts.length === 2 ? [String(parts[0] || '').trim(), String(parts[1] || '').trim()] : ['', String(parts[0] || '').trim()];

                const ctx = prefix ? ctxByPrefix.get(prefix.toLowerCase()) : defaultCtx;
                if (!ctx || !base || !ctx.fieldKeys.has(base)) return match;

                const next = buildTargetKey(ctx, base);
                if (!next || rawKey === next) return match;
                replaced += 1;
                return `{${next}}`;
            });
        };

        const normalizeFieldKey = (field: string | null | undefined) => {
            const raw = field == null ? '' : String(field).trim();
            if (!raw) return raw;
            const parts = raw.split('.').filter(Boolean);
            if (parts.length >= 3) return raw;

            const [prefix, base] =
                parts.length === 2 ? [String(parts[0] || '').trim(), String(parts[1] || '').trim()] : ['', String(parts[0] || '').trim()];

            const ctx = prefix ? ctxByPrefix.get(prefix.toLowerCase()) : defaultCtx;
            if (!ctx || !base || !ctx.fieldKeys.has(base)) return raw;

            const next = buildTargetKey(ctx, base);
            if (!next || raw === next) return raw;
            replaced += 1;
            return next;
        };

        const normalizeTriggerCondition = (trigger: string | null | undefined) => {
            const raw = trigger == null ? '' : String(trigger).trim();
            if (!raw) return raw;
            try {
                const root = JSON.parse(raw);
                const walk = (node: any) => {
                    if (!node || typeof node !== 'object') return;
                    const t = node.type || 'group';
                    if (t === 'group') {
                        const children = Array.isArray(node.children) ? node.children : [];
                        children.forEach(walk);
                        return;
                    }
                    if (t === 'rule') {
                        if (typeof node.field === 'string') node.field = normalizeFieldKey(node.field);
                        if (node.value != null) node.value = replaceInText(String(node.value));
                    }
                };
                walk(root);
                return JSON.stringify(root);
            } catch {
                return replaceInText(raw);
            }
        };

        const nextTemplate: VoucherTemplate = {
            ...template,
            book_number_expr: replaceInText(template.book_number_expr),
            vouchertype_number_expr: replaceInText(template.vouchertype_number_expr),
            attachment_expr: replaceInText(template.attachment_expr),
            bizdate_expr: replaceInText(template.bizdate_expr),
            bookeddate_expr: replaceInText(template.bookeddate_expr),
            trigger_condition: normalizeTriggerCondition(template.trigger_condition),
            rules: (template.rules || []).map(r => ({
                ...r,
                account_code: replaceInText(r.account_code),
                amount_expr: replaceInText(r.amount_expr),
                summary_expr: replaceInText(r.summary_expr),
                currency_expr: replaceInText(r.currency_expr),
                localrate_expr: replaceInText(r.localrate_expr),
                aux_items: r.aux_items == null ? r.aux_items : replaceInText(r.aux_items),
                main_cf_assgrp: r.main_cf_assgrp == null ? r.main_cf_assgrp : replaceInText(r.main_cf_assgrp),
            })),
        };

        return { template: nextTemplate, replaced };
    };



    const handleCreate = () => {
        setSaveErrors([]);
        setCurrentTemplate({
            template_id: '',
            template_name: '',
            business_type: 'payment',
            description: '',
            active: true,
            priority: 100,
            book_number_expr: "'BU-35256'",
            vouchertype_number_expr: "'0001'",
            attachment_expr: "0",
            bizdate_expr: "{CURRENT_DATE}",
            bookeddate_expr: "{CURRENT_DATE}",
            source_module: 'marki',
            source_type: 'bills',
            rules: [
                { line_no: 1, dr_cr: 'D', account_code: '', amount_expr: '', summary_expr: '', currency_expr: "'CNY'", localrate_expr: "1" },
                { line_no: 2, dr_cr: 'C', account_code: '', amount_expr: '', summary_expr: '', currency_expr: "'CNY'", localrate_expr: "1" }
            ]
        });
        setEditingTemplateId(null);
        setIsEditing(true);
        setActiveTab('basic');
    };

    const handleEdit = (template: VoucherTemplate) => {
        setSaveErrors([]);
        setCurrentTemplate({
            ...template,
            source_module: template.source_module || inferModuleIdFromSourceType(voucherFieldModules, template.source_type),
        });
        setEditingTemplateId(template.template_id);
        setIsEditing(true);
        setActiveTab('basic');
    };

    const handleCopy = (template: VoucherTemplate) => {
        setSaveErrors([]);
        setCurrentTemplate({
            ...template,
            template_id: getUniqueCopiedTemplateId(template.template_id, templates),
            template_name: getUniqueCopiedTemplateName(template.template_name, templates),
            source_module: template.source_module || inferModuleIdFromSourceType(voucherFieldModules, template.source_type),
            rules: (template.rules || []).map((rule, index) => ({
                ...rule,
                rule_id: null,
                line_no: index + 1
            }))
        });
        setEditingTemplateId(null);
        setIsEditing(true);
        setActiveTab('basic');
    };

    const handleDelete = async (id: string) => {
        if (!confirm('确定要删除这个模板吗？')) return;
        try {
            await axios.delete(`${API_BASE}/vouchers/templates/${id}`);
            fetchTemplates();
        } catch (err) {
            alert('删除失败');
        }
    };

    const handleSave = async () => {
        if (!currentTemplate) return;
        setSaveErrors([]);

        const normalized = normalizeTemplateFieldBindings(currentTemplate);
        const workingTemplate = normalized.replaced > 0 ? normalized.template : currentTemplate;
        if (normalized.replaced > 0) {
            setCurrentTemplate(workingTemplate);
        }

        const payload: VoucherTemplate = {
            ...workingTemplate,
            template_id: workingTemplate.template_id.trim(),
            template_name: workingTemplate.template_name.trim(),
            priority: Number.isFinite(Number(workingTemplate.priority))
                ? Math.max(0, Number(workingTemplate.priority))
                : 100
        };

        if (!payload.template_id || !payload.template_name) {
            setSaveErrors(['请填写必要字段：模板 ID、模板名称']);
            return;
        }

        try {
            if (editingTemplateId) {
                await axios.put(`${API_BASE}/vouchers/templates/${editingTemplateId}`, payload);
            } else {
                await axios.post(`${API_BASE}/vouchers/templates`, payload);
            }
            setSaveErrors([]);
            setIsEditing(false);
            setEditingTemplateId(null);
            fetchTemplates();
        } catch (err: any) {
            setSaveErrors(extractSaveErrorMessages(err));
        }
    };

    const addRule = () => {
        if (!currentTemplate) return;
        const newRule: VoucherEntryRule = {
            line_no: currentTemplate.rules.length + 1,
            dr_cr: 'D',
            account_code: '',
            amount_expr: '',
            summary_expr: '',
            currency_expr: "'CNY'",
            localrate_expr: "1"
        };
        setCurrentTemplate({
            ...currentTemplate,
            rules: [...currentTemplate.rules, newRule]
        });
    };

    const removeRule = (idx: number) => {
        if (!currentTemplate) return;
        const newRules = currentTemplate.rules.filter((_, i) => i !== idx)
            .map((r, i) => ({ ...r, line_no: i + 1 }));
        setCurrentTemplate({ ...currentTemplate, rules: newRules });
    };

    const updateRule = (idx: number, updates: Partial<VoucherEntryRule>) => {
        if (!currentTemplate) return;
        const newRules = [...currentTemplate.rules];
        newRules[idx] = { ...newRules[idx], ...updates };
        setCurrentTemplate({ ...currentTemplate, rules: newRules });
    };

    const openRuleDetails = (idx: number) => {
        setCurrentRuleIndex(idx);
        setDetailTab('assgrp');
        setDetailModalOpen(true);
    };

    const updateCurrentRuleDetail = (field: 'aux_items' | 'main_cf_assgrp', value: string) => {
        if (currentRuleIndex === null) return;
        updateRule(currentRuleIndex, { [field]: value });
    };

    const moveRule = (idx: number, direction: 'up' | 'down') => {
        if (!currentTemplate) return;
        const newRules = [...currentTemplate.rules];
        if (direction === 'up') {
            if (idx === 0) return;
            [newRules[idx - 1], newRules[idx]] = [newRules[idx], newRules[idx - 1]];
        } else {
            if (idx === newRules.length - 1) return;
            [newRules[idx + 1], newRules[idx]] = [newRules[idx], newRules[idx + 1]];
        }
        // Re-assign line numbers
        const reindexedRules = newRules.map((r, i) => ({ ...r, line_no: i + 1 }));
        setCurrentTemplate({ ...currentTemplate, rules: reindexedRules });
    };

    const handleAccountChange = (idx: number, accountCode: string) => {
        const subject = subjects.find(s => s.number === accountCode);
        const updates: Partial<VoucherEntryRule> = { account_code: accountCode };

        if (subject) {
            // 1. Process Check Items (Auxiliary Dimensions)
            if (subject.check_items) {
                try {
                    const checkItems = JSON.parse(subject.check_items);
                    if (Array.isArray(checkItems) && checkItems.length > 0) {
                        // Build the aux_items JSON: { "DimensionName": { "number": "" } }
                        // We assume items in check_items are required or at least suggested
                        const newAuxItemsObj: Record<string, any> = {};
                        checkItems.forEach((item: any) => {
                            // Extract name, fallback to number or default
                            const key = item.asstactitem_name || item.asstactitem_number || '未知维度';
                            // Start with empty value
                            newAuxItemsObj[key] = { number: '' };
                        });
                        updates.aux_items = JSON.stringify(newAuxItemsObj, null, 2);
                    } else {
                        // If no check items, clear existing aux items to avoid confusion
                        updates.aux_items = '';
                    }
                } catch (e) {
                    console.error("Failed to parse check_items for subject", accountCode, e);
                    // Keep existing if parse fails, or clear? Better safe than sorry, maybe keep generic?
                    // But here we are changing account, so better to clear irrelevant dims.
                    updates.aux_items = '';
                }
            } else {
                updates.aux_items = '';
            }

            // 2. Handle Direction (Optional: auto-set Dr/Cr based on subject direction?)
            // if (subject.direction) {
            //    updates.dr_cr = subject.direction === '1' ? 'D' : 'C';
            // }
        }

        updateRule(idx, updates);
    };

    const handleVariableSelect = (variable: any) => {
        if (lastFocusedField) {
            const insertText = variable?.insert_text || (variable?.key ? `{${variable.key}}` : String(variable || ''));
            lastFocusedField.insert(insertText);
        }
    };

    if (isEditing && currentTemplate) {
        return (
            <div className="template-editor-container animate-in">
                <header className="editor-header">
                    <div className="header-left">
                        <button onClick={() => { setIsEditing(false); setEditingTemplateId(null); setSaveErrors([]); }} className="back-btn">
                            <ChevronLeft size={20} />
                        </button>
                        <div>
                            <h1>{currentTemplate.template_name || '未命名模板'}</h1>
                            <p>ID: {currentTemplate.template_id || '新模板'}</p>
                        </div>
                    </div>
                    <div className="header-actions">
                        <button onClick={() => handleCopy(currentTemplate)} className="clone-btn" title="复制当前模板并创建新模板">
                            <Copy size={16} /> 复制为新模板
                        </button>
                        <button onClick={handleSave} className="save-btn">
                            <Save size={18} /> 保存模板
                        </button>
                    </div>
                </header>
                {saveErrors.length > 0 && (
                    <div className="save-error-panel">
                        <div className="save-error-title">
                            <AlertTriangle size={16} />
                            <span>保存失败，请修正以下问题：</span>
                        </div>
                        <ul className="save-error-list">
                            {saveErrors.map((msg, idx) => (
                                <li key={`${idx}-${msg}`}>{msg}</li>
                            ))}
                        </ul>
                    </div>
                )}

                <div className="editor-tabs">
                    <button
                        className={`tab-btn ${activeTab === 'basic' ? 'active' : ''}`}
                        onClick={() => setActiveTab('basic')}
                    >
                        <Settings size={16} /> 基本配置
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'condition' ? 'active' : ''}`}
                        onClick={() => setActiveTab('condition')}
                    >
                        <Sliders size={16} /> 触发条件
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'subject' ? 'active' : ''}`}
                        onClick={() => setActiveTab('subject')}
                    >
                        <Hash size={16} /> 凭证主体
                    </button>
                    <button
                        className={`tab-btn ${activeTab === 'rules' ? 'active' : ''}`}
                        onClick={() => setActiveTab('rules')}
                    >
                        <Layers size={16} /> 分录规则 ({currentTemplate.rules.length})
                    </button>
                </div>

                <div className="editor-content">
                    {activeTab === 'basic' && (
                        <div className="modern-editor-layout animate-slide-up">
                            {/* 基础与控制区域 */}
                            <div className="editor-card primary-card">
                                <div className="card-header-styled">
                                    <Layers size={18} />
                                    <div className="header-text">
                                        <h3>基础配置 <span className="tag">核心信息</span></h3>
                                        <p>定义模板在系统中的基本属性与运行控制逻辑</p>
                                    </div>
                                </div>
                                <div className="field-grid-three">
                                    <div className="field-item">
                                        <label>模板名称</label>
                                        <input
                                            type="text"
                                            placeholder="输入易于辨识的模板名称"
                                            value={currentTemplate.template_name}
                                            onChange={e => setCurrentTemplate({ ...currentTemplate, template_name: e.target.value })}
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>模板 ID</label>
                                        <input
                                            type="text"
                                            value={currentTemplate.template_id}
                                            onChange={e => setCurrentTemplate({ ...currentTemplate, template_id: e.target.value })}
                                            disabled={editingTemplateId !== null}
                                            placeholder="UNIQUE_ID"
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>业务类型</label>
                                        <input
                                            type="text"
                                            value={currentTemplate.business_type}
                                            onChange={e => setCurrentTemplate({ ...currentTemplate, business_type: e.target.value })}
                                            placeholder="例如：payment"
                                        />
                                    </div>
                                </div>
                            <div className="field-grid-three">
                                    <div className="field-item">
                                        <label>业务模块</label>
                                        <select
                                            value={currentTemplate.source_module || ''}
                                            onChange={e => {
                                                const nextModuleId = String(e.target.value || '').trim();
                                                const nextModule = voucherFieldModules.find(m => String(m?.id) === nextModuleId);

                                                const allowed = new Set(
                                                    (nextModule?.sources || [])
                                                        .map(s => String(s?.source_type || '').trim())
                                                        .filter(Boolean)
                                                        .map(s => s.toLowerCase())
                                                );

                                                const currentSt = String(currentTemplate.source_type || '').trim();
                                                let nextSt = currentSt;
                                                if (allowed.size > 0) {
                                                    if (!currentSt || !allowed.has(currentSt.toLowerCase())) {
                                                        nextSt = String(nextModule?.sources?.[0]?.source_type || '').trim();
                                                    }
                                                }

                                                setCurrentTemplate({
                                                    ...currentTemplate,
                                                    source_module: nextModuleId,
                                                    source_type: nextSt,
                                                });
                                            }}
                                        >
                                            {voucherFieldModules.map(m => (
                                                <option key={m.id} value={String(m.id)}>
                                                    {m.label}
                                                </option>
                                            ))}
                                        </select>
                                    </div>
                                    <div className="field-item">
                                        <label>优先级 (数字越小越优先)</label>
                                        <input
                                            type="number"
                                            min={0}
                                            value={Number.isFinite(Number(currentTemplate.priority)) ? currentTemplate.priority : 100}
                                            onChange={e => setCurrentTemplate({ ...currentTemplate, priority: Number(e.target.value) })}
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>模板状态</label>
                                        <button 
                                            className={`toggle-switch ${currentTemplate.active ? 'active' : ''}`}
                                            onClick={() => setCurrentTemplate({ ...currentTemplate, active: !currentTemplate.active })}
                                        >
                                            <div className="toggle-thumb" />
                                            <span className="toggle-label">{currentTemplate.active ? '已启用' : '已停用'}</span>
                                        </button>
                                    </div>
                                </div>
                                <div className="field-grid-single">
                                    <div className="field-item" style={{ paddingBottom: '0' }}>
                                        <label>功能详细描述</label>
                                        <input
                                            type="text"
                                            value={currentTemplate.description}
                                            onChange={e => setCurrentTemplate({ ...currentTemplate, description: e.target.value })}
                                            placeholder="请输入该凭证模板的设计初衷或应用场景..."
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}

                    {activeTab === 'condition' && (
                        <div className="modern-editor-layout animate-slide-up">
                            {/* 第二层级：触发条件区域 (仅在有数据源时显示) */}
                            {['bills', 'receipt_bills'].includes(getEffectiveSourceType(currentTemplate.source_type)) ? (
                                <div className="editor-card animate-slide-up">
                                    <div className="card-header-styled">
                                        <Sliders size={18} />
                                        <div className="header-text">
                                            <h3>自动化触发源配置 <span className="tag">高级逻辑</span></h3>
                                            <p>只有满足下方所有条件的业务单据才会应用此凭证模板</p>
                                        </div>
                                    </div>
                                    <div className="nested-body" style={{ padding: '1.5rem' }}>
                                        <div style={{ display: 'flex', gap: '1rem', alignItems: 'flex-end', marginBottom: '1rem' }}>
                                            <div className="field-item" style={{ paddingBottom: 0, minWidth: 240 }}>
                                                <label>{'\u89e6\u53d1\u6570\u636e\u6e90'}</label>
                                                <select
                                                    value={getEffectiveSourceType(currentTemplate.source_type)}
                                                    onChange={e => setCurrentTemplate({ ...currentTemplate, source_type: String(e.target.value || '').trim() })}
                                                >
                                                    {(() => {
                                                        const mid = String(currentTemplate.source_module || '').trim()
                                                            || inferModuleIdFromSourceType(voucherFieldModules, currentTemplate.source_type);
                                                        const mod = voucherFieldModules.find(m => String(m?.id) === mid) || voucherFieldModules[0];
                                                        const triggerSources = (mod?.sources || []).filter(s =>
                                                            ['bills', 'receipt_bills'].includes(String(s?.source_type || '').trim())
                                                        );
                                                        return triggerSources.map(s => (
                                                            <option key={`${mod?.id}:${s.id}`} value={String(s.source_type || '')}>
                                                                {s.label} ({s.source_type})
                                                            </option>
                                                        ));
                                                    })()}
                                                </select>
                                            </div>
                                            <div style={{ fontSize: '0.75rem', color: '#94a3b8', paddingBottom: '0.25rem' }}>
                                                仅用于触发条件判断，不限制分录取数来源
                                            </div>
                                        </div>
                                        <ConditionBuilder
                                            value={currentTemplate.trigger_condition}
                                            onChange={(val) => setCurrentTemplate({ ...currentTemplate, trigger_condition: val })}
                                            fields={currentTemplate.source_type === 'receipt_bills' ? receiptBillSourceFields : billSourceFields}
                                            fieldModules={filterModulesBySourceType(voucherFieldModules, getEffectiveSourceType(currentTemplate.source_type))}
                                        />
                                    </div>
                                </div>
                            ) : (
                                <div className="editor-card animate-slide-up" style={{ padding: '3rem', textAlign: 'center', color: '#64748b' }}>
                                    <AlertTriangle size={48} style={{ margin: '0 auto 1rem', opacity: 0.5 }} />
                                    <h3>当前业务模块不支持触发条件</h3>
                                    <p>请选择支持触发条件的业务模块（例如：马克系统）。</p>
                                </div>
                            )}
                        </div>
                    )}

                    {activeTab === 'subject' && (
                        <div className="modern-editor-layout animate-slide-up">
                            {/* 第三层级：系统映射区域 */}
                            <div className="editor-card secondary-card">
                                <div className="card-header-styled alt-theme">
                                    <Hash size={18} />
                                    <div className="header-text">
                                        <h3>
                                            金蝶系统映射
                                            <span className="tag alt">数据中心</span>
                                            <span className="mapping-help-tooltip">
                                                <Info size={14} />
                                                <span className="mapping-help-tooltip__bubble">
                                                    业务日期对应凭证体的 `bizdate`；记账日期对应凭证体的 `bookeddate`。
                                                </span>
                                            </span>
                                        </h3>
                                        <p>配置由原始数据生成的会计凭证在金蝶系统中的元数据映射规则</p>
                                    </div>
                                </div>
                                <div className="field-grid-three">
                                    <div className="field-item">
                                        <label>账簿编码映射</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.book_number_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, book_number_expr: val })}
                                            onGlobalFocus={ins => setLastFocusedField({ insert: ins })}
                                            onOpenPicker={() => setIsVariablePickerOpen(true)}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            placeholder="'BU-CODE'"
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>凭证字映射</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.vouchertype_number_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, vouchertype_number_expr: val })}
                                            onGlobalFocus={ins => setLastFocusedField({ insert: ins })}
                                            onOpenPicker={() => setIsVariablePickerOpen(true)}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            placeholder="'0001'"
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>附件数量表达式</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.attachment_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, attachment_expr: val })}
                                            onGlobalFocus={ins => setLastFocusedField({ insert: ins })}
                                            onOpenPicker={() => setIsVariablePickerOpen(true)}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            placeholder="0"
                                        />
                                    </div>
                                </div>
                                <div className="field-grid-two">
                                    <div className="field-item">
                                        <label>业务日期表达式 (bizdate)</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.bizdate_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, bizdate_expr: val })}
                                            onGlobalFocus={ins => setLastFocusedField({ insert: ins })}
                                            onOpenPicker={() => setIsVariablePickerOpen(true)}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>记账日期表达式 (bookeddate)</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.bookeddate_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, bookeddate_expr: val })}
                                            onGlobalFocus={ins => setLastFocusedField({ insert: ins })}
                                            onOpenPicker={() => setIsVariablePickerOpen(true)}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}

                    {activeTab === 'rules' && (
                        <div className="rules-editor animate-slide-up">
                            <div className="rules-header-actions" style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }}>
                                <button onClick={addRule} className="save-btn" style={{ width: 'auto', background: '#3b82f6' }}>
                                    <Plus size={16} /> 添加分录
                                </button>
                            </div>
                            <div className="rules-table-container shadow-lg">
                                <table className="rules-table">
                                    <thead>
                                        <tr>
                                            <th style={{ width: '60px' }}>行号</th>
                                            <th>摘要表达式</th>
                                            <th>会计科目</th>
                                            <th style={{ width: '80px' }}>借/贷</th>
                                            <th>金额表达式</th>
                                            <th style={{ width: '120px' }}>操作</th>
                                            <th style={{ width: '60px' }}>排序</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {currentTemplate.rules.map((rule, idx) => (
                                            <tr key={idx}>
                                                <td>{rule.line_no}</td>
                                                <td>
                                                    <ExpressionInputWithActions
                                                        value={rule.summary_expr}
                                                        onChange={val => updateRule(idx, { summary_expr: val })}
                                                        onGlobalFocus={ins => setLastFocusedField({ insert: ins })}
                                                        onOpenPicker={() => setIsVariablePickerOpen(true)}
                                                        fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                                    />
                                                </td>
                                                <td>
                                                    <AccountSelector
                                                        value={rule.account_code}
                                                        onChange={(val) => handleAccountChange(idx, val)}
                                                        subjects={subjects}
                                                        onFocus={() => setLastFocusedField({
                                                            insert: (text) => updateRule(idx, { account_code: (rule.account_code || '') + text })
                                                        })}
                                                    />
                                                </td>
                                                <td>
                                                    <select
                                                        value={rule.dr_cr}
                                                        onChange={e => updateRule(idx, { dr_cr: e.target.value as 'D' | 'C' })}
                                                        className={rule.dr_cr === 'D' ? 'debit' : 'credit'}
                                                    >
                                                        <option value="D">借</option>
                                                        <option value="C">贷</option>
                                                    </select>
                                                </td>
                                                <td>
                                                    <ExpressionInputWithActions
                                                        value={rule.amount_expr}
                                                        onChange={val => updateRule(idx, { amount_expr: val })}
                                                        onGlobalFocus={ins => setLastFocusedField({ insert: ins })}
                                                        onOpenPicker={() => setIsVariablePickerOpen(true)}
                                                        fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                                    />
                                                </td>
                                                <td>
                                                    <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                                                        <button
                                                            className="detail-config-btn-large"
                                                            onClick={() => openRuleDetails(idx)}
                                                            title="配置辅助核算"
                                                        >
                                                            <Sliders size={16} />
                                                        </button>
                                                        <button onClick={() => removeRule(idx)} className="delete-row-btn" title="删除分录">
                                                            <Trash2 size={16} />
                                                        </button>
                                                    </div>
                                                </td>
                                                <td>
                                                    <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
                                                        <button
                                                            onClick={() => moveRule(idx, 'up')}
                                                            disabled={idx === 0}
                                                            style={{
                                                                border: 'none', background: 'transparent', cursor: idx === 0 ? 'default' : 'pointer',
                                                                opacity: idx === 0 ? 0.3 : 1, padding: 0, display: 'flex'
                                                            }}
                                                            title="上移"
                                                        >
                                                            <ArrowUp size={14} />
                                                        </button>
                                                        <button
                                                            onClick={() => moveRule(idx, 'down')}
                                                            disabled={idx === currentTemplate.rules.length - 1}
                                                            style={{
                                                                border: 'none', background: 'transparent', cursor: idx === currentTemplate.rules.length - 1 ? 'default' : 'pointer',
                                                                opacity: idx === currentTemplate.rules.length - 1 ? 0.3 : 1, padding: 0, display: 'flex'
                                                            }}
                                                            title="下移"
                                                        >
                                                            <ArrowDown size={14} />
                                                        </button>
                                                    </div>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}
                </div >

                <VariablePicker
                    isOpen={isVariablePickerOpen}
                    onClose={() => setIsVariablePickerOpen(false)}
                    onSelect={handleVariableSelect}
                    includeFunctions
                />

                {/* Detail Configuration Modal */}
                {
                    detailModalOpen && currentRuleIndex !== null && currentTemplate.rules[currentRuleIndex] && (
                        <div className="rule-detail-overlay">
                            <div className="rule-detail-modal glass-card">
                                <div className="modal-header">
                                    <h3>分录行 #{currentTemplate.rules[currentRuleIndex].line_no} 详细配置</h3>
                                    <button onClick={() => setDetailModalOpen(false)}><X size={20} /></button>
                                </div>

                                <div className="detail-tabs">
                                    <button
                                        className={`detail-tab ${detailTab === 'assgrp' ? 'active' : ''}`}
                                        onClick={() => setDetailTab('assgrp')}
                                    >
                                        辅助核算
                                    </button>
                                    <button
                                        className={`detail-tab ${detailTab === 'maincf' ? 'active' : ''}`}
                                        onClick={() => setDetailTab('maincf')}
                                    >
                                        主表核算 (现金流量项目)
                                    </button>
                                </div>

                                <div className="detail-body">
                                    {detailTab === 'assgrp' && (
                                        <div className="detail-section">
                                            <div className="info-box">
                                                <Info size={14} />
                                                <span>
                                                    配置科目辅助核算维度，对应金蝶的 assgrp 字段。<br />
                                                    通常格式：维度名称 (如 "客户") -&gt; 属性 (如 "number") -&gt; 编码值
                                                </span>
                                            </div>
                                            <DimensionFormEditor
                                                value={currentTemplate.rules[currentRuleIndex].aux_items}
                                                onChange={(val) => updateCurrentRuleDetail('aux_items', val)}
                                                onFocusField={(insert) => setLastFocusedField({ insert })}
                                                onOpenPicker={() => setIsVariablePickerOpen(true)}
                                                fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                                requiredKeys={(() => {
                                                    const accountCode = currentTemplate.rules[currentRuleIndex].account_code;
                                                    const subject = subjects.find(s => s.number === accountCode);
                                                    if (subject?.check_items) {
                                                        try {
                                                            const checkItems = JSON.parse(subject.check_items);
                                                            if (Array.isArray(checkItems)) {
                                                                return checkItems.map((item: any) =>
                                                                    item.asstactitem_name || item.asstactitem_number || '未知维度'
                                                                );
                                                            }
                                                        } catch { }
                                                    }
                                                    return [];
                                                })()}
                                            />
                                        </div>
                                    )}

                                    {detailTab === 'maincf' && (
                                        <div className="detail-section">
                                            <div className="info-box">
                                                <Info size={14} />
                                                <span>
                                                    配置现金流量项目核算维度，对应金蝶的 maincfassgrp 字段。<br />
                                                    通常需要配置：项目 (如 "现金流量项目") -&gt; number -&gt; 项目编码
                                                </span>
                                            </div>
                                            <DimensionFormEditor
                                                value={currentTemplate.rules[currentRuleIndex].main_cf_assgrp}
                                                onChange={(val) => updateCurrentRuleDetail('main_cf_assgrp', val)}
                                                onFocusField={(insert) => setLastFocusedField({ insert })}
                                                onOpenPicker={() => setIsVariablePickerOpen(true)}
                                                fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            />
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    )
                }
            </div >
        );
    }

    return (
        <div className="templates-page animate-in">
            <div className="page-header flex justify-end mb-4">
                <div>
                    <h1>模板管理</h1>
                    <p>管理业务流程触发的会计凭证自动生成规则，适配金蝶 OpenAPI 结构。</p>
                </div>
                <button onClick={handleCreate} className="create-btn">
                    <Plus size={18} /> 新建模板
                </button>
            </div>

            {isLoading ? (
                <div className="loading-state">加载中...</div>
            ) : (
                <>
                    {loadError && (
                        <div className="save-error-panel" style={{ marginBottom: '1.25rem' }}>
                            <div className="save-error-title">
                                <AlertTriangle size={16} />
                                <span>模板列表加载失败</span>
                            </div>
                            <ul className="save-error-list">
                                <li>{loadError}</li>
                            </ul>
                            <button onClick={fetchTemplates} className="create-btn" style={{ marginTop: '0.75rem', width: 'fit-content' }}>
                                重试加载
                            </button>
                        </div>
                    )}
                    <div className="templates-grid">
                        {templates.map(t => (
                            <div
                                key={t.template_id}
                                className="template-card glass-card cursor-pointer group"
                                onClick={() => handleEdit(t)}
                            >
                                <div className="card-badge">{`${t.business_type} · P${t.priority}`}</div>
                                <div className="card-content">
                                    <h3>{t.template_name}</h3>
                                    <p>{t.description || '暂无描述'}</p>
                                    <div className="card-stats">
                                        <span><Layers size={14} /> {t.rules.length} 条分录规则</span>
                                        <span><FileText size={14} /> {t.template_id}</span>
                                        <span>{t.active ? '启用' : '停用'}</span>
                                    </div>
                                </div>
                                <div className="card-actions">
                                    <button onClick={(e) => { e.stopPropagation(); handleCopy(t); }} className="copy-btn"><Copy size={14} />复制</button>
                                    <button onClick={(e) => { e.stopPropagation(); handleEdit(t); }} className="edit-btn">编辑</button>
                                    <button onClick={(e) => { e.stopPropagation(); handleDelete(t.template_id); }} className="delete-btn"><Trash2 size={16} /></button>
                                </div>
                            </div>
                        ))}
                        {templates.length === 0 && !loadError && (
                            <div className="empty-state">
                                <Info size={40} />
                                <p>尚未创建任何模板</p>
                                <button onClick={handleCreate}>立即创建</button>
                            </div>
                        )}
                    </div>
                </>
            )}
        </div>
    );
};

export default VoucherTemplates;

