import { useState, useEffect, useMemo, useRef, forwardRef, useImperativeHandle } from 'react';
import { useLocation } from 'react-router-dom';
import type { DragEvent as ReactDragEvent } from 'react';
import { ToastContainer, useToast } from '../../components/Toast';
import ConfirmModal from '../../components/common/ConfirmModal';
import {
    Layers, FileText, Settings, Hash, Info, X, Sliders, GripVertical, AlertTriangle,
    Plus, Save, Trash2, ChevronLeft, Database, Copy, ChevronRight, ChevronDown, Search, LayoutGrid, List,
    CheckSquare, Square, ToggleLeft, ToggleRight
} from 'lucide-react';
import axios from 'axios';
import ConditionBuilder from './ConditionBuilder';
import AccountSelector from './AccountSelector';
import type { AccountingSubject, VoucherFieldModule, VoucherRelationOption, VoucherSourceFieldOption } from '../../types';
import SourceFieldPickerModal from './SourceFieldPickerModal';
import ExpressionInputWithActions from './ExpressionInputWithActions';
import { getUnifiedSourceFieldLabel, normalizeVoucherFieldModules } from './sourceFieldLabelUtils';
import './VoucherTemplates.css';

import { API_BASE_URL } from '../../services/apiBase';
import { getVoucherFieldModules, getVoucherTemplateCategoriesTree } from '../../services/api';

void forwardRef;
void useImperativeHandle;
void Database;
void SourceFieldPickerModal;

const API_BASE = API_BASE_URL;

type SourceFieldOption = {
    label: string;
    value: string;
    group?: string;
};

const buildDefaultVoucherFieldModules = (
    billsFields: SourceFieldOption[] = [],
    receiptBillFields: SourceFieldOption[] = [],
    depositRecordFields: SourceFieldOption[] = [],
): VoucherFieldModule[] => {
    return [
        {
            id: 'marki',
            label: '马克系统',
            sources: [
                {
                    id: 'bills',
                    label: '运营账单',
                    source_type: 'bills',
                    root_enabled: true,
                    fields: billsFields as unknown as VoucherSourceFieldOption[],
                }
                ,
                {
                    id: 'receipt_bills',
                    label: '收款账单',
                    source_type: 'receipt_bills',
                    root_enabled: true,
                    fields: receiptBillFields as unknown as VoucherSourceFieldOption[],
                },
                {
                    id: 'deposit_records',
                    label: '押金记录',
                    source_type: 'deposit_records',
                    root_enabled: false,
                    fields: depositRecordFields as unknown as VoucherSourceFieldOption[],
                }
            ]
        },
        {
            id: 'oa',
            label: 'OA系统',
            note: '仅预留扩展架构，暂未接入实体数据',
            sources: [
                {
                    id: 'oa_forms',
                    label: 'OA单据',
                    source_type: 'oa_forms',
                    root_enabled: false,
                    note: '仅预留扩展架构，暂未接入实体字段与关联关系',
                    fields: [],
                }
            ]
        }
    ];
};

const EMPTY_VOUCHER_FIELD_MODULES = buildDefaultVoucherFieldModules();



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
        const label = getUnifiedSourceFieldLabel({
            label: String(item?.label ?? value).trim() || value,
            value,
        });
        const group = String(item?.group ?? '').trim() || '账单字段';
        normalized.push({ label, value, group });
    });
    return normalized;
};



const DimensionFormEditor = ({
    value,
    onChange,
    fieldModules,
    requiredKeys
}: {
    value: string | null | undefined,
    onChange: (json: string) => void,
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
                                    fieldModules={fieldModules || EMPTY_VOUCHER_FIELD_MODULES}
                                    editorTitle={`编辑维度值${row.key ? ` - ${row.key}` : ''}`}
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
    display_condition_expr?: string;
    amount_expr: string;
    summary_expr: string;
    currency_expr: string;
    localrate_expr: string;
    aux_items?: string | null;
    main_cf_assgrp?: string | null;
}

type RuleDragPosition = 'before' | 'after';

interface TemplateCategory {
    id: number;
    name: string;
    parent_id?: number | null;
    path?: string | null;
    children?: TemplateCategory[];
}

interface VoucherTemplate {
    template_id: string;
    template_name: string;
    business_type: string;
    description: string;
    active: boolean;
    priority: number;
    category_id?: number | null;
    category_path?: string | null;
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

const flattenTemplateCategories = (
    nodes: TemplateCategory[] = [],
    parentPath: string = '',
    level: number = 0
): Array<{ id: number; path: string; name: string; level: number; isLeaf: boolean }> => {
    const result: Array<{ id: number; path: string; name: string; level: number; isLeaf: boolean }> = [];
    nodes.forEach(node => {
        const path = parentPath ? `${parentPath} / ${node.name}` : node.name;
        const hasChildren = Boolean(node.children && node.children.length > 0);
        result.push({ id: node.id, path, name: node.name, level, isLeaf: !hasChildren });
        if (node.children && node.children.length > 0) {
            result.push(...flattenTemplateCategories(node.children, path, level + 1));
        }
    });
    return result;
};

const buildCategoryDescendantsMap = (nodes: TemplateCategory[] = []) => {
    const map = new Map<number, Set<number>>();
    const walk = (node: TemplateCategory): Set<number> => {
        const ids = new Set<number>([node.id]);
        (node.children || []).forEach(child => {
            const childIds = walk(child);
            childIds.forEach(id => ids.add(id));
        });
        map.set(node.id, ids);
        return ids;
    };
    nodes.forEach(node => walk(node));
    return map;
};

const CategoryPickerNode = ({
    node,
    level,
    selectedId,
    onSelect
}: {
    node: TemplateCategory;
    level: number;
    selectedId: number | null;
    onSelect: (node: TemplateCategory) => void;
}) => {
    const [expanded, setExpanded] = useState(true);
    const hasChildren = Boolean(node.children && node.children.length > 0);
    const isLeaf = !hasChildren;
    const isSelected = selectedId === node.id;

    return (
        <div className="category-picker-node">
            <div
                className={`category-picker-item ${isSelected ? 'selected' : ''} ${!isLeaf ? 'non-leaf' : ''}`}
                style={{ paddingLeft: `${level * 18 + 12}px` }}
            >
                <button
                    type="button"
                    className="category-toggle"
                    onClick={() => hasChildren && setExpanded(!expanded)}
                    aria-label={expanded ? '折叠' : '展开'}
                >
                    {hasChildren ? (expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />) : <span className="toggle-spacer" />}
                </button>
                <button
                    type="button"
                    className="category-label"
                    onClick={() => isLeaf && onSelect(node)}
                    title={node.path || node.name}
                    disabled={!isLeaf}
                >
                    {node.name}
                </button>
                {!isLeaf && <span className="category-hint">父级</span>}
            </div>
            {hasChildren && expanded && (
                <div className="category-picker-children">
                    {node.children!.map(child => (
                        <CategoryPickerNode
                            key={child.id}
                            node={child}
                            level={level + 1}
                            selectedId={selectedId}
                            onSelect={onSelect}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

const CategoryNavNode = ({
    node,
    level,
    selectedId,
    onSelect
}: {
    node: TemplateCategory;
    level: number;
    selectedId: number | null;
    onSelect: (id: number) => void;
}) => {
    const [expanded, setExpanded] = useState(true);
    const hasChildren = Boolean(node.children && node.children.length > 0);
    const isSelected = selectedId === node.id;

    return (
        <div className="category-nav-node">
            <div
                className={`category-nav-item ${isSelected ? 'active' : ''}`}
                style={{ paddingLeft: `${level * 18 + 12}px` }}
            >
                <button
                    type="button"
                    className="category-nav-toggle"
                    onClick={() => hasChildren && setExpanded(!expanded)}
                    aria-label={expanded ? '折叠' : '展开'}
                >
                    {hasChildren ? (expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />) : <span className="toggle-spacer" />}
                </button>
                <button
                    type="button"
                    className="category-nav-label"
                    onClick={() => onSelect(node.id)}
                    title={node.path || node.name}
                >
                    {node.name}
                </button>
            </div>
            {hasChildren && expanded && (
                <div className="category-nav-children">
                    {node.children!.map(child => (
                        <CategoryNavNode
                            key={child.id}
                            node={child}
                            level={level + 1}
                            selectedId={selectedId}
                            onSelect={onSelect}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

const normalizeTemplateFromApi = (t: any, categoryPathMap: Record<number, string>, voucherFieldModules: VoucherFieldModule[]): VoucherTemplate => {
    return {
        template_id: t.template_id || '',
        template_name: t.template_name || '',
        business_type: t.business_type || '',
        description: t.description || '',
        active: t.active !== false,
        priority: Number.isFinite(Number(t.priority)) ? Number(t.priority) : 100,
        category_id: Number.isFinite(Number(t.category_id)) ? Number(t.category_id) : null,
        category_path: t.category_path || (Number.isFinite(Number(t.category_id)) ? categoryPathMap[Number(t.category_id)] : null),
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
        book_number_expr: t.book_number_expr || "{CURRENT_ACCOUNT_BOOK_NUMBER}",
        vouchertype_number_expr: t.vouchertype_number_expr || "'0001'",
        attachment_expr: t.attachment_expr || "0",
        bizdate_expr: t.bizdate_expr || "{CURRENT_DATE}",
        bookeddate_expr: t.bookeddate_expr || "{CURRENT_DATE}",
        rules: Array.isArray(t.rules) ? t.rules : []
    };
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

const getUniqueCopiedTemplateIdFromSet = (sourceId: string, usedIds: Set<string>) => {
    const baseId = (sourceId || 'template').trim().replace(/\s+/g, '_');
    const copyBase = `${baseId}_copy`;
    let candidate = copyBase;
    let index = 2;
    while (usedIds.has(candidate.toLowerCase())) {
        candidate = `${copyBase}_${index}`;
        index += 1;
    }
    usedIds.add(candidate.toLowerCase());
    return candidate;
};

const getUniqueCopiedTemplateNameFromSet = (sourceName: string, usedNames: Set<string>) => {
    const baseName = (sourceName || '未命名模板').trim();
    const copyBase = `${baseName}（副本）`;
    let candidate = copyBase;
    let index = 2;
    while (usedNames.has(candidate)) {
        candidate = `${baseName}（副本${index}）`;
        index += 1;
    }
    usedNames.add(candidate);
    return candidate;
};

const VIEW_MODE_STORAGE_KEY = 'voucher_templates_view_mode';
const getInitialViewMode = (): 'card' | 'list' => {
    if (typeof window === 'undefined') return 'list';
    try {
        const stored = window.localStorage.getItem(VIEW_MODE_STORAGE_KEY);
        if (stored === 'card' || stored === 'list') return stored;
    } catch { }
    return 'list';
};



const VoucherTemplates = () => {
    const [templates, setTemplates] = useState<VoucherTemplate[]>([]);
    const [isEditing, setIsEditing] = useState(false);
    const [currentTemplate, setCurrentTemplate] = useState<VoucherTemplate | null>(null);
    const [editingTemplateId, setEditingTemplateId] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);

    const [activeTab, setActiveTab] = useState<'basic' | 'condition' | 'subject' | 'rules'>('basic');

    // Detail Modal State
    const [detailModalOpen, setDetailModalOpen] = useState(false);
    const [displayConditionModalOpen, setDisplayConditionModalOpen] = useState(false);
    const [currentRuleIndex, setCurrentRuleIndex] = useState<number | null>(null);
    const [detailTab, setDetailTab] = useState<'assgrp' | 'maincf'>('assgrp');

    const [subjects, setSubjects] = useState<AccountingSubject[]>([]);
    const [saveErrors, setSaveErrors] = useState<string[]>([]);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [billSourceFields, setBillSourceFields] = useState<SourceFieldOption[]>([]);
    const [receiptBillSourceFields, setReceiptBillSourceFields] = useState<SourceFieldOption[]>([]);
    const [depositSourceFields, setDepositSourceFields] = useState<SourceFieldOption[]>([]);
    const [voucherFieldModules, setVoucherFieldModules] = useState<VoucherFieldModule[]>(EMPTY_VOUCHER_FIELD_MODULES);
    const [relationOptions, setRelationOptions] = useState<VoucherRelationOption[]>([]);
    const [templateCategories, setTemplateCategories] = useState<TemplateCategory[]>([]);
    const [categoryFilter, setCategoryFilter] = useState<string>('all');
    const [categoryPickerOpen, setCategoryPickerOpen] = useState(false);
    const [pendingCategoryId, setPendingCategoryId] = useState<number | null>(null);
    const [searchText, setSearchText] = useState('');
    const [sortKey, setSortKey] = useState<'priority_desc' | 'priority_asc' | 'name_asc' | 'name_desc'>('priority_desc');
    const [selectedTemplateIds, setSelectedTemplateIds] = useState<Set<string>>(new Set());
    const [viewMode, setViewMode] = useState<'card' | 'list'>(() => getInitialViewMode());
    const [batchMenuOpen, setBatchMenuOpen] = useState(false);
    const batchMenuRef = useRef<HTMLDivElement | null>(null);
    const templatesListRef = useRef<HTMLDivElement | null>(null);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageSize, setPageSize] = useState(12);
    const [draggingRuleIndex, setDraggingRuleIndex] = useState<number | null>(null);
    const [dragOverRuleIndex, setDragOverRuleIndex] = useState<number | null>(null);
    const [dragOverRulePosition, setDragOverRulePosition] = useState<RuleDragPosition>('before');
    const location = useLocation();
    const { toasts, showToast, removeToast } = useToast();
    const [confirmState, setConfirmState] = useState<{
        open: boolean;
        title: string;
        message: string;
        confirmText: string;
        intent: 'primary' | 'danger';
        onConfirm: (() => void | Promise<void>) | null;
        loading: boolean;
    }>({
        open: false,
        title: '',
        message: '',
        confirmText: '确认',
        intent: 'primary',
        onConfirm: null,
        loading: false
    });

    const categoryOptions = useMemo(() => flattenTemplateCategories(templateCategories), [templateCategories]);
    const categoryPathMap = useMemo(() => {
        const map: Record<number, string> = {};
        categoryOptions.forEach(opt => { map[opt.id] = opt.path; });
        return map;
    }, [categoryOptions]);
    const categoryDescendantsMap = useMemo(() => buildCategoryDescendantsMap(templateCategories), [templateCategories]);
    const categoryLeafSet = useMemo(() => {
        const set = new Set<number>();
        categoryOptions.forEach(opt => {
            if (opt.isLeaf) set.add(opt.id);
        });
        return set;
    }, [categoryOptions]);
    const filteredTemplates = useMemo(() => {
        if (categoryFilter === 'all') return templates;
        if (categoryFilter === 'uncategorized') {
            return templates.filter(t => !t.category_id);
        }
        const targetId = Number(categoryFilter);
        if (!Number.isFinite(targetId)) return templates;
        const allowedIds = categoryDescendantsMap.get(targetId) || new Set<number>([targetId]);
        return templates.filter(t => allowedIds.has(Number(t.category_id)));
    }, [templates, categoryFilter, categoryDescendantsMap]);
    const searchedTemplates = useMemo(() => {
        const query = searchText.trim().toLowerCase();
        if (!query) return filteredTemplates;
        return filteredTemplates.filter(t => {
            const hay = [
                t.template_name,
                t.template_id,
                t.description,
                t.business_type,
                t.category_path,
            ].map(v => String(v || '').toLowerCase());
            return hay.some(v => v.includes(query));
        });
    }, [filteredTemplates, searchText]);
    const displayedTemplates = useMemo(() => {
        const list = [...searchedTemplates];
        switch (sortKey) {
            case 'priority_asc':
                return list.sort((a, b) => (a.priority ?? 0) - (b.priority ?? 0));
            case 'name_asc':
                return list.sort((a, b) => String(a.template_name || '').localeCompare(String(b.template_name || '')));
            case 'name_desc':
                return list.sort((a, b) => String(b.template_name || '').localeCompare(String(a.template_name || '')));
            case 'priority_desc':
            default:
                return list.sort((a, b) => (b.priority ?? 0) - (a.priority ?? 0));
        }
    }, [searchedTemplates, sortKey]);
    const totalPages = useMemo(() => {
        return Math.max(1, Math.ceil(displayedTemplates.length / pageSize));
    }, [displayedTemplates.length, pageSize]);
    const pagedTemplates = useMemo(() => {
        const start = (currentPage - 1) * pageSize;
        return displayedTemplates.slice(start, start + pageSize);
    }, [displayedTemplates, currentPage, pageSize]);

    const currentFilterLabel = useMemo(() => {
        if (categoryFilter === 'all') return '全部';
        if (categoryFilter === 'uncategorized') return '未分类';
        const targetId = Number(categoryFilter);
        if (!Number.isFinite(targetId)) return '全部';
        return categoryPathMap[targetId] || '未分类';
    }, [categoryFilter, categoryPathMap]);
    const selectedCount = selectedTemplateIds.size;
    const isAllSelected = pagedTemplates.length > 0 && pagedTemplates.every(t => selectedTemplateIds.has(t.template_id));

    useEffect(() => {
        setSelectedTemplateIds(new Set());
        setCurrentPage(1);
    }, [categoryFilter, searchText, templates]);

    useEffect(() => {
        if (currentPage > totalPages) {
            setCurrentPage(totalPages);
        }
    }, [currentPage, totalPages]);

    useEffect(() => {
        if (templatesListRef.current) {
            templatesListRef.current.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }, [currentPage, pageSize, viewMode]);

    const openConfirm = (payload: {
        title: string;
        message: string;
        confirmText?: string;
        intent?: 'primary' | 'danger';
        onConfirm: () => void | Promise<void>;
    }) => {
        setConfirmState({
            open: true,
            title: payload.title,
            message: payload.message,
            confirmText: payload.confirmText || '确认',
            intent: payload.intent || 'primary',
            onConfirm: payload.onConfirm,
            loading: false
        });
    };

    const closeConfirm = () => {
        setConfirmState(prev => ({ ...prev, open: false, loading: false }));
    };

    const handleConfirm = async () => {
        const action = confirmState.onConfirm;
        if (!action || confirmState.loading) return;
        setConfirmState(prev => ({ ...prev, loading: true }));
        try {
            await action();
        } finally {
            closeConfirm();
        }
    };

    useEffect(() => {
        if (categoryPickerOpen) {
            setPendingCategoryId(currentTemplate?.category_id ?? null);
        }
    }, [categoryPickerOpen, currentTemplate?.category_id]);

    useEffect(() => {
        try {
            window.localStorage.setItem(VIEW_MODE_STORAGE_KEY, viewMode);
        } catch { }
    }, [viewMode]);

    useEffect(() => {
        const handler = (event: MouseEvent) => {
            if (!batchMenuRef.current) return;
            if (!batchMenuRef.current.contains(event.target as Node)) {
                setBatchMenuOpen(false);
            }
        };
        if (batchMenuOpen) {
            document.addEventListener('mousedown', handler);
        }
        return () => document.removeEventListener('mousedown', handler);
    }, [batchMenuOpen]);

    useEffect(() => {
        if (!batchMenuOpen) return;
        const onKeyDown = (event: KeyboardEvent) => {
            if (!event.altKey) return;
            const key = event.key.toLowerCase();
            switch (key) {
                case 'a':
                    event.preventDefault();
                    handleToggleSelectAll();
                    setBatchMenuOpen(false);
                    break;
                case 'c':
                    event.preventDefault();
                    setSelectedTemplateIds(new Set());
                    setBatchMenuOpen(false);
                    break;
                case 'e':
                    event.preventDefault();
                    handleBatchSetActive(true);
                    setBatchMenuOpen(false);
                    break;
                case 'd':
                    event.preventDefault();
                    handleBatchSetActive(false);
                    setBatchMenuOpen(false);
                    break;
                case 'p':
                    event.preventDefault();
                    handleBatchCopy();
                    setBatchMenuOpen(false);
                    break;
                case 'x':
                    event.preventDefault();
                    handleBatchDelete();
                    setBatchMenuOpen(false);
                    break;
                default:
                    break;
            }
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [batchMenuOpen, displayedTemplates.length, selectedCount, isAllSelected]);

    const toggleTemplateSelected = (id: string) => {
        setSelectedTemplateIds(prev => {
            const next = new Set(prev);
            if (next.has(id)) {
                next.delete(id);
            } else {
                next.add(id);
            }
            return next;
        });
    };

    const handleToggleSelectAll = () => {
        if (isAllSelected) {
            setSelectedTemplateIds(new Set());
            return;
        }
        const next = new Set<string>();
        pagedTemplates.forEach(t => next.add(t.template_id));
        setSelectedTemplateIds(next);
    };

    const handleBatchDelete = async () => {
        if (selectedTemplateIds.size === 0) return;
        openConfirm({
            title: '批量删除',
            message: `确定要删除选中的 ${selectedTemplateIds.size} 个模板吗？`,
            confirmText: '删除',
            intent: 'danger',
            onConfirm: async () => {
                try {
                    const tasks = Array.from(selectedTemplateIds).map(id => axios.delete(`${API_BASE}/vouchers/templates/${id}`));
                    const results = await Promise.allSettled(tasks);
                    const failed = results.filter(r => r.status === 'rejected');
                    if (failed.length > 0) {
                        showToast('error', '批量删除未完全成功', `成功 ${selectedTemplateIds.size - failed.length} 个，失败 ${failed.length} 个`);
                    } else {
                        showToast('success', '批量删除成功');
                    }
                    setSelectedTemplateIds(new Set());
                    fetchTemplates();
                } catch (err) {
                    console.error('Failed to batch delete templates:', err);
                    showToast('error', '批量删除失败', '请稍后重试');
                }
            }
        });
    };

    const handleBatchSetActive = async (active: boolean) => {
        if (selectedTemplateIds.size === 0) return;
        const actionLabel = active ? '启用' : '停用';
        openConfirm({
            title: `批量${actionLabel}`,
            message: `确定要${actionLabel}选中的 ${selectedTemplateIds.size} 个模板吗？`,
            confirmText: actionLabel,
            intent: 'primary',
            onConfirm: async () => {
                try {
                    const tasks = Array.from(selectedTemplateIds).map(id =>
                        axios.put(`${API_BASE}/vouchers/templates/${id}`, { active })
                    );
                    const results = await Promise.allSettled(tasks);
                    const failed = results.filter(r => r.status === 'rejected');
                    if (failed.length > 0) {
                        showToast('error', `批量${actionLabel}未完全成功`, `成功 ${selectedTemplateIds.size - failed.length} 个，失败 ${failed.length} 个`);
                    } else {
                        showToast('success', `批量${actionLabel}成功`);
                    }
                    setSelectedTemplateIds(new Set());
                    fetchTemplates();
                } catch (err) {
                    console.error('Failed to batch update templates:', err);
                    showToast('error', `批量${actionLabel}失败`, '请稍后重试');
                }
            }
        });
    };

    const handleBatchCopy = async () => {
        if (selectedTemplateIds.size === 0) return;
        openConfirm({
            title: '批量复制',
            message: `确定要复制选中的 ${selectedTemplateIds.size} 个模板吗？`,
            confirmText: '复制',
            intent: 'primary',
            onConfirm: async () => {
                try {
                    const usedIds = new Set(
                        templates.map(t => (t.template_id || '').trim().toLowerCase()).filter(Boolean)
                    );
                    const usedNames = new Set(
                        templates.map(t => (t.template_name || '').trim()).filter(Boolean)
                    );
                    const toCopyIds = Array.from(selectedTemplateIds);
                    let resolvedTemplates: VoucherTemplate[] = [];
                    try {
                        const detailResponses = await Promise.all(
                            toCopyIds.map(id => axios.get(`${API_BASE}/vouchers/templates/${id}`))
                        );
                        resolvedTemplates = detailResponses.map(res =>
                            normalizeTemplateFromApi(res.data, categoryPathMap, voucherFieldModules)
                        );
                    } catch (err) {
                        showToast('error', '批量复制失败', '获取模板详情失败，已终止操作');
                        return;
                    }

                    const payloads = resolvedTemplates.map(t => {
                        const newId = getUniqueCopiedTemplateIdFromSet(t.template_id, usedIds);
                        const newName = getUniqueCopiedTemplateNameFromSet(t.template_name, usedNames);
                        const { category_path, ...restTemplate } = t;
                        return {
                            ...restTemplate,
                            template_id: newId,
                            template_name: newName,
                            rules: (t.rules || []).map((rule, index) => ({
                                ...rule,
                                rule_id: null,
                                line_no: index + 1
                            }))
                        };
                    });
                    if (payloads.length === 0) {
                        showToast('error', '批量复制失败', '未能获取模板详情');
                        return;
                    }
                    try {
                        await Promise.all(payloads.map(payload => axios.post(`${API_BASE}/vouchers/templates`, payload)));
                        showToast('success', '批量复制成功');
                    } catch (err) {
                        showToast('error', '批量复制失败', '复制过程中发生错误，已终止操作');
                        return;
                    }
                    setSelectedTemplateIds(new Set());
                    fetchTemplates();
                } catch (err) {
                    console.error('Failed to batch copy templates:', err);
                    showToast('error', '批量复制失败', '请稍后重试');
                }
            }
        });
    };

    useEffect(() => {
        fetchSubjects();
        fetchTemplates();
        fetchVoucherFieldModules();
        fetchTemplateCategories();
    }, []);

    useEffect(() => {
        if (location.pathname.includes('/vouchers/templates')) {
            fetchTemplateCategories();
        }
    }, [location.pathname]);

    useEffect(() => {
        const onVisible = () => {
            if (document.visibilityState === 'visible' && location.pathname.includes('/vouchers/templates')) {
                fetchTemplateCategories();
            }
        };
        document.addEventListener('visibilitychange', onVisible);
        return () => document.removeEventListener('visibilitychange', onVisible);
    }, [location.pathname]);

    const fetchTemplateCategories = async () => {
        try {
            const data = await getVoucherTemplateCategoriesTree();
            setTemplateCategories(Array.isArray(data) ? data : []);
        } catch (err) {
            console.warn('Failed to fetch template categories:', err);
            setTemplateCategories([]);
        }
    };

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
            const normalized = (res.data || []).map((t: any) =>
                normalizeTemplateFromApi(t, categoryPathMap, voucherFieldModules)
            ) as VoucherTemplate[];
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

    const extractSourceFields = (modules: VoucherFieldModule[], sourceType: string): SourceFieldOption[] => {
        const normalizedSourceType = String(sourceType || '').trim().toLowerCase();
        const matchedSource = modules
            .flatMap(module => module.sources || [])
            .find(source => String(source?.source_type || '').trim().toLowerCase() === normalizedSourceType);
        return normalizeSourceFields(matchedSource?.fields || []);
    };

    const applySourceMetadata = (
        modules: VoucherFieldModule[],
        relations: VoucherRelationOption[] = [],
    ) => {
        const normalizedModules = normalizeVoucherFieldModules(modules);
        setVoucherFieldModules(normalizedModules);
        setRelationOptions(relations);
        setBillSourceFields(extractSourceFields(normalizedModules, 'bills'));
        setReceiptBillSourceFields(extractSourceFields(normalizedModules, 'receipt_bills'));
        setDepositSourceFields(extractSourceFields(normalizedModules, 'deposit_records'));
    };

    const fetchSourceFieldsByType = async (sourceType: 'bills' | 'receipt_bills' | 'deposit_records') => {
        const res = await axios.get(`${API_BASE}/vouchers/source-fields`, {
            params: { source_type: sourceType }
        });
        return normalizeSourceFields(res?.data?.fields ?? res?.data ?? []);
    };

    const fetchSourceFieldsFallback = async () => {
        const [bills, receiptBills, depositRecords] = await Promise.all([
            fetchSourceFieldsByType('bills'),
            fetchSourceFieldsByType('receipt_bills'),
            fetchSourceFieldsByType('deposit_records'),
        ]);

        applySourceMetadata(buildDefaultVoucherFieldModules(bills, receiptBills, depositRecords), []);
    };

    const fetchVoucherFieldModules = async () => {
        try {
            const res = await getVoucherFieldModules();
            const modules = res?.modules as VoucherFieldModule[] | undefined;
            const relations = Array.isArray(res?.relations) ? res.relations : [];
            if (Array.isArray(modules) && modules.length > 0) {
                applySourceMetadata(modules, relations);
                return;
            }
        } catch (err) {
            console.warn('Failed to fetch voucher source modules, falling back to source-fields APIs.', err);
        }

        try {
            await fetchSourceFieldsFallback();
        } catch (err) {
            console.warn('Failed to fetch voucher source-fields metadata from backend.', err);
            applySourceMetadata(EMPTY_VOUCHER_FIELD_MODULES, []);
        }
    };

    const getTriggerSourcesForTemplate = (template: VoucherTemplate) => {
        const effectiveSourceType = getEffectiveSourceType(template.source_type);
        const moduleId = String(template.source_module || '').trim()
            || inferModuleIdFromSourceType(voucherFieldModules, effectiveSourceType);
        const module = voucherFieldModules.find(m => String(m?.id) === moduleId) || voucherFieldModules[0];
        return (module?.sources || []).filter(source => Boolean(source?.root_enabled));
    };

    const getConditionRootFields = (sourceType: string): SourceFieldOption[] => {
        const normalized = String(sourceType || '').trim().toLowerCase();
        if (normalized === 'receipt_bills') return receiptBillSourceFields;
        if (normalized === 'deposit_records') return depositSourceFields;
        if (normalized === 'bills') return billSourceFields;

        const matchedSource = voucherFieldModules
            .flatMap(module => module.sources || [])
            .find(source => String(source?.source_type || '').trim().toLowerCase() === normalized);

        return normalizeSourceFields(matchedSource?.fields || []);
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

        const normalizeFieldKey = (field: string | null | undefined, preferredSourceType?: string | null) => {
            const raw = field == null ? '' : String(field).trim();
            if (!raw) return raw;
            const parts = raw.split('.').filter(Boolean);
            if (parts.length >= 3) return raw;

            const [prefix, base] =
                parts.length === 2 ? [String(parts[0] || '').trim(), String(parts[1] || '').trim()] : ['', String(parts[0] || '').trim()];

            const ctx = prefix
                ? ctxByPrefix.get(prefix.toLowerCase())
                : (preferredSourceType ? ctxByPrefix.get(String(preferredSourceType).trim().toLowerCase()) : defaultCtx) || defaultCtx;
            if (!ctx || !base || !ctx.fieldKeys.has(base)) return raw;

            const next = buildTargetKey(ctx, base);
            if (!next || raw === next) return raw;
            replaced += 1;
            return next;
        };

        const normalizeConditionTree = (trigger: string | null | undefined) => {
            const raw = trigger == null ? '' : String(trigger).trim();
            if (!raw) return raw;
            try {
                const root = JSON.parse(raw);
                const walk = (node: any, currentSourceType: string) => {
                    if (!node || typeof node !== 'object') return;
                    const t = node.type || 'group';
                    if (t === 'group') {
                        const children = Array.isArray(node.children) ? node.children : [];
                        children.forEach((child: any) => walk(child, currentSourceType));
                        return;
                    }
                    if (t === 'relation') {
                        const relationSourceType = String(node.target_source || '').trim() || currentSourceType;
                        if (Array.isArray(node.children)) {
                            node.children.forEach((child: any) => walk(child, relationSourceType));
                        } else if (node.where && typeof node.where === 'object') {
                            walk(node.where, relationSourceType);
                            if (typeof node.where.logic === 'string' && typeof node.logic !== 'string') {
                                node.logic = node.where.logic;
                            }
                            if (!Array.isArray(node.children) && Array.isArray(node.where.children)) {
                                node.children = node.where.children;
                            }
                        }
                        return;
                    }
                    if (t === 'rule') {
                        if (typeof node.field === 'string') node.field = normalizeFieldKey(node.field, currentSourceType);
                        if (node.value != null) node.value = replaceInText(String(node.value));
                    }
                };
                walk(root, effectiveSourceType);
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
            trigger_condition: normalizeConditionTree(template.trigger_condition),
            rules: (template.rules || []).map(r => ({
                ...r,
                account_code: replaceInText(r.account_code),
                display_condition_expr: normalizeConditionTree(r.display_condition_expr),
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
            category_id: null,
            book_number_expr: "{CURRENT_ACCOUNT_BOOK_NUMBER}",
            vouchertype_number_expr: "'0001'",
            attachment_expr: "0",
            bizdate_expr: "{CURRENT_DATE}",
            bookeddate_expr: "{CURRENT_DATE}",
            source_module: 'marki',
            source_type: 'bills',
            rules: [
                { line_no: 1, dr_cr: 'D', account_code: '', display_condition_expr: '', amount_expr: '', summary_expr: '', currency_expr: "'CNY'", localrate_expr: "1" },
                { line_no: 2, dr_cr: 'C', account_code: '', display_condition_expr: '', amount_expr: '', summary_expr: '', currency_expr: "'CNY'", localrate_expr: "1" }
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
            category_path: template.category_path || (template.category_id ? categoryPathMap[template.category_id] : null),
        });
        setEditingTemplateId(template.template_id);
        setIsEditing(true);
        setActiveTab('basic');
    };

    const handleCopy = async (template: VoucherTemplate, options?: { preferServer?: boolean }) => {
        setSaveErrors([]);
        let sourceTemplate = template;
        const preferServer = options?.preferServer ?? false;

        if (preferServer) {
            try {
                const res = await axios.get(`${API_BASE}/vouchers/templates/${template.template_id}`);
                sourceTemplate = normalizeTemplateFromApi(res.data, categoryPathMap, voucherFieldModules);
            } catch (err) {
                showToast('error', '获取模板详情失败', '将使用当前列表数据复制');
            }
        }

        setCurrentTemplate({
            ...sourceTemplate,
            template_id: getUniqueCopiedTemplateId(sourceTemplate.template_id, templates),
            template_name: getUniqueCopiedTemplateName(sourceTemplate.template_name, templates),
            source_module: sourceTemplate.source_module || inferModuleIdFromSourceType(voucherFieldModules, sourceTemplate.source_type),
            rules: (sourceTemplate.rules || []).map((rule, index) => ({
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
        openConfirm({
            title: '删除模板',
            message: '确定要删除这个模板吗？',
            confirmText: '删除',
            intent: 'danger',
            onConfirm: async () => {
                try {
                    await axios.delete(`${API_BASE}/vouchers/templates/${id}`);
                    showToast('success', '删除成功');
                    fetchTemplates();
                } catch (err) {
                    showToast('error', '删除失败', '请稍后重试');
                }
            }
        });
    };

    const handleSave = async () => {
        if (!currentTemplate) return;
        setSaveErrors([]);

        const normalized = normalizeTemplateFieldBindings(currentTemplate);
        const workingTemplate = normalized.replaced > 0 ? normalized.template : currentTemplate;
        if (normalized.replaced > 0) {
            setCurrentTemplate(workingTemplate);
        }

        const { category_path, ...restTemplate } = workingTemplate;
        const payload: VoucherTemplate = {
            ...restTemplate,
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
            display_condition_expr: '',
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

    const reindexRules = (rules: VoucherEntryRule[]) => (
        rules.map((rule, index) => ({ ...rule, line_no: index + 1 }))
    );

    const removeRule = (idx: number) => {
        if (!currentTemplate) return;
        const newRules = reindexRules(currentTemplate.rules.filter((_, i) => i !== idx));
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

    const openRuleDisplayCondition = (idx: number) => {
        setCurrentRuleIndex(idx);
        setDisplayConditionModalOpen(true);
    };

    const updateCurrentRuleDetail = (field: 'aux_items' | 'main_cf_assgrp' | 'display_condition_expr', value: string) => {
        if (currentRuleIndex === null) return;
        updateRule(currentRuleIndex, { [field]: value });
    };

    const moveRuleToIndex = (fromIndex: number, insertIndex: number) => {
        if (!currentTemplate) return;
        if (fromIndex < 0 || fromIndex >= currentTemplate.rules.length) return;

        const newRules = [...currentTemplate.rules];
        const [movingRule] = newRules.splice(fromIndex, 1);
        if (!movingRule) return;

        const boundedIndex = Math.max(0, Math.min(insertIndex, newRules.length));
        newRules.splice(boundedIndex, 0, movingRule);
        setCurrentTemplate({ ...currentTemplate, rules: reindexRules(newRules) });
    };

    const resetRuleDragState = () => {
        setDraggingRuleIndex(null);
        setDragOverRuleIndex(null);
        setDragOverRulePosition('before');
    };

    const getRuleDragPosition = (event: ReactDragEvent<HTMLTableRowElement>): RuleDragPosition => {
        const rect = event.currentTarget.getBoundingClientRect();
        return event.clientY < rect.top + rect.height / 2 ? 'before' : 'after';
    };

    const handleRuleDragStart = (idx: number, event: ReactDragEvent<HTMLButtonElement>) => {
        setDraggingRuleIndex(idx);
        setDragOverRuleIndex(null);
        setDragOverRulePosition('before');
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', String(idx));
    };

    const handleRuleDragOver = (idx: number, event: ReactDragEvent<HTMLTableRowElement>) => {
        if (draggingRuleIndex === null) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
        const nextPosition = getRuleDragPosition(event);
        if (dragOverRuleIndex !== idx || dragOverRulePosition !== nextPosition) {
            setDragOverRuleIndex(idx);
            setDragOverRulePosition(nextPosition);
        }
    };

    const handleRuleDrop = (idx: number, event: ReactDragEvent<HTMLTableRowElement>) => {
        if (draggingRuleIndex === null) return;
        event.preventDefault();

        const dropPosition = getRuleDragPosition(event);
        let insertIndex = idx + (dropPosition === 'after' ? 1 : 0);
        if (insertIndex > draggingRuleIndex) {
            insertIndex -= 1;
        }

        if (insertIndex !== draggingRuleIndex) {
            moveRuleToIndex(draggingRuleIndex, insertIndex);
        }
        resetRuleDragState();
    };

    const handleRuleTailDragOver = (event: ReactDragEvent<HTMLTableRowElement>) => {
        if (draggingRuleIndex === null || !currentTemplate) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
        const tailIndex = currentTemplate.rules.length;
        if (dragOverRuleIndex !== tailIndex || dragOverRulePosition !== 'after') {
            setDragOverRuleIndex(tailIndex);
            setDragOverRulePosition('after');
        }
    };

    const handleRuleTailDrop = (event: ReactDragEvent<HTMLTableRowElement>) => {
        if (draggingRuleIndex === null || !currentTemplate) return;
        event.preventDefault();
        const insertIndex = currentTemplate.rules.length - 1;
        if (insertIndex >= 0 && insertIndex !== draggingRuleIndex) {
            moveRuleToIndex(draggingRuleIndex, currentTemplate.rules.length);
        }
        resetRuleDragState();
    };

    const handleRuleDragEnd = () => {
        resetRuleDragState();
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
                                        <label>模板分类</label>
                                        <div className="category-picker-field">
                                            <input
                                                type="text"
                                                readOnly
                                                value={currentTemplate.category_id ? (currentTemplate.category_path || categoryPathMap[currentTemplate.category_id] || '') : ''}
                                                placeholder="未分类"
                                            />
                                            <button
                                                type="button"
                                                className="category-picker-trigger"
                                                onClick={() => setCategoryPickerOpen(true)}
                                                title="选择分类"
                                            >
                                                <Search size={16} />
                                            </button>
                                            {currentTemplate.category_id && (
                                                <button
                                                    type="button"
                                                    className="category-picker-clear"
                                                    onClick={() => setCurrentTemplate({ ...currentTemplate, category_id: null, category_path: null })}
                                                    title="清除分类"
                                                >
                                                    <X size={14} />
                                                </button>
                                            )}
                                        </div>
                                    </div>
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
                            {(() => {
                                const triggerSources = getTriggerSourcesForTemplate(currentTemplate);
                                return triggerSources.length > 0 ? (
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
                                                        {triggerSources.map(source => (
                                                            <option key={`${source.id}:${source.source_type}`} value={String(source.source_type || '')}>
                                                                {source.label} ({source.source_type})
                                                            </option>
                                                        ))}
                                                    </select>
                                                </div>
                                                <div style={{ fontSize: '0.75rem', color: '#94a3b8', paddingBottom: '0.25rem' }}>
                                                    仅用于触发条件判断，不限制分录取数来源
                                                </div>
                                            </div>
                                            {getEffectiveSourceType(currentTemplate.source_type) === 'receipt_bills' && (
                                                <div style={{
                                                    marginBottom: '1rem',
                                                    padding: '0.75rem 1rem',
                                                    border: '1px solid #fcd34d',
                                                    background: '#fffbeb',
                                                    color: '#92400e',
                                                    borderRadius: '0.75rem',
                                                    fontSize: '0.875rem'
                                                }}>
                                                    当前已启用“收款账单根数据源”模式。你可以在下方条件树中直接点击“添加关联条件”，把子条件切到“运营账单”或“押金记录”。
                                                </div>
                                            )}
                                            <ConditionBuilder
                                                value={currentTemplate.trigger_condition}
                                                onChange={(val) => setCurrentTemplate({ ...currentTemplate, trigger_condition: val })}
                                                fields={getConditionRootFields(getEffectiveSourceType(currentTemplate.source_type))}
                                                fieldModules={voucherFieldModules}
                                                rootSourceType={getEffectiveSourceType(currentTemplate.source_type)}
                                                relationOptions={relationOptions}
                                            />
                                        </div>
                                    </div>
                                ) : (
                                    <div className="editor-card animate-slide-up" style={{ padding: '3rem', textAlign: 'center', color: '#64748b' }}>
                                        <AlertTriangle size={48} style={{ margin: '0 auto 1rem', opacity: 0.5 }} />
                                        <h3>当前业务模块不支持触发条件</h3>
                                        <p>请选择支持触发条件的业务模块（例如：马克系统）。</p>
                                    </div>
                                );
                            })()}
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
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            placeholder="'BU-CODE'"
                                            editorTitle="编辑账簿编码映射"
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>凭证字映射</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.vouchertype_number_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, vouchertype_number_expr: val })}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            placeholder="'0001'"
                                            editorTitle="编辑凭证字映射"
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>附件数量表达式</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.attachment_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, attachment_expr: val })}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            placeholder="0"
                                            editorTitle="编辑附件数量表达式"
                                        />
                                    </div>
                                </div>
                                <div className="field-grid-two">
                                    <div className="field-item">
                                        <label>业务日期表达式 (bizdate)</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.bizdate_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, bizdate_expr: val })}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            editorTitle="编辑业务日期表达式"
                                        />
                                    </div>
                                    <div className="field-item">
                                        <label>记账日期表达式 (bookeddate)</label>
                                        <ExpressionInputWithActions
                                            value={currentTemplate.bookeddate_expr}
                                            onChange={val => setCurrentTemplate({ ...currentTemplate, bookeddate_expr: val })}
                                            fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            editorTitle="编辑记账日期表达式"
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
                                            <th style={{ width: '72px' }}>拖拽</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {currentTemplate.rules.map((rule, idx) => (
                                            <tr
                                                key={idx}
                                                className={[
                                                    'rule-row',
                                                    draggingRuleIndex === idx ? 'is-dragging' : '',
                                                    draggingRuleIndex !== idx && dragOverRuleIndex === idx && dragOverRulePosition === 'before' ? 'drop-before' : '',
                                                    draggingRuleIndex !== idx && dragOverRuleIndex === idx && dragOverRulePosition === 'after' ? 'drop-after' : '',
                                                ].filter(Boolean).join(' ')}
                                                onDragOver={(event) => handleRuleDragOver(idx, event)}
                                                onDrop={(event) => handleRuleDrop(idx, event)}
                                            >
                                                <td>{rule.line_no}</td>
                                                <td>
                                                    <ExpressionInputWithActions
                                                        value={rule.summary_expr}
                                                        onChange={val => updateRule(idx, { summary_expr: val })}
                                                        fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                                        editorTitle={`编辑摘要表达式 - 第 ${rule.line_no} 行`}
                                                    />
                                                </td>
                                                <td>
                                                    <AccountSelector
                                                        value={rule.account_code}
                                                        onChange={(val) => handleAccountChange(idx, val)}
                                                        subjects={subjects}
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
                                                        fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                                        editorTitle={`编辑金额表达式 - 第 ${rule.line_no} 行`}
                                                    />
                                                </td>
                                                <td>
                                                    <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                                                        <button
                                                            className={`detail-config-btn-large condition-config-btn ${rule.display_condition_expr?.trim() ? 'is-active' : ''}`}
                                                            onClick={() => openRuleDisplayCondition(idx)}
                                                            title={rule.display_condition_expr?.trim() ? '编辑显示条件（已配置）' : '配置显示条件'}
                                                        >
                                                            <ToggleRight size={16} />
                                                        </button>
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
                                                    <div className="rule-drag-cell">
                                                        <button
                                                            type="button"
                                                            className="rule-drag-handle"
                                                            draggable
                                                            onDragStart={(event) => handleRuleDragStart(idx, event)}
                                                            onDragEnd={handleRuleDragEnd}
                                                            title="拖动调整分录顺序"
                                                        >
                                                            <GripVertical size={16} />
                                                        </button>
                                                    </div>
                                                </td>
                                            </tr>
                                        ))}
                                        {draggingRuleIndex !== null && (
                                            <tr
                                                className={`rule-drop-tail ${dragOverRuleIndex === currentTemplate.rules.length ? 'is-active' : ''}`}
                                                onDragOver={handleRuleTailDragOver}
                                                onDrop={handleRuleTailDrop}
                                            >
                                                <td colSpan={7}>
                                                    拖到这里，放到最后
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}
                </div >

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
                                                fieldModules={filterModulesByModuleId(voucherFieldModules, currentTemplate.source_module)}
                                            />
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    )
                }

                {
                    displayConditionModalOpen && currentRuleIndex !== null && currentTemplate.rules[currentRuleIndex] && (
                        <div className="rule-detail-overlay">
                            <div className="rule-detail-modal display-condition-modal glass-card">
                                <div className="modal-header">
                                    <h3>分录行 #{currentTemplate.rules[currentRuleIndex].line_no} 显示条件</h3>
                                    <button onClick={() => setDisplayConditionModalOpen(false)}><X size={20} /></button>
                                </div>

                                <div className="display-condition-body">
                                    {(() => {
                                        const effectiveSourceType = getEffectiveSourceType(currentTemplate.source_type);
                                        const triggerSources = getTriggerSourcesForTemplate(currentTemplate);
                                        return triggerSources.length > 0 ? (
                                            <>
                                                <div className="info-box">
                                                    <Info size={14} />
                                                    <span>
                                                        配置当前分录的显示条件。<br />
                                                        留空表示默认显示；设置条件后，只有命中条件时才生成该分录。
                                                    </span>
                                                </div>
                                                {effectiveSourceType === 'receipt_bills' && (
                                                    <div style={{
                                                        marginBottom: '1rem',
                                                        padding: '0.75rem 1rem',
                                                        border: '1px solid #fcd34d',
                                                        background: '#fffbeb',
                                                        color: '#92400e',
                                                        borderRadius: '0.75rem',
                                                        fontSize: '0.875rem'
                                                    }}>
                                                        当前为“收款账单根数据源”模式。你可以像模板触发条件一样，通过“添加关联条件”切换到运营账单或押金记录做显示判断。
                                                    </div>
                                                )}
                                                <ConditionBuilder
                                                    value={currentTemplate.rules[currentRuleIndex].display_condition_expr || ''}
                                                    onChange={(val) => updateCurrentRuleDetail('display_condition_expr', val)}
                                                    fields={getConditionRootFields(effectiveSourceType)}
                                                    fieldModules={voucherFieldModules}
                                                    rootSourceType={effectiveSourceType}
                                                    relationOptions={relationOptions}
                                                />
                                            </>
                                        ) : (
                                            <div className="editor-card animate-slide-up" style={{ padding: '3rem', textAlign: 'center', color: '#64748b' }}>
                                                <AlertTriangle size={48} style={{ margin: '0 auto 1rem', opacity: 0.5 }} />
                                                <h3>当前业务模块不支持显示条件</h3>
                                                <p>请选择支持条件判断的数据源模块后再配置。</p>
                                            </div>
                                        );
                                    })()}
                                </div>
                            </div>
                        </div>
                    )
                }

                {categoryPickerOpen && (
                    <div className="category-picker-overlay" onClick={() => setCategoryPickerOpen(false)}>
                        <div className="category-picker-modal glass-card" onClick={e => e.stopPropagation()}>
                            <div className="modal-header">
                                <h3>选择模板分类</h3>
                                <button onClick={() => setCategoryPickerOpen(false)}><X size={20} /></button>
                            </div>
                            <div className="category-picker-body">
                                <div className="category-tree">
                                    {templateCategories.length === 0 ? (
                                        <div className="category-empty">暂无模板分类</div>
                                    ) : (
                                        templateCategories.map(category => (
                                            <CategoryPickerNode
                                                key={category.id}
                                                node={category}
                                                level={0}
                                                selectedId={pendingCategoryId}
                                                onSelect={(node) => setPendingCategoryId(node.id)}
                                            />
                                        ))
                                    )}
                                </div>
                                <div className="category-picker-footer">
                                    <div className="category-selected">
                                        已选：{pendingCategoryId ? (categoryPathMap[pendingCategoryId] || '') : '未分类'}
                                    </div>
                                    <div className="category-actions">
                                        <button
                                            type="button"
                                            className="btn btn-outline"
                                            onClick={() => setPendingCategoryId(null)}
                                        >
                                            设为未分类
                                        </button>
                                        <button
                                            type="button"
                                            className="btn btn-outline"
                                            onClick={() => setCategoryPickerOpen(false)}
                                        >
                                            取消
                                        </button>
                                        <button
                                            type="button"
                                            className="btn btn-primary"
                                            onClick={() => {
                                                const nextId = pendingCategoryId ?? null;
                                                setCurrentTemplate({
                                                    ...currentTemplate,
                                                    category_id: nextId,
                                                    category_path: nextId ? categoryPathMap[nextId] : null,
                                                });
                                                setCategoryPickerOpen(false);
                                            }}
                                            disabled={pendingCategoryId !== null && !categoryLeafSet.has(pendingCategoryId)}
                                        >
                                            确定
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                )}
            </div >
        );
    }

    return (
        <div className="templates-page">
            <div className="templates-frame">
                <div className="templates-frame-header">
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
                    <div className="templates-content">
                        <div className="templates-layout">
                            <aside className="templates-sidebar">
                                <div className="sidebar-card">
                                    <div className="sidebar-title">分类导航</div>
                                    <button
                                        className={`sidebar-item ${categoryFilter === 'all' ? 'active' : ''}`}
                                        onClick={() => setCategoryFilter('all')}
                                    >
                                        全部
                                    </button>
                                    <button
                                        className={`sidebar-item ${categoryFilter === 'uncategorized' ? 'active' : ''}`}
                                        onClick={() => setCategoryFilter('uncategorized')}
                                    >
                                        未分类
                                    </button>
                                    <div className="category-nav-tree">
                                        {templateCategories.length === 0 ? (
                                            <div className="category-empty">暂无模板分类</div>
                                        ) : (
                                            templateCategories.map(category => (
                                                <CategoryNavNode
                                                    key={category.id}
                                                    node={category}
                                                    level={0}
                                                    selectedId={Number.isFinite(Number(categoryFilter)) ? Number(categoryFilter) : null}
                                                    onSelect={(id) => setCategoryFilter(String(id))}
                                                />
                                            ))
                                        )}
                                    </div>
                                </div>
                            </aside>
                            <div className="templates-main">
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
                            <div className="templates-toolbar">
                                <div className="toolbar-left">
                                    <div className="search-box">
                                        <Search size={16} />
                                        <input
                                            type="text"
                                            placeholder="搜索模板名称 / ID / 描述"
                                            value={searchText}
                                            onChange={e => setSearchText(e.target.value)}
                                        />
                                    </div>
                                    <div className="sort-box">
                                        <label>排序</label>
                                        <select value={sortKey} onChange={e => setSortKey(e.target.value as typeof sortKey)}>
                                            <option value="priority_desc">优先级 高→低</option>
                                            <option value="priority_asc">优先级 低→高</option>
                                            <option value="name_asc">名称 A→Z</option>
                                            <option value="name_desc">名称 Z→A</option>
                                        </select>
                                    </div>
                                </div>
                                <div className="view-toggle">
                                    <button
                                        type="button"
                                        className={viewMode === 'card' ? 'active' : ''}
                                        onClick={() => setViewMode('card')}
                                        title="卡片视图"
                                    >
                                        <LayoutGrid size={16} />
                                    </button>
                                    <button
                                        type="button"
                                        className={viewMode === 'list' ? 'active' : ''}
                                        onClick={() => setViewMode('list')}
                                        title="列表视图"
                                    >
                                        <List size={16} />
                                    </button>
                                </div>
                                <div className={`batch-bar ${selectedCount > 0 ? 'active' : ''}`}>
                                    <div className="batch-info">
                                        <span className="summary-label">当前分类</span>
                                        <span className="summary-chip">{currentFilterLabel}</span>
                                        <span className="summary-count">共 {displayedTemplates.length} 个模板</span>
                                    </div>
                                    <div className="batch-actions">
                                        {selectedCount > 0 && (
                                            <span className="batch-count">已选 {selectedCount} 项</span>
                                        )}
                                        <div className="batch-menu" ref={batchMenuRef}>
                                            <button
                                                type="button"
                                                className="btn btn-outline batch-menu-button"
                                                onClick={() => setBatchMenuOpen(prev => !prev)}
                                            >
                                                批量操作
                                                <ChevronDown size={14} />
                                            </button>
                                            {batchMenuOpen && (
                                                <div className="batch-menu-list">
                                                    <button
                                                        type="button"
                                                        className="batch-menu-item"
                                                        onClick={() => {
                                                            handleToggleSelectAll();
                                                            setBatchMenuOpen(false);
                                                        }}
                                                        disabled={pagedTemplates.length === 0}
                                                    >
                                                        <span className="menu-left">
                                                            {isAllSelected ? <Square size={14} /> : <CheckSquare size={14} />}
                                                            {isAllSelected ? '取消全选' : '全选当前页'}
                                                        </span>
                                                        <span className="menu-shortcut">Alt+A</span>
                                                    </button>
                                                    <button
                                                        type="button"
                                                        className="batch-menu-item"
                                                        onClick={() => {
                                                            setSelectedTemplateIds(new Set());
                                                            setBatchMenuOpen(false);
                                                        }}
                                                        disabled={selectedCount === 0}
                                                    >
                                                        <span className="menu-left">
                                                            <Square size={14} />
                                                            清空选择
                                                        </span>
                                                        <span className="menu-shortcut">Alt+C</span>
                                                    </button>
                                                    <div className="batch-menu-divider" />
                                                    <button
                                                        type="button"
                                                        className="batch-menu-item"
                                                        onClick={() => {
                                                            handleBatchSetActive(true);
                                                            setBatchMenuOpen(false);
                                                        }}
                                                        disabled={selectedCount === 0}
                                                    >
                                                        <span className="menu-left">
                                                            <ToggleRight size={14} />
                                                            批量启用
                                                        </span>
                                                        <span className="menu-shortcut">Alt+E</span>
                                                    </button>
                                                    <button
                                                        type="button"
                                                        className="batch-menu-item"
                                                        onClick={() => {
                                                            handleBatchSetActive(false);
                                                            setBatchMenuOpen(false);
                                                        }}
                                                        disabled={selectedCount === 0}
                                                    >
                                                        <span className="menu-left">
                                                            <ToggleLeft size={14} />
                                                            批量停用
                                                        </span>
                                                        <span className="menu-shortcut">Alt+D</span>
                                                    </button>
                                                    <button
                                                        type="button"
                                                        className="batch-menu-item"
                                                        onClick={() => {
                                                            handleBatchCopy();
                                                            setBatchMenuOpen(false);
                                                        }}
                                                        disabled={selectedCount === 0}
                                                    >
                                                        <span className="menu-left">
                                                            <Copy size={14} />
                                                            批量复制
                                                        </span>
                                                        <span className="menu-shortcut">Alt+P</span>
                                                    </button>
                                                    <button
                                                        type="button"
                                                        className="batch-menu-item danger"
                                                        onClick={() => {
                                                            handleBatchDelete();
                                                            setBatchMenuOpen(false);
                                                        }}
                                                        disabled={selectedCount === 0}
                                                    >
                                                        <span className="menu-left">
                                                            <Trash2 size={14} />
                                                            批量删除
                                                        </span>
                                                        <span className="menu-shortcut">Alt+X</span>
                                                    </button>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="templates-list-body" ref={templatesListRef}>
                                {viewMode === 'card' ? (
                                    <div className="templates-grid">
                                        {pagedTemplates.map(t => (
                                            <div
                                                key={t.template_id}
                                                className={`template-card glass-card cursor-pointer group ${selectedTemplateIds.has(t.template_id) ? 'selected' : ''}`}
                                                onClick={() => handleEdit(t)}
                                            >
                                                <div className="template-select" onClick={e => e.stopPropagation()}>
                                                    <label>
                                                        <input
                                                            type="checkbox"
                                                            checked={selectedTemplateIds.has(t.template_id)}
                                                            onChange={() => toggleTemplateSelected(t.template_id)}
                                                        />
                                                    </label>
                                                </div>
                                                <div className="card-badge">{`${t.business_type} · P${t.priority}`}</div>
                                                <div className="card-content">
                                                    <h3>{t.template_name}</h3>
                                                    <p>{t.description || '暂无描述'}</p>
                                                    <div className="card-stats">
                                                        <span><Layers size={14} /> {t.rules.length} 条分录规则</span>
                                                        <span><FileText size={14} /> {t.template_id}</span>
                                                        <span>分类: {t.category_path || (t.category_id ? categoryPathMap[t.category_id] : '未分类')}</span>
                                                        <span>{t.active ? '启用' : '停用'}</span>
                                                    </div>
                                                </div>
                                                <div className="card-actions">
                                                <button onClick={(e) => { e.stopPropagation(); handleCopy(t, { preferServer: true }); }} className="copy-btn"><Copy size={14} />复制</button>
                                                    <button onClick={(e) => { e.stopPropagation(); handleEdit(t); }} className="edit-btn">编辑</button>
                                                    <button onClick={(e) => { e.stopPropagation(); handleDelete(t.template_id); }} className="delete-btn"><Trash2 size={16} /></button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="templates-list">
                                        <div className="templates-list-header">
                                            <div className="col-check" />
                                            <div className="col-name">模板</div>
                                            <div className="col-category">分类</div>
                                            <div className="col-meta">优先级 / 状态</div>
                                            <div className="col-actions">操作</div>
                                        </div>
                                        {pagedTemplates.map(t => (
                                            <div
                                                key={t.template_id}
                                                className={`templates-list-row ${selectedTemplateIds.has(t.template_id) ? 'selected' : ''}`}
                                            >
                                                <div className="col-check">
                                                    <input
                                                        type="checkbox"
                                                        checked={selectedTemplateIds.has(t.template_id)}
                                                        onChange={() => toggleTemplateSelected(t.template_id)}
                                                    />
                                                </div>
                                                <div className="col-name" onClick={() => handleEdit(t)}>
                                                    <div className="row-title">{t.template_name}</div>
                                                    <div className="row-sub">
                                                        <span><FileText size={12} /> {t.template_id}</span>
                                                        <span><Layers size={12} /> {t.rules.length} 条</span>
                                                    </div>
                                                    {t.description && <div className="row-desc">{t.description}</div>}
                                                </div>
                                                <div className="col-category">
                                                    {t.category_path || (t.category_id ? categoryPathMap[t.category_id] : '未分类')}
                                                </div>
                                                <div className="col-meta">
                                                    <span className="meta-pill">P{t.priority}</span>
                                                    <span className={`status-pill ${t.active ? 'active' : 'inactive'}`}>{t.active ? '启用' : '停用'}</span>
                                                </div>
                                                <div className="col-actions">
                                                <button onClick={() => handleCopy(t, { preferServer: true })} className="copy-btn">复制</button>
                                                    <button onClick={() => handleEdit(t)} className="edit-btn">编辑</button>
                                                    <button onClick={() => handleDelete(t.template_id)} className="delete-btn"><Trash2 size={14} /></button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                                {displayedTemplates.length === 0 && !loadError && (
                                    <div className="empty-state">
                                        <Info size={40} />
                                        <p>{searchText.trim() ? '未找到匹配的模板' : (categoryFilter === 'all' ? '尚未创建任何模板' : '暂无符合该分类的模板')}</p>
                                        {categoryFilter === 'all' && !searchText.trim() && (
                                            <button onClick={handleCreate}>立即创建</button>
                                        )}
                                    </div>
                                )}
                            </div>
                            {displayedTemplates.length > 0 && (
                                <div className="templates-pagination">
                                    <div className="page-info">
                                        共 {displayedTemplates.length} 条，第 {currentPage} / {totalPages} 页
                                    </div>
                                    <div className="page-controls">
                                        <button
                                            className="page-button"
                                            onClick={() => setCurrentPage(1)}
                                            disabled={currentPage === 1}
                                        >
                                            首页
                                        </button>
                                        <button
                                            className="page-button"
                                            onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                                            disabled={currentPage === 1}
                                        >
                                            上一页
                                        </button>
                                        <span className="page-number">{currentPage}</span>
                                        <button
                                            className="page-button"
                                            onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                                            disabled={currentPage === totalPages}
                                        >
                                            下一页
                                        </button>
                                        <button
                                            className="page-button"
                                            onClick={() => setCurrentPage(totalPages)}
                                            disabled={currentPage === totalPages}
                                        >
                                            末页
                                        </button>
                                    </div>
                                    <div className="page-size">
                                        <label>每页</label>
                                        <select
                                            value={pageSize}
                                            onChange={e => {
                                                setPageSize(Number(e.target.value));
                                                setCurrentPage(1);
                                            }}
                                        >
                                            <option value={8}>8</option>
                                            <option value={12}>12</option>
                                            <option value={20}>20</option>
                                            <option value={40}>40</option>
                                        </select>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
        <ToastContainer toasts={toasts} removeToast={removeToast} />
        <ConfirmModal
            isOpen={confirmState.open}
            title={confirmState.title}
            message={confirmState.message}
            confirmText={confirmState.confirmText}
            variant={confirmState.intent}
            loading={confirmState.loading}
            onConfirm={handleConfirm}
            onCancel={closeConfirm}
        />
    </div>
    );
};

export default VoucherTemplates;

