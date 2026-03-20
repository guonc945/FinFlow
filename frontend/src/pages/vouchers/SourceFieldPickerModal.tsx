import { useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { Database, Search, X } from 'lucide-react';
import type { VoucherFieldModule, VoucherFieldSource, VoucherSourceFieldOption } from '../../types';
import { getSourceFieldDisplayCode, getUnifiedSourceFieldLabel } from './sourceFieldLabelUtils';
import './SourceFieldPickerModal.css';

type PickContext = {
    module_id: string;
    source_id: string;
    source_type: string;
};

const normalizeText = (s: unknown) => String(s ?? '').toLowerCase().trim();

const groupBy = (fields: VoucherSourceFieldOption[]) => {
    const groups: Record<string, VoucherSourceFieldOption[]> = {};
    fields.forEach(f => {
        const g = String(f.group || '其他').trim() || '其他';
        if (!groups[g]) groups[g] = [];
        groups[g]!.push(f);
    });
    return groups;
};

const firstNonEmpty = <T,>(arr: T[] | undefined | null) => (Array.isArray(arr) && arr.length > 0 ? arr[0] : null);

type RelatedArchiveView = 'all' | 'customer' | 'supplier' | 'house' | 'project' | 'bank' | 'other';

const isRelatedArchiveField = (field: VoucherSourceFieldOption) => {
    const groupName = String(field.group || '').trim();
    const fieldValue = String(field.value || '').trim().toLowerCase();

    if (fieldValue.startsWith('kd_')) return true;
    if (groupName === '金蝶关联' || groupName === '银行账户') return true;
    if (groupName.includes('档案')) return true;
    if (groupName.includes('关联') && groupName !== '关联ID') return true;
    return false;
};

const getRelatedArchiveCategory = (field: VoucherSourceFieldOption): Exclude<RelatedArchiveView, 'all'> => {
    const groupName = String(field.group || '').trim();
    const fieldValue = String(field.value || '').trim().toLowerCase();
    const fieldLabel = getUnifiedSourceFieldLabel(field);

    if (groupName === '银行账户' || fieldValue.includes('_bank_') || fieldLabel.includes('银行')) {
        return 'bank';
    }
    if (fieldValue.includes('supplier') || fieldLabel.includes('供应商')) {
        return 'supplier';
    }
    if (fieldValue.includes('customer') || fieldLabel.includes('客户')) {
        return 'customer';
    }
    if (
        fieldValue.includes('park_house')
        || fieldValue.includes('house')
        || fieldLabel.includes('房号')
        || fieldLabel.includes('车位映射房号')
    ) {
        return 'house';
    }
    if (fieldValue.includes('project') || fieldLabel.includes('项目')) {
        return 'project';
    }
    return 'other';
};

export default function SourceFieldPickerModal({
    open,
    onClose,
    modules,
    title = '选择数据源字段',
    onPick,
}: {
    open: boolean;
    onClose: () => void;
    modules: VoucherFieldModule[];
    title?: string;
    onPick: (field: VoucherSourceFieldOption, ctx: PickContext) => void;
}) {
    const [activeModuleId, setActiveModuleId] = useState<string>('');
    const [activeSourceId, setActiveSourceId] = useState<string>('');
    const [query, setQuery] = useState<string>('');
    const [fieldView, setFieldView] = useState<'all' | 'primary' | 'related'>('all');
    const [relatedView, setRelatedView] = useState<RelatedArchiveView>('all');

    const activeModule = useMemo(() => {
        const fallback = firstNonEmpty(modules);
        const found = modules.find(m => m.id === activeModuleId) || fallback;
        return found || null;
    }, [modules, activeModuleId]);

    const activeSource = useMemo(() => {
        if (!activeModule) return null;
        const fallback = firstNonEmpty(activeModule.sources);
        const found = activeModule.sources.find(s => s.id === activeSourceId) || fallback;
        return found || null;
    }, [activeModule, activeSourceId]);

    // Initialize defaults whenever module/source list changes.
    useEffect(() => {
        if (!open) return;
        const firstModule = firstNonEmpty(modules);
        if (!firstModule) return;
        setActiveModuleId(prev => prev || firstModule.id);
        const firstSource = firstNonEmpty(firstModule.sources);
        if (firstSource) setActiveSourceId(prev => prev || firstSource.id);
    }, [open, modules]);

    // Reset search when switching context.
    useEffect(() => {
        if (!open) return;
        setQuery('');
        setFieldView('all');
        setRelatedView('all');
    }, [open, activeModuleId, activeSourceId]);

    useEffect(() => {
        if (!open) return;
        const onKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [open, onClose]);

    useEffect(() => {
        if (!open) return;
        const prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.body.style.overflow = prevOverflow;
        };
    }, [open]);

    const filteredFields = useMemo(() => {
        const list = (activeSource?.fields || []) as VoucherSourceFieldOption[];
        const q = normalizeText(query);
        if (!q) return list;
        return list.filter(f => {
            const hay = `${getUnifiedSourceFieldLabel(f)} ${f.value} ${f.group || ''}`;
            return normalizeText(hay).includes(q);
        });
    }, [activeSource, query]);

    const primaryFields = useMemo(
        () => filteredFields.filter(field => !isRelatedArchiveField(field)),
        [filteredFields]
    );

    const relatedFields = useMemo(
        () => filteredFields.filter(field => isRelatedArchiveField(field)),
        [filteredFields]
    );

    const relatedFieldsFiltered = useMemo(() => {
        if (relatedView === 'all') return relatedFields;
        return relatedFields.filter(field => getRelatedArchiveCategory(field) === relatedView);
    }, [relatedFields, relatedView]);

    const groupedPrimary = useMemo(() => groupBy(primaryFields), [primaryFields]);
    const groupedRelated = useMemo(() => groupBy(relatedFieldsFiltered), [relatedFieldsFiltered]);

    const getFieldKey = (field: VoucherSourceFieldOption) => {
        const baseKey = String(field?.value || '').trim();
        if (!baseKey) return baseKey;

        const mid = String(activeModule?.id || '').trim();
        const sid = String(activeSource?.id || '').trim();
        if (mid && sid) return `${mid}.${sid}.${baseKey}`;

        const st = String(activeSource?.source_type || '').trim();
        return st ? `${st}.${baseKey}` : baseKey;
    };

    if (!open) return null;

    const renderFieldGroups = (
        groupedFields: Record<string, VoucherSourceFieldOption[]>,
        emptyText: string,
    ) => {
        const entries = Object.entries(groupedFields);
        if (entries.length === 0) {
            return <div className="sfpm-empty">{emptyText}</div>;
        }

        return (
            <div className="sfpm-groups">
                {entries.map(([groupName, items]) => (
                    <div key={groupName} className="sfpm-group">
                        <div className="sfpm-group-title">{groupName}</div>
                        <div className="sfpm-group-items">
                            {items.map(f => (
                                <button
                                    key={`${groupName}:${f.value}`}
                                    type="button"
                                    className="sfpm-field"
                                    onClick={() => {
                                        onPick(f, {
                                            module_id: activeModule?.id || '',
                                            source_id: activeSource!.id,
                                            source_type: activeSource!.source_type,
                                        });
                                    }}
                                    title={`${getFieldKey(f)} (raw=${f.value})`}
                                >
                                    <span className="sfpm-field-label">{getUnifiedSourceFieldLabel(f)}</span>
                                    <code className="sfpm-field-key">{getSourceFieldDisplayCode(f)}</code>
                                </button>
                            ))}
                        </div>
                    </div>
                ))}
            </div>
        );
    };

    const modalNode = (
        <div className="sfpm-overlay" onMouseDown={(e) => {
            if (e.target === e.currentTarget) onClose();
        }}>
            <div className="sfpm-modal" role="dialog" aria-modal="true" aria-label={title}>
                <div className="sfpm-header">
                    <div className="sfpm-title">
                        <Database size={16} />
                        <span>{title}</span>
                    </div>
                    <button className="sfpm-close" type="button" onClick={onClose} title="关闭">
                        <X size={16} />
                    </button>
                </div>

                <div className="sfpm-body">
                    <div className="sfpm-col sfpm-modules">
                        <div className="sfpm-col-title">模块</div>
                        <div className="sfpm-list">
                            {modules.map(m => (
                                <button
                                    key={m.id}
                                    type="button"
                                    className={`sfpm-item ${activeModule?.id === m.id ? 'active' : ''}`}
                                    onClick={() => {
                                        setActiveModuleId(m.id);
                                        const firstSource = firstNonEmpty(m.sources);
                                        setActiveSourceId(firstSource?.id || '');
                                    }}
                                    title={m.note || m.label}
                                >
                                    <span className="sfpm-item-label">{m.label}</span>
                                    {m.note && <span className="sfpm-item-note">{m.note}</span>}
                                </button>
                            ))}
                        </div>
                    </div>

                    <div className="sfpm-col sfpm-sources">
                        <div className="sfpm-col-title">数据模型</div>
                        <div className="sfpm-list">
                            {(activeModule?.sources || []).map((s: VoucherFieldSource) => (
                                <button
                                    key={s.id}
                                    type="button"
                                    className={`sfpm-item ${activeSource?.id === s.id ? 'active' : ''}`}
                                    onClick={() => setActiveSourceId(s.id)}
                                    title={s.source_type}
                                >
                                    <span className="sfpm-item-label">{s.label}</span>
                                    <span className="sfpm-item-note">{s.source_type}</span>
                                </button>
                            ))}
                        </div>
                    </div>

                    <div className="sfpm-col sfpm-fields">
                        <div className="sfpm-search">
                            <Search size={14} />
                            <input
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                                placeholder="搜索字段名 / 键名 / 分组..."
                                autoFocus
                            />
                        </div>
                        <div className="sfpm-field-toolbar">
                            <div className="sfpm-view-switch" role="tablist" aria-label="字段视图切换">
                                <button
                                    type="button"
                                    className={`sfpm-view-btn ${fieldView === 'all' ? 'active' : ''}`}
                                    onClick={() => setFieldView('all')}
                                >
                                    全部
                                </button>
                                <button
                                    type="button"
                                    className={`sfpm-view-btn ${fieldView === 'primary' ? 'active' : ''}`}
                                    onClick={() => setFieldView('primary')}
                                >
                                    本模型
                                </button>
                                <button
                                    type="button"
                                    className={`sfpm-view-btn ${fieldView === 'related' ? 'active' : ''}`}
                                    onClick={() => setFieldView('related')}
                                >
                                    关联档案
                                </button>
                            </div>
                        </div>

                        {!activeSource ? (
                            <div className="sfpm-empty">暂无字段</div>
                        ) : filteredFields.length === 0 ? (
                            <div className="sfpm-empty">未找到匹配字段</div>
                        ) : (
                            <div className="sfpm-field-panels">
                                <div className={`sfpm-field-panel ${fieldView === 'related' ? 'is-hidden' : ''}`}>
                                    <div className="sfpm-field-panel-title">
                                        <span>本模型字段</span>
                                        <span className="sfpm-field-panel-count">{primaryFields.length}</span>
                                    </div>
                                    {renderFieldGroups(groupedPrimary, '当前搜索结果中没有本模型字段')}
                                </div>
                                <div className={`sfpm-field-panel ${fieldView === 'primary' ? 'is-hidden' : ''}`}>
                                    <div className="sfpm-field-panel-title">
                                        <span>关联档案字段</span>
                                        <span className="sfpm-field-panel-count">{relatedFields.length}</span>
                                    </div>
                                    <div className="sfpm-related-toolbar">
                                        <div className="sfpm-related-switch" role="tablist" aria-label="关联档案字段分类切换">
                                            <button
                                                type="button"
                                                className={`sfpm-related-btn ${relatedView === 'all' ? 'active' : ''}`}
                                                onClick={() => setRelatedView('all')}
                                            >
                                                全部
                                            </button>
                                            <button
                                                type="button"
                                                className={`sfpm-related-btn ${relatedView === 'customer' ? 'active' : ''}`}
                                                onClick={() => setRelatedView('customer')}
                                            >
                                                客户
                                            </button>
                                            <button
                                                type="button"
                                                className={`sfpm-related-btn ${relatedView === 'supplier' ? 'active' : ''}`}
                                                onClick={() => setRelatedView('supplier')}
                                            >
                                                供应商
                                            </button>
                                            <button
                                                type="button"
                                                className={`sfpm-related-btn ${relatedView === 'house' ? 'active' : ''}`}
                                                onClick={() => setRelatedView('house')}
                                            >
                                                房号
                                            </button>
                                            <button
                                                type="button"
                                                className={`sfpm-related-btn ${relatedView === 'project' ? 'active' : ''}`}
                                                onClick={() => setRelatedView('project')}
                                            >
                                                管理项目
                                            </button>
                                            <button
                                                type="button"
                                                className={`sfpm-related-btn ${relatedView === 'bank' ? 'active' : ''}`}
                                                onClick={() => setRelatedView('bank')}
                                            >
                                                银行
                                            </button>
                                            <button
                                                type="button"
                                                className={`sfpm-related-btn ${relatedView === 'other' ? 'active' : ''}`}
                                                onClick={() => setRelatedView('other')}
                                            >
                                                其他关联
                                            </button>
                                        </div>
                                    </div>
                                    {renderFieldGroups(groupedRelated, '当前筛选下没有关联档案字段')}
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );

    const portalTarget = typeof document !== 'undefined' ? document.body : null;
    return portalTarget ? createPortal(modalNode, portalTarget) : modalNode;
}
