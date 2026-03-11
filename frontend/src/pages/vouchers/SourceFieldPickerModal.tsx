import { useEffect, useMemo, useState } from 'react';
import { Database, Search, X } from 'lucide-react';
import type { VoucherFieldModule, VoucherFieldSource, VoucherSourceFieldOption } from '../../types';
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
    }, [open, activeModuleId, activeSourceId]);

    useEffect(() => {
        if (!open) return;
        const onKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [open, onClose]);

    const filteredFields = useMemo(() => {
        const list = (activeSource?.fields || []) as VoucherSourceFieldOption[];
        const q = normalizeText(query);
        if (!q) return list;
        return list.filter(f => {
            const hay = `${f.label} ${f.value} ${f.group || ''}`;
            return normalizeText(hay).includes(q);
        });
    }, [activeSource, query]);

    const grouped = useMemo(() => groupBy(filteredFields), [filteredFields]);

    if (!open) return null;

    return (
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

                        {!activeSource ? (
                            <div className="sfpm-empty">暂无字段</div>
                        ) : filteredFields.length === 0 ? (
                            <div className="sfpm-empty">未找到匹配字段</div>
                        ) : (
                            <div className="sfpm-groups">
                                {Object.entries(grouped).map(([groupName, items]) => (
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
                                                            source_id: activeSource.id,
                                                            source_type: activeSource.source_type,
                                                        });
                                                    }}
                                                    title={f.value}
                                                >
                                                    <span className="sfpm-field-label">{f.label}</span>
                                                    <code className="sfpm-field-key">{f.value}</code>
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
