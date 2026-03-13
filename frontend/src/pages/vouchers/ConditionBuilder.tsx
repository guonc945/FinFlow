import React, { useEffect, useRef, useState } from 'react';
import type { DragEvent } from 'react';
import { ChevronDown, ChevronRight, Database, GripVertical, Hash, Layers, Plus, Redo2, Sliders, Trash2, Undo2, X } from 'lucide-react';
import VariablePicker from '../settings/VariablePicker';
import SourceFieldPickerModal from './SourceFieldPickerModal';
import type { VoucherFieldModule } from '../../types';
import './ConditionBuilder.css';

type ExpressionFunctionOption = {
    key: string;
    category: string;
    insert_text?: string;
    label?: string;
};

// 字段侧格式化预设（对字段原始值做变换后再比较）
const FIELD_FORMAT_PRESETS: ExpressionFunctionOption[] = [
    { key: '__field_date_ymd', label: '提取日期 YYYY-MM-DD', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'YYYY-MM-DD')" },
    { key: '__field_date_ym', label: '提取年月 YYYY-MM', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'YYYY-MM')" },
    { key: '__field_date_y', label: '提取年份 YYYY', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'YYYY')" },
    { key: '__field_date_m', label: '提取月份 MM', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'MM')" },
    { key: '__field_trim', label: '去空格', category: '文本处理', insert_text: 'TRIM(__VALUE__)' },
    { key: '__field_upper', label: '转大写', category: '文本处理', insert_text: 'UPPER(__VALUE__)' },
    { key: '__field_lower', label: '转小写', category: '文本处理', insert_text: 'LOWER(__VALUE__)' },
];

/** 构建格式化后的源字段文本 */
const buildFormattedText = (
    fieldValue: string,
    formatter: ExpressionFunctionOption | null | undefined,
    useBraces: boolean
) => {
    const sourceText = useBraces ? `{${fieldValue}}` : fieldValue;
    if (!formatter?.key) return sourceText;

    const insertTemplate = formatter.insert_text?.trim();
    if (insertTemplate) {
        if (insertTemplate.includes('__VALUE__')) {
            return insertTemplate.replace('__VALUE__', sourceText);
        }
        if (insertTemplate.includes('{') && insertTemplate.includes('}')) {
            return insertTemplate.replace(/\{[^{}]+\}/, sourceText);
        }
        const fnName = formatter.key.toUpperCase();
        if (insertTemplate === `${fnName}()`) {
            return `${fnName}(${sourceText})`;
        }
    }
    return `${formatter.key.toUpperCase()}(${sourceText})`;
};

export type Operator = '==' | '!=' | '>' | '>=' | '<' | '<=' | 'contains' | 'startswith' | 'endswith';
export type LogicType = 'AND' | 'OR';

export interface Condition {
    id: string;
    field: string;
    /** 字段侧格式化模板，如 DATE_FORMAT(__VALUE__, 'YYYY-MM')  */
    field_format?: string;
    operator: Operator;
    value: string;
    type: 'rule';
}

export interface ConditionGroup {
    id: string;
    type: 'group';
    logic: LogicType;
    children: (Condition | ConditionGroup)[];
}

type ConditionNode = Condition | ConditionGroup;

interface DragState {
    nodeId: string;
    nodeType: 'rule' | 'group';
    sourceParentId: string;
}

interface DropTarget {
    parentId: string;
    index: number;
}

interface ConditionBuilderProps {
    value?: string;
    onChange: (value: string) => void;
    fields: { label: string; value: string; group?: string }[];
    /** 数据源字段列表（如账单字段） */
    fieldModules?: VoucherFieldModule[] | null;
}

const generateId = () => Math.random().toString(36).slice(2, 11);

const OPERATORS: { label: string; value: Operator }[] = [
    { label: '等于', value: '==' },
    { label: '不等于', value: '!=' },
    { label: '大于', value: '>' },
    { label: '大于等于', value: '>=' },
    { label: '小于', value: '<' },
    { label: '小于等于', value: '<=' },
    { label: '包含', value: 'contains' },
    { label: '开头是', value: 'startswith' },
    { label: '结尾是', value: 'endswith' },
];

const HISTORY_LIMIT = 60;

const ConditionBuilder: React.FC<ConditionBuilderProps> = ({ value, onChange, fields, fieldModules }) => {
    const [root, setRoot] = useState<ConditionGroup>({
        id: 'root',
        type: 'group',
        logic: 'AND',
        children: [],
    });
    const [variablePickerOpen, setVariablePickerOpen] = useState(false);
    const [targetConditionId, setTargetConditionId] = useState<string | null>(null);
    const [dragState, setDragState] = useState<DragState | null>(null);
    const [dropTarget, setDropTarget] = useState<DropTarget | null>(null);
    const [collapsedGroupIds, setCollapsedGroupIds] = useState<string[]>([]);
    const [historyPast, setHistoryPast] = useState<string[]>([]);
    const [historyFuture, setHistoryFuture] = useState<string[]>([]);
    const [sourceFieldPickerOpen, setSourceFieldPickerOpen] = useState(false);
    const [sourceFieldPickerTarget, setSourceFieldPickerTarget] = useState<{ mode: 'field' | 'value'; nodeId: string } | null>(null);
    const builderRef = useRef<HTMLDivElement | null>(null);
    const lastAutoScrollAtRef = useRef<number>(0);
    const autoExpandTimerRef = useRef<number | null>(null);
    const pendingAutoExpandGroupRef = useRef<string | null>(null);
    const lastSerializedRef = useRef<string>('');

    const serializeRoot = (node: ConditionGroup): string => JSON.stringify(node);

    const applyRootState = (nextRoot: ConditionGroup, serialized?: string) => {
        const nextSerialized = serialized ?? serializeRoot(nextRoot);
        lastSerializedRef.current = nextSerialized;
        setRoot(nextRoot);
        onChange(nextSerialized);
    };

    const updateRoot = (newRoot: ConditionGroup, options?: { recordHistory?: boolean }) => {
        const currentSerialized = lastSerializedRef.current || serializeRoot(root);
        const nextSerialized = serializeRoot(newRoot);
        if (currentSerialized === nextSerialized) return;

        if (options?.recordHistory !== false) {
            setHistoryPast(prev => [...prev, currentSerialized].slice(-HISTORY_LIMIT));
            setHistoryFuture([]);
        }

        applyRootState(newRoot, nextSerialized);
    };

    useEffect(() => {
        const defaultSerialized = serializeRoot(root);
        if (!lastSerializedRef.current) {
            lastSerializedRef.current = defaultSerialized;
        }
    }, [root]);

    useEffect(() => {
        if (!value || value === lastSerializedRef.current) return;
        try {
            const parsed = JSON.parse(value);
            if (parsed && parsed.type === 'group') {
                lastSerializedRef.current = value;
                setRoot(parsed);
                setHistoryPast([]);
                setHistoryFuture([]);
            }
        } catch (e) {
            console.error('Failed to parse condition JSON', e);
        }
    }, [value]);

    const findNode = (node: ConditionNode, id: string): ConditionNode | null => {
        if (node.id === id) return node;
        if (node.type === 'group') {
            for (const child of node.children) {
                const found = findNode(child, id);
                if (found) return found;
            }
        }
        return null;
    };

    const findParent = (node: ConditionGroup, id: string): ConditionGroup | null => {
        if (node.children.some(c => c.id === id)) return node;
        for (const child of node.children) {
            if (child.type === 'group') {
                const found = findParent(child as ConditionGroup, id);
                if (found) return found;
            }
        }
        return null;
    };

    const getDisplayFieldLabel = (fieldKey: string) => {
        const raw = String(fieldKey || '').trim();
        if (!raw) return '选择字段';

        const parts = raw.split('.').filter(Boolean);
        if (parts.length >= 3) {
            const moduleId = parts[0];
            const sourceId = parts[1];
            const baseKey = parts.slice(2).join('.');
            const baseLabel = fields.find(f => f.value === baseKey)?.label || baseKey;

            const moduleLabel = fieldModules?.find(m => m.id === moduleId)?.label || moduleId;
            const sourceLabel = fieldModules
                ?.find(m => m.id === moduleId)
                ?.sources
                ?.find(s => s.id === sourceId)?.label || sourceId;

            return `${moduleLabel}.${sourceLabel}.${baseLabel}`;
        }
        if (parts.length === 2) {
            const [sourcePrefix, baseKey] = parts as [string, string];
            const baseLabel = fields.find(f => f.value === baseKey)?.label || baseKey;
            const matched = fieldModules
                ?.flatMap(m => (m.sources || []).map(s => ({ module: m, source: s })))
                ?.find(x => String(x.source.source_type || '').toLowerCase() === String(sourcePrefix).toLowerCase());
            const sourceLabel = matched?.source?.label || sourcePrefix;
            return `${sourceLabel}.${baseLabel}`;
        }

        return fields.find(f => f.value === raw)?.label || raw;
    };

    const collectGroupIds = (node: ConditionNode): string[] => {
        if (node.type !== 'group') return [];
        return [node.id, ...node.children.flatMap(child => collectGroupIds(child))];
    };

    useEffect(() => {
        const activeGroupIds = new Set(collectGroupIds(root));
        setCollapsedGroupIds(prev => {
            const filtered = prev.filter(id => activeGroupIds.has(id) && id !== 'root');
            return filtered.length === prev.length ? prev : filtered;
        });
    }, [root]);

    const applyHistorySnapshot = (serialized: string) => {
        try {
            const parsed = JSON.parse(serialized);
            if (parsed && parsed.type === 'group') {
                applyRootState(parsed, serialized);
            }
        } catch (e) {
            console.error('Failed to apply history snapshot', e);
        }
    };

    const handleUndo = () => {
        if (historyPast.length === 0) return;
        const currentSerialized = lastSerializedRef.current || serializeRoot(root);
        const previousSerialized = historyPast[historyPast.length - 1];
        setHistoryPast(prev => prev.slice(0, -1));
        setHistoryFuture(prev => [currentSerialized, ...prev].slice(0, HISTORY_LIMIT));
        applyHistorySnapshot(previousSerialized);
    };

    const handleRedo = () => {
        if (historyFuture.length === 0) return;
        const currentSerialized = lastSerializedRef.current || serializeRoot(root);
        const nextSerialized = historyFuture[0];
        setHistoryFuture(prev => prev.slice(1));
        setHistoryPast(prev => [...prev, currentSerialized].slice(-HISTORY_LIMIT));
        applyHistorySnapshot(nextSerialized);
    };

    useEffect(() => {
        const onKeyDown = (event: KeyboardEvent) => {
            const isAccel = event.ctrlKey || event.metaKey;
            if (!isAccel || event.altKey) return;

            const key = event.key.toLowerCase();
            if (key === 'z') {
                event.preventDefault();
                if (event.shiftKey) {
                    handleRedo();
                } else {
                    handleUndo();
                }
            } else if (key === 'y') {
                event.preventDefault();
                handleRedo();
            }
        };

        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [historyPast, historyFuture, root]);

    const isGroupCollapsed = (groupId: string): boolean => collapsedGroupIds.includes(groupId);

    const toggleGroupCollapse = (groupId: string) => {
        if (groupId === 'root') return;
        setCollapsedGroupIds(prev => (
            prev.includes(groupId)
                ? prev.filter(id => id !== groupId)
                : [...prev, groupId]
        ));
    };

    const clearAutoExpandTimer = () => {
        if (autoExpandTimerRef.current !== null) {
            window.clearTimeout(autoExpandTimerRef.current);
            autoExpandTimerRef.current = null;
        }
        pendingAutoExpandGroupRef.current = null;
    };

    const scheduleAutoExpand = (groupId: string) => {
        if (!dragState || !isGroupCollapsed(groupId) || !isDropValid(groupId)) return;
        if (pendingAutoExpandGroupRef.current === groupId) return;
        clearAutoExpandTimer();
        pendingAutoExpandGroupRef.current = groupId;
        autoExpandTimerRef.current = window.setTimeout(() => {
            setCollapsedGroupIds(prev => prev.filter(id => id !== groupId));
            clearAutoExpandTimer();
        }, 380);
    };

    useEffect(() => {
        return () => clearAutoExpandTimer();
    }, []);

    const addRule = (parentId: string) => {
        const newRule: Condition = {
            id: generateId(),
            type: 'rule',
            field: fields[0]?.value || '',
            operator: '==',
            value: '',
        };
        const newRoot = JSON.parse(JSON.stringify(root));
        if (newRoot.id === parentId) {
            newRoot.children.push(newRule);
        } else {
            const parent = findNode(newRoot, parentId) as ConditionGroup;
            if (parent) parent.children.push(newRule);
        }
        updateRoot(newRoot);
    };

    const addGroup = (parentId: string) => {
        const newGroup: ConditionGroup = {
            id: generateId(),
            type: 'group',
            logic: 'AND',
            children: [],
        };
        const newRoot = JSON.parse(JSON.stringify(root));
        if (newRoot.id === parentId) {
            newRoot.children.push(newGroup);
        } else {
            const parent = findNode(newRoot, parentId) as ConditionGroup;
            if (parent) parent.children.push(newGroup);
        }
        updateRoot(newRoot);
    };

    const removeNode = (id: string) => {
        if (id === 'root') return;
        const newRoot = JSON.parse(JSON.stringify(root));
        const removedNode = findNode(newRoot, id);
        const removedGroupIds = removedNode ? collectGroupIds(removedNode) : [];
        const parent = findParent(newRoot, id);
        if (parent) {
            parent.children = parent.children.filter((c: { id: string }) => c.id !== id);
            updateRoot(newRoot);
            if (removedGroupIds.length > 0) {
                setCollapsedGroupIds(prev => prev.filter(groupId => !removedGroupIds.includes(groupId)));
            }
        }
    };

    const updateNode = (id: string, updates: Partial<ConditionNode>) => {
        const newRoot = JSON.parse(JSON.stringify(root));
        const node = findNode(newRoot, id);
        if (!node) return;
        Object.assign(node, updates);
        updateRoot(newRoot);
    };

    const containsNode = (node: ConditionNode, targetId: string): boolean => {
        if (node.id === targetId) return true;
        if (node.type === 'group') {
            return node.children.some(child => containsNode(child, targetId));
        }
        return false;
    };

    const isDropValid = (targetParentId: string): boolean => {
        if (!dragState) return false;
        if (targetParentId === dragState.nodeId) return false;
        if (dragState.nodeType === 'group') {
            const draggingNode = findNode(root, dragState.nodeId);
            if (draggingNode && draggingNode.type === 'group' && containsNode(draggingNode, targetParentId)) {
                return false;
            }
        }
        return true;
    };

    const handleDragStart = (
        e: DragEvent<HTMLElement>,
        node: ConditionNode,
        sourceParentId: string
    ) => {
        setDragState({
            nodeId: node.id,
            nodeType: node.type,
            sourceParentId,
        });
        setDropTarget(null);
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', node.id);
    };

    const handleDragEnd = () => {
        clearAutoExpandTimer();
        setDragState(null);
        setDropTarget(null);
    };

    const handleAutoScroll = (clientY: number) => {
        const now = Date.now();
        if (now - lastAutoScrollAtRef.current < 18) return;
        lastAutoScrollAtRef.current = now;

        const windowThreshold = 72;
        const windowStep = 22;
        if (clientY < windowThreshold) {
            window.scrollBy(0, -windowStep);
        } else if (clientY > window.innerHeight - windowThreshold) {
            window.scrollBy(0, windowStep);
        }

        const container = builderRef.current;
        if (!container || container.scrollHeight <= container.clientHeight + 4) return;
        const rect = container.getBoundingClientRect();
        const localThreshold = Math.min(70, rect.height * 0.2);
        if (clientY < rect.top + localThreshold) {
            container.scrollTop -= 18;
        } else if (clientY > rect.bottom - localThreshold) {
            container.scrollTop += 18;
        }
    };

    const handleDragOver = (
        e: DragEvent<HTMLDivElement>,
        targetParentId: string,
        targetIndex: number
    ) => {
        if (!dragState) return;
        e.preventDefault();
        const valid = isDropValid(targetParentId);
        handleAutoScroll(e.clientY);
        scheduleAutoExpand(targetParentId);
        e.dataTransfer.dropEffect = valid ? 'move' : 'none';
        setDropTarget(prev => (
            prev && prev.parentId === targetParentId && prev.index === targetIndex
                ? prev
                : { parentId: targetParentId, index: targetIndex }
        ));
    };

    const handleDrop = (
        e: DragEvent<HTMLDivElement>,
        targetParentId: string,
        targetIndex: number
    ) => {
        e.preventDefault();
        clearAutoExpandTimer();
        if (!dragState) return;
        if (!isDropValid(targetParentId)) {
            setDragState(null);
            setDropTarget(null);
            return;
        }

        const newRoot = JSON.parse(JSON.stringify(root)) as ConditionGroup;
        const sourceParent = findNode(newRoot, dragState.sourceParentId);
        const targetParent = findNode(newRoot, targetParentId);
        if (!sourceParent || sourceParent.type !== 'group' || !targetParent || targetParent.type !== 'group') {
            setDragState(null);
            setDropTarget(null);
            return;
        }

        const sourceIndex = sourceParent.children.findIndex(c => c.id === dragState.nodeId);
        if (sourceIndex < 0) {
            setDragState(null);
            setDropTarget(null);
            return;
        }

        const [movingNode] = sourceParent.children.splice(sourceIndex, 1);
        let insertIndex = targetIndex;
        if (sourceParent.id === targetParent.id && sourceIndex < insertIndex) {
            insertIndex -= 1;
        }
        insertIndex = Math.max(0, Math.min(insertIndex, targetParent.children.length));

        if (sourceParent.id === targetParent.id && sourceIndex === insertIndex) {
            setDragState(null);
            setDropTarget(null);
            return;
        }

        targetParent.children.splice(insertIndex, 0, movingNode);
        updateRoot(newRoot);
        setDragState(null);
        setDropTarget(null);
    };

    const renderNode = (
        node: ConditionNode,
        depth = 0,
        parentId?: string,
        indexInParent?: number
    ): React.ReactNode => {
        const renderDropIndicator = (
            targetParentId: string,
            targetIndex: number,
            isTail = false
        ) => {
            const isActive = dropTarget?.parentId === targetParentId && dropTarget.index === targetIndex;
            const isInvalid = isActive && dragState && !isDropValid(targetParentId);
            return (
                <div
                    key={`drop-${targetParentId}-${targetIndex}`}
                    className={`condition-insert-line ${isTail ? 'is-tail' : ''} ${isActive ? 'is-active' : ''} ${isInvalid ? 'is-invalid' : ''}`}
                    onDragOver={(e) => handleDragOver(e, targetParentId, targetIndex)}
                    onDrop={(e) => handleDrop(e, targetParentId, targetIndex)}
                >
                    {isActive && (
                        <span>{isInvalid ? '该位置不可放置' : '在此插入'}</span>
                    )}
                </div>
            );
        };

        if (node.type === 'group') {
            const collapsed = isGroupCollapsed(node.id);
            const isGroupDropActive = dropTarget?.parentId === node.id;
            const isGroupDropInvalid = isGroupDropActive && dragState && !isDropValid(node.id);
            return (
                <div
                    key={node.id}
                    className={`condition-group depth-${depth} ${dragState?.nodeId === node.id ? 'is-dragging' : ''} ${isGroupDropActive ? 'is-drop-active' : ''} ${isGroupDropInvalid ? 'is-drop-invalid' : ''}`}
                >
                    <div className="group-header">
                        <div className="group-header-left">
                            {depth > 0 && parentId !== undefined && indexInParent !== undefined && (
                                <button
                                    className="drag-handle"
                                    draggable
                                    onDragStart={(e) => handleDragStart(e, node, parentId)}
                                    onDragEnd={handleDragEnd}
                                    title="拖动规则组"
                                    type="button"
                                >
                                    <GripVertical size={14} />
                                </button>
                            )}
                            {depth > 0 && (
                                <button
                                    className="group-toggle-btn"
                                    type="button"
                                    onClick={() => toggleGroupCollapse(node.id)}
                                    title={collapsed ? '展开分组' : '折叠分组'}
                                >
                                    {collapsed ? <ChevronRight size={14} /> : <ChevronDown size={14} />}
                                </button>
                            )}
                            <select
                                value={node.logic}
                                onChange={(e) => updateNode(node.id, { logic: e.target.value as LogicType })}
                                className="logic-select"
                            >
                                <option value="AND">全部满足 (AND)</option>
                                <option value="OR">任意满足 (OR)</option>
                            </select>
                        </div>
                        <div className="group-actions">
                            <button onClick={() => addRule(node.id)} className="action-btn add-rule">
                                <Plus size={14} /> 添加规则
                            </button>
                            <button onClick={() => addGroup(node.id)} className="action-btn add-group">
                                <Layers size={14} /> 添加规则组
                            </button>
                            {depth === 0 && (
                                <div className="history-actions">
                                    <button
                                        type="button"
                                        className="action-btn history-btn"
                                        onClick={handleUndo}
                                        disabled={historyPast.length === 0}
                                        title="撤销 (Ctrl/Cmd+Z)"
                                    >
                                        <Undo2 size={14} /> 撤销
                                    </button>
                                    <button
                                        type="button"
                                        className="action-btn history-btn"
                                        onClick={handleRedo}
                                        disabled={historyFuture.length === 0}
                                        title="重做 (Ctrl+Y / Cmd+Shift+Z)"
                                    >
                                        <Redo2 size={14} /> 重做
                                    </button>
                                </div>
                            )}
                            {depth > 0 && (
                                <button onClick={() => removeNode(node.id)} className="action-btn remove">
                                    <X size={14} />
                                </button>
                            )}
                        </div>
                    </div>
                    <div className="group-children">
                        {!collapsed && node.children.length > 0 && (
                            node.children.map((child, idx) => (
                                <React.Fragment key={child.id}>
                                    {renderDropIndicator(node.id, idx)}
                                    {renderNode(child, depth + 1, node.id, idx)}
                                </React.Fragment>
                            ))
                        )}
                        {!collapsed && node.children.length === 0 && <div className="empty-group">无条件</div>}
                        {collapsed && (
                            <div
                                className="group-collapsed-hint"
                                onDragOver={(e) => handleDragOver(e, node.id, node.children.length)}
                            >
                                已折叠，拖拽悬停可自动展开
                            </div>
                        )}
                        {renderDropIndicator(node.id, node.children.length, true)}
                    </div>
                </div>
            );
        }

        // ---------- 字段侧格式化 ----------
        const fieldFormatKey = node.field_format || '';
        const fieldFormatter = FIELD_FORMAT_PRESETS.find(f => f.insert_text === fieldFormatKey) || null;
        const fieldFormatPreview = fieldFormatter
            ? buildFormattedText(node.field, fieldFormatter, false)
            : '';

        return (
            <div key={node.id} className={`condition-rule ${dragState?.nodeId === node.id ? 'is-dragging' : ''}`}>
                {parentId !== undefined && indexInParent !== undefined && (
                    <button
                        className="drag-handle"
                        draggable
                        onDragStart={(e) => handleDragStart(e, node, parentId)}
                        onDragEnd={handleDragEnd}
                        title="拖动规则"
                        type="button"
                    >
                        <GripVertical size={14} />
                    </button>
                )}
                <div className="rule-content">
                    {/* 字段选择 + 字段格式化 */}
                    <div className="rule-field-group">
                        {fieldModules && fieldModules.length > 0 ? (
                            <button
                                type="button"
                                className="rule-field cb-adv-field-btn"
                                onClick={() => {
                                    setSourceFieldPickerTarget({ mode: 'field', nodeId: node.id });
                                    setSourceFieldPickerOpen(true);
                                }}
                                title="选择字段"
                            >
                                <span className="cb-adv-field-text">
                                    {getDisplayFieldLabel(node.field)}
                                </span>
                                <ChevronDown size={14} />
                            </button>
                        ) : (
                            <select
                                value={node.field}
                                onChange={(e) => updateNode(node.id, { field: e.target.value })}
                                className="rule-field"
                            >
                                {fields.map(f => (
                                    <option key={f.value} value={f.value}>{f.label}</option>
                                ))}
                            </select>
                        )}
                        <div className="cb-field-format-combo" title={fieldFormatter ? `字段格式：${fieldFormatPreview}` : '对字段值做格式化后再比较'}>
                            <Sliders size={11} />
                            <select
                                className="cb-format-select"
                                value={fieldFormatKey}
                                onChange={(e) => updateNode(node.id, { field_format: e.target.value || undefined } as any)}
                            >
                                <option value="">原始值</option>
                                {(() => {
                                    const groups: Record<string, ExpressionFunctionOption[]> = {};
                                    FIELD_FORMAT_PRESETS.forEach(item => {
                                        const g = item.category || '其他';
                                        if (!groups[g]) groups[g] = [];
                                        groups[g]!.push(item);
                                    });
                                    return Object.entries(groups).map(([gName, fns]) => (
                                        <optgroup key={gName} label={gName}>
                                            {fns.map(f => (
                                                <option key={f.key} value={f.insert_text || ''}>{f.label}</option>
                                            ))}
                                        </optgroup>
                                    ));
                                })()}
                            </select>
                        </div>
                        {fieldFormatter && (
                            <div className="cb-format-hint">
                                <code>{fieldFormatPreview}</code>
                            </div>
                        )}
                    </div>
                    <select
                        value={node.operator}
                        onChange={(e) => updateNode(node.id, { operator: e.target.value as Operator })}
                        className="rule-operator"
                    >
                        {OPERATORS.map(op => (
                            <option key={op.value} value={op.value}>{op.label}</option>
                        ))}
                    </select>
                    <div className="rule-value-group">
                        <div className="input-with-action mini">
                            <input
                                type="text"
                                value={node.value}
                                onChange={(e) => updateNode(node.id, { value: e.target.value })}
                                placeholder="值（支持变量和格式化函数）"
                                className="rule-value"
                            />
                            <button
                                onClick={() => {
                                    setTargetConditionId(node.id);
                                    setVariablePickerOpen(true);
                                }}
                                title="插入变量"
                            >
                                <Hash size={12} />
                            </button>
                            {fieldModules && fieldModules.length > 0 && (
                                <div className="cb-field-combo" title="选择数据源字段">
                                    <button
                                        type="button"
                                        className="cb-field-trigger"
                                        onClick={() => {
                                            setSourceFieldPickerTarget({ mode: 'value', nodeId: node.id });
                                            setSourceFieldPickerOpen(true);
                                        }}
                                        title="选择数据源字段"
                                    >
                                        <Database size={12} />
                                    </button>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
                <button onClick={() => removeNode(node.id)} className="action-btn remove-rule">
                    <Trash2 size={14} />
                </button>
            </div>
        );
    };

    return (
        <div ref={builderRef} className={`condition-builder ${dragState ? 'drag-active' : ''}`}>
            {renderNode(root)}
            <VariablePicker
                isOpen={variablePickerOpen}
                onClose={() => setVariablePickerOpen(false)}
                includeFunctions
                onSelect={(variable) => {
                    if (targetConditionId) {
                        const node = findNode(root, targetConditionId) as Condition | null;
                        const prevValue = node && node.type === 'rule' ? (node.value || '') : '';
                        const insertText = variable?.insert_text || (variable?.key ? `{${variable.key}}` : String(variable || ''));
                        updateNode(targetConditionId, { value: prevValue + insertText });
                        setTargetConditionId(null);
                    }
                }}
            />
            {fieldModules && fieldModules.length > 0 && (
                <SourceFieldPickerModal
                    open={sourceFieldPickerOpen}
                    onClose={() => {
                        setSourceFieldPickerOpen(false);
                        setSourceFieldPickerTarget(null);
                    }}
                    modules={fieldModules}
                    onPick={(f, ctx) => {
                        const target = sourceFieldPickerTarget;
                        if (!target) return;
                        const key = (ctx?.module_id && ctx?.source_id)
                            ? `${ctx.module_id}.${ctx.source_id}.${f.value}`
                            : (ctx?.source_type ? `${ctx.source_type}.${f.value}` : f.value);
                        if (target.mode === 'field') {
                            updateNode(target.nodeId, { field: key });
                        } else {
                            const node = findNode(root, target.nodeId) as Condition | null;
                            const prevValue = node && node.type === 'rule' ? (node.value || '') : '';
                            updateNode(target.nodeId, { value: prevValue + `{${key}}` });
                        }
                        setSourceFieldPickerOpen(false);
                        setSourceFieldPickerTarget(null);
                    }}
                />
            )}
        </div>
    );
};

export default ConditionBuilder;
