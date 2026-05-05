import React, { useCallback, useEffect, useRef, useState } from 'react';
import type { DragEvent } from 'react';
import {
    ChevronDown,
    ChevronRight,
    Database,
    GripVertical,
    Layers,
    Plus,
    Redo2,
    Sliders,
    Trash2,
    Undo2,
    X,
} from 'lucide-react';
import Select from '../../components/common/Select';
import SourceFieldPickerModal from './SourceFieldPickerModal';
import type { VoucherFieldModule, VoucherRelationOption, VoucherSourceFieldOption } from '../../types';
import ExpressionInputWithActions from './ExpressionInputWithActions';
import { getSourceFieldDisplayText, getUnifiedSourceFieldLabel } from './sourceFieldLabelUtils';
import './ConditionBuilder.css';

type ExpressionFunctionOption = {
    key: string;
    category: string;
    insert_text?: string;
    label?: string;
};

const FIELD_FORMAT_PRESETS: ExpressionFunctionOption[] = [
    { key: '__field_date_ymd', label: '提取日期 YYYY-MM-DD', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'YYYY-MM-DD')" },
    { key: '__field_date_ym', label: '提取年月 YYYY-MM', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'YYYY-MM')" },
    { key: '__field_date_y', label: '提取年份 YYYY', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'YYYY')" },
    { key: '__field_date_m', label: '提取月份 MM', category: '日期提取', insert_text: "DATE_FORMAT(__VALUE__, 'MM')" },
    { key: '__field_trim', label: '去空格', category: '文本处理', insert_text: 'TRIM(__VALUE__)' },
    { key: '__field_upper', label: '转大写', category: '文本处理', insert_text: 'UPPER(__VALUE__)' },
    { key: '__field_lower', label: '转小写', category: '文本处理', insert_text: 'LOWER(__VALUE__)' },
];

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
export type RelationQuantifier = 'EXISTS' | 'NOT_EXISTS';

export interface Condition {
    id: string;
    field: string;
    field_format?: string;
    operator: Operator;
    value: string;
    type: 'rule';
}

export interface ConditionGroup {
    id: string;
    type: 'group';
    logic: LogicType;
    children: ConditionNode[];
}

export interface RelationCondition {
    id: string;
    type: 'relation';
    logic: LogicType;
    target_source: string;
    resolver: string;
    quantifier: RelationQuantifier;
    children: ConditionNode[];
}

type ConditionNode = Condition | ConditionGroup | RelationCondition;

interface DragState {
    nodeId: string;
    nodeType: 'rule' | 'group' | 'relation';
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
    fieldModules?: VoucherFieldModule[] | null;
    rootSourceType?: string | null;
    relationOptions?: VoucherRelationOption[] | null;
}

const generateId = () => Math.random().toString(36).slice(2, 11);

type RawConditionNode = {
    id?: string;
    type?: string;
    field?: string;
    field_format?: string;
    operator?: Operator;
    value?: string;
    logic?: LogicType;
    children?: RawConditionNode[];
    where?: {
        logic?: LogicType;
        children?: RawConditionNode[];
    };
    target_source?: string;
    resolver?: string;
    quantifier?: RelationQuantifier;
};

const normalizeNode = (node: RawConditionNode | null | undefined): ConditionNode => {
    if (!node || typeof node !== 'object') {
        return {
            id: generateId(),
            type: 'group',
            logic: 'AND',
            children: [],
        };
    }

    const nodeType = node.type || 'group';
    if (nodeType === 'rule') {
        return {
            id: String(node.id || generateId()),
            type: 'rule',
            field: String(node.field || ''),
            field_format: typeof node.field_format === 'string' ? node.field_format : undefined,
            operator: (node.operator || '==') as Operator,
            value: String(node.value || ''),
        };
    }

    if (nodeType === 'relation') {
        const relationChildren = Array.isArray(node.children)
            ? node.children
            : Array.isArray(node.where?.children)
                ? node.where.children
                : [];

        return {
            id: String(node.id || generateId()),
            type: 'relation',
            logic: ((node.logic || node.where?.logic || 'AND') as LogicType),
            target_source: String(node.target_source || 'bills'),
            resolver: String(node.resolver || 'receipt_to_bills'),
            quantifier: (node.quantifier || 'EXISTS') as RelationQuantifier,
            children: relationChildren.map((child: RawConditionNode) => normalizeNode(child)),
        };
    }

    return {
        id: String(node.id || generateId()),
        type: 'group',
        logic: ((node.logic || 'AND') as LogicType),
        children: (Array.isArray(node.children) ? node.children : []).map((child: RawConditionNode) => normalizeNode(child)),
    };
};

const normalizeRootNode = (node: RawConditionNode | null | undefined): ConditionGroup => {
    const normalized = normalizeNode(node);
    if (normalized.type === 'group') return normalized;
    return {
        id: 'root',
        type: 'group',
        logic: 'AND',
        children: [normalized],
    };
};

const collectContainerIds = (node: ConditionNode): string[] => {
    if (node.type === 'rule') return [];
    return [node.id, ...node.children.flatMap((child) => collectContainerIds(child))];
};

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

const ConditionBuilder: React.FC<ConditionBuilderProps> = ({
    value,
    onChange,
    fields,
    fieldModules,
    rootSourceType,
    relationOptions,
}) => {
    const [root, setRoot] = useState<ConditionGroup>({
        id: 'root',
        type: 'group',
        logic: 'AND',
        children: [],
    });
    const [dragState, setDragState] = useState<DragState | null>(null);
    const [dropTarget, setDropTarget] = useState<DropTarget | null>(null);
    const [collapsedGroupIds, setCollapsedGroupIds] = useState<string[]>([]);
    const [historyPast, setHistoryPast] = useState<string[]>([]);
    const [historyFuture, setHistoryFuture] = useState<string[]>([]);
    const [sourceFieldPickerOpen, setSourceFieldPickerOpen] = useState(false);
    const [sourceFieldPickerTarget, setSourceFieldPickerTarget] = useState<{
        mode: 'field' | 'value';
        nodeId: string;
        sourceType: string;
    } | null>(null);
    const builderRef = useRef<HTMLDivElement | null>(null);
    const lastAutoScrollAtRef = useRef<number>(0);
    const autoExpandTimerRef = useRef<number | null>(null);
    const pendingAutoExpandGroupRef = useRef<string | null>(null);
    const lastSerializedRef = useRef<string>('');
    const effectiveRootSourceType = String(rootSourceType || '').trim() || 'bills';

    const getNodeChildren = (node: ConditionNode): ConditionNode[] => (
        node.type === 'rule' ? [] : node.children
    );

    const serializeRoot = (node: ConditionGroup) => JSON.stringify(node);

    const applyRootState = useCallback((nextRoot: ConditionGroup, serialized?: string) => {
        const nextSerialized = serialized ?? serializeRoot(nextRoot);
        lastSerializedRef.current = nextSerialized;
        setRoot(nextRoot);
        onChange(nextSerialized);
    }, [onChange]);

    const updateRoot = (nextRoot: ConditionGroup, options?: { recordHistory?: boolean }) => {
        const currentSerialized = lastSerializedRef.current || serializeRoot(root);
        const nextSerialized = serializeRoot(nextRoot);
        if (currentSerialized === nextSerialized) return;

        if (options?.recordHistory !== false) {
            setHistoryPast(prev => [...prev, currentSerialized].slice(-HISTORY_LIMIT));
            setHistoryFuture([]);
        }

        applyRootState(nextRoot, nextSerialized);
    };

    const filterModulesForSourceType = (sourceType: string) => {
        const normalized = String(sourceType || '').trim().toLowerCase();
        if (!fieldModules || fieldModules.length === 0 || !normalized) return fieldModules || [];

        const next = fieldModules
            .map(module => ({
                ...module,
                sources: (module.sources || []).filter(source => (
                    String(source?.source_type || '').trim().toLowerCase() === normalized
                )),
            }))
            .filter(module => module.sources.length > 0);

        return next.length > 0 ? next : fieldModules;
    };

    const getFieldOptionsForSourceType = (sourceType: string): VoucherSourceFieldOption[] => {
        const fromModules = filterModulesForSourceType(sourceType)
            .flatMap(module => module.sources || [])
            .flatMap(source => source.fields || [])
            .map(field => ({
                label: getUnifiedSourceFieldLabel(field),
                value: String(field.value || ''),
                group: field.group ? String(field.group) : undefined,
            }))
            .filter(field => field.value);

        return fromModules.length > 0 ? fromModules : fields;
    };

    const getRelationOptionsForSource = (sourceType: string) => (
        (relationOptions || []).filter(option => option.root_source === sourceType)
    );

    const findNode = (node: ConditionNode, id: string): ConditionNode | null => {
        if (node.id === id) return node;
        for (const child of getNodeChildren(node)) {
            const found = findNode(child, id);
            if (found) return found;
        }
        return null;
    };

    const findParent = (node: ConditionNode, id: string): ConditionNode | null => {
        const children = getNodeChildren(node);
        if (children.some(child => child.id === id)) return node;
        for (const child of children) {
            const found = findParent(child, id);
            if (found) return found;
        }
        return null;
    };

    const findNodeSourceType = (
        targetId: string,
        currentNode: ConditionNode = root,
        inheritedSourceType: string = effectiveRootSourceType
    ): string | null => {
        const currentSourceType = currentNode.type === 'relation'
            ? String(currentNode.target_source || inheritedSourceType || effectiveRootSourceType)
            : inheritedSourceType;

        if (currentNode.id === targetId) return currentSourceType;

        for (const child of getNodeChildren(currentNode)) {
            const found = findNodeSourceType(targetId, child, currentSourceType);
            if (found) return found;
        }

        return null;
    };

    const getNodeSourceType = (targetId: string) => (
        findNodeSourceType(targetId) || effectiveRootSourceType
    );

    const getDisplayFieldLabel = (fieldKey: string, sourceType: string) => {
        const raw = String(fieldKey || '').trim();
        if (!raw) return '选择字段';

        const parts = raw.split('.').filter(Boolean);
        if (parts.length >= 3) {
            const [moduleId, sourceId, ...rest] = parts;
            const baseKey = rest.join('.');
            const module = fieldModules?.find(item => item.id === moduleId);
            const source = module?.sources?.find(item => item.id === sourceId);
            const fieldLabel = source?.fields?.find(item => item.value === baseKey)?.label
                || getFieldOptionsForSourceType(source?.source_type || sourceType).find(item => item.value === baseKey)?.label
                || baseKey;
            return `${module?.label || moduleId}.${source?.label || sourceId}.${fieldLabel}`;
        }

        if (parts.length === 2) {
            const [prefix, baseKey] = parts;
            const matched = fieldModules
                ?.flatMap(module => (module.sources || []).map(source => ({ module, source })))
                ?.find(item => (
                    String(item.source.source_type || '').trim().toLowerCase() === prefix.toLowerCase()
                    || String(item.source.id || '').trim().toLowerCase() === prefix.toLowerCase()
                ));
            const fieldLabel = matched?.source?.fields?.find(item => item.value === baseKey)?.label
                || getFieldOptionsForSourceType(matched?.source?.source_type || sourceType).find(item => item.value === baseKey)?.label
                || baseKey;
            return `${matched?.source?.label || prefix}.${fieldLabel}`;
        }

        return getFieldOptionsForSourceType(sourceType).find(item => item.value === raw)?.label || raw;
    };

    const getDisplayFieldCode = (fieldKey: string) => {
        const raw = String(fieldKey || '').trim();
        if (!raw) return '';

        const parts = raw.split('.').filter(Boolean);
        if (parts.length >= 2) {
            return parts[parts.length - 1] || raw;
        }
        return raw;
    };

    const applyHistorySnapshot = useCallback((serialized: string) => {
        try {
            const parsed = JSON.parse(serialized);
            if (parsed && parsed.type === 'group') {
                applyRootState(normalizeRootNode(parsed), serialized);
            }
        } catch (error) {
            console.error('Failed to apply history snapshot', error);
        }
    }, [applyRootState]);

    const handleUndo = useCallback(() => {
        if (historyPast.length === 0) return;
        const currentSerialized = lastSerializedRef.current || serializeRoot(root);
        const previousSerialized = historyPast[historyPast.length - 1];
        setHistoryPast(prev => prev.slice(0, -1));
        setHistoryFuture(prev => [currentSerialized, ...prev].slice(0, HISTORY_LIMIT));
        applyHistorySnapshot(previousSerialized);
    }, [applyHistorySnapshot, historyPast, root]);

    const handleRedo = useCallback(() => {
        if (historyFuture.length === 0) return;
        const currentSerialized = lastSerializedRef.current || serializeRoot(root);
        const nextSerialized = historyFuture[0];
        setHistoryFuture(prev => prev.slice(1));
        setHistoryPast(prev => [...prev, currentSerialized].slice(-HISTORY_LIMIT));
        applyHistorySnapshot(nextSerialized);
    }, [applyHistorySnapshot, historyFuture, root]);

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
                setRoot(normalizeRootNode(parsed));
                setHistoryPast([]);
                setHistoryFuture([]);
            }
        } catch (error) {
            console.error('Failed to parse condition JSON', error);
        }
    }, [value]);

    useEffect(() => {
        const activeIds = new Set(collectContainerIds(root));
        setCollapsedGroupIds(prev => {
            const next = prev.filter(id => activeIds.has(id) && id !== 'root');
            return next.length === prev.length ? prev : next;
        });
    }, [root]);

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
    }, [handleRedo, handleUndo]);

    const isGroupCollapsed = (groupId: string) => collapsedGroupIds.includes(groupId);

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

    const containsNode = (node: ConditionNode, targetId: string): boolean => {
        if (node.id === targetId) return true;
        return getNodeChildren(node).some(child => containsNode(child, targetId));
    };

    const isDropValid = (targetParentId: string): boolean => {
        if (!dragState) return false;
        if (targetParentId === dragState.nodeId) return false;
        if (dragState.nodeType !== 'rule') {
            const draggingNode = findNode(root, dragState.nodeId);
            if (draggingNode && draggingNode.type !== 'rule' && containsNode(draggingNode, targetParentId)) {
                return false;
            }
        }
        return true;
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
        const sourceType = getNodeSourceType(parentId);
        const sourceFields = getFieldOptionsForSourceType(sourceType);
        const newRule: Condition = {
            id: generateId(),
            type: 'rule',
            field: sourceFields[0]?.value || '',
            operator: '==',
            value: '',
        };

        const nextRoot = JSON.parse(JSON.stringify(root)) as ConditionGroup;
        const parent = parentId === 'root' ? nextRoot : findNode(nextRoot, parentId);
        if (parent && parent.type !== 'rule') {
            parent.children.push(newRule);
            updateRoot(nextRoot);
        }
    };

    const addGroup = (parentId: string) => {
        const newGroup: ConditionGroup = {
            id: generateId(),
            type: 'group',
            logic: 'AND',
            children: [],
        };

        const nextRoot = JSON.parse(JSON.stringify(root)) as ConditionGroup;
        const parent = parentId === 'root' ? nextRoot : findNode(nextRoot, parentId);
        if (parent && parent.type !== 'rule') {
            parent.children.push(newGroup);
            updateRoot(nextRoot);
        }
    };

    const addRelation = (parentId: string) => {
        const sourceType = getNodeSourceType(parentId);
        const option = getRelationOptionsForSource(sourceType)[0];
        if (!option) return;

        const newRelation: RelationCondition = {
            id: generateId(),
            type: 'relation',
            logic: 'AND',
            target_source: option.target_source,
            resolver: option.resolver,
            quantifier: 'EXISTS',
            children: [],
        };

        const nextRoot = JSON.parse(JSON.stringify(root)) as ConditionGroup;
        const parent = parentId === 'root' ? nextRoot : findNode(nextRoot, parentId);
        if (parent && parent.type !== 'rule') {
            parent.children.push(newRelation);
            updateRoot(nextRoot);
        }
    };

    const removeNode = (id: string) => {
        if (id === 'root') return;
        const nextRoot = JSON.parse(JSON.stringify(root)) as ConditionGroup;
        const removedNode = findNode(nextRoot, id);
        const removedIds = removedNode ? collectContainerIds(removedNode) : [];
        const parent = findParent(nextRoot, id);
        if (parent && parent.type !== 'rule') {
            parent.children = parent.children.filter(child => child.id !== id);
            updateRoot(nextRoot);
            if (removedIds.length > 0) {
                setCollapsedGroupIds(prev => prev.filter(groupId => !removedIds.includes(groupId)));
            }
        }
    };

    const updateNode = (id: string, updates: Partial<ConditionNode>) => {
        const nextRoot = JSON.parse(JSON.stringify(root)) as ConditionGroup;
        const node = findNode(nextRoot, id);
        if (!node) return;
        Object.assign(node, updates);
        updateRoot(nextRoot);
    };

    const handleDragStart = (
        event: DragEvent<HTMLElement>,
        node: ConditionNode,
        sourceParentId: string
    ) => {
        setDragState({
            nodeId: node.id,
            nodeType: node.type,
            sourceParentId,
        });
        setDropTarget(null);
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', node.id);
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
        event: DragEvent<HTMLDivElement>,
        targetParentId: string,
        targetIndex: number
    ) => {
        if (!dragState) return;
        event.preventDefault();
        const valid = isDropValid(targetParentId);
        handleAutoScroll(event.clientY);
        scheduleAutoExpand(targetParentId);
        event.dataTransfer.dropEffect = valid ? 'move' : 'none';
        setDropTarget(prev => (
            prev && prev.parentId === targetParentId && prev.index === targetIndex
                ? prev
                : { parentId: targetParentId, index: targetIndex }
        ));
    };

    const handleDrop = (
        event: DragEvent<HTMLDivElement>,
        targetParentId: string,
        targetIndex: number
    ) => {
        event.preventDefault();
        clearAutoExpandTimer();
        if (!dragState || !isDropValid(targetParentId)) {
            setDragState(null);
            setDropTarget(null);
            return;
        }

        const nextRoot = JSON.parse(JSON.stringify(root)) as ConditionGroup;
        const sourceParent = findNode(nextRoot, dragState.sourceParentId);
        const targetParent = findNode(nextRoot, targetParentId);
        if (!sourceParent || sourceParent.type === 'rule' || !targetParent || targetParent.type === 'rule') {
            setDragState(null);
            setDropTarget(null);
            return;
        }

        const sourceIndex = sourceParent.children.findIndex(child => child.id === dragState.nodeId);
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
        updateRoot(nextRoot);
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
                    onDragOver={(event) => handleDragOver(event, targetParentId, targetIndex)}
                    onDrop={(event) => handleDrop(event, targetParentId, targetIndex)}
                >
                    {isActive && (
                        <span>{isInvalid ? '该位置不可放置' : '在此插入'}</span>
                    )}
                </div>
            );
        };

        if (node.type !== 'rule') {
            const collapsed = isGroupCollapsed(node.id);
            const isContainerDropActive = dropTarget?.parentId === node.id;
            const isContainerDropInvalid = isContainerDropActive && dragState && !isDropValid(node.id);
            const currentSourceType = node.type === 'relation'
                ? String(node.target_source || getNodeSourceType(node.id))
                : getNodeSourceType(node.id);
            const parentSourceType = parentId ? getNodeSourceType(parentId) : effectiveRootSourceType;
            const relationOptions = node.type === 'relation'
                ? getRelationOptionsForSource(parentSourceType)
                : getRelationOptionsForSource(currentSourceType);
            const relationValue = node.type === 'relation'
                ? `${node.resolver}::${node.target_source}`
                : '';

            return (
                <div
                    key={node.id}
                    className={`condition-group ${node.type === 'relation' ? 'condition-relation' : ''} depth-${depth} ${dragState?.nodeId === node.id ? 'is-dragging' : ''} ${isContainerDropActive ? 'is-drop-active' : ''} ${isContainerDropInvalid ? 'is-drop-invalid' : ''}`}
                >
                    <div className="group-header">
                        <div className="group-header-left">
                            {depth > 0 && parentId !== undefined && indexInParent !== undefined && (
                                <button
                                    className="drag-handle"
                                    draggable
                                    onDragStart={(event) => handleDragStart(event, node, parentId)}
                                    onDragEnd={handleDragEnd}
                                    title="拖动条件组"
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
                            {node.type === 'relation' ? (
                                <>
                                    <Select
                                        value={node.quantifier}
                                        onChange={(v) => updateNode(node.id, { quantifier: v as RelationQuantifier })}
                                        className="logic-select"
                                        options={[
                                            { value: 'EXISTS', label: '存在关联记录' },
                                            { value: 'NOT_EXISTS', label: '不存在关联记录' },
                                        ]}
                                    />
                                    <Select
                                        value={relationValue}
                                        onChange={(v) => {
                                            const next = relationOptions.find(option => (
                                                `${option.resolver}::${option.target_source}` === v
                                            ));
                                            if (!next) return;
                                            updateNode(node.id, {
                                                resolver: next.resolver,
                                                target_source: next.target_source,
                                            });
                                        }}
                                        className="logic-select"
                                        options={relationOptions.map(option => ({
                                            value: `${option.resolver}::${option.target_source}`,
                                            label: option.label,
                                        }))}
                                    />
                                    <Select
                                        value={node.logic}
                                        onChange={(v) => updateNode(node.id, { logic: v as LogicType })}
                                        className="logic-select"
                                        options={[
                                            { value: 'AND', label: '子条件全部满足' },
                                            { value: 'OR', label: '子条件任一满足' },
                                        ]}
                                    />
                                </>
                            ) : (
                                <Select
                                    value={node.logic}
                                    onChange={(v) => updateNode(node.id, { logic: v as LogicType })}
                                    className="logic-select"
                                    options={[
                                        { value: 'AND', label: '全部满足 (AND)' },
                                        { value: 'OR', label: '任一满足 (OR)' },
                                    ]}
                                />
                            )}
                        </div>
                        <div className="group-actions">
                            <button onClick={() => addRule(node.id)} className="action-btn add-rule" type="button">
                                <Plus size={14} /> 添加规则
                            </button>
                            <button onClick={() => addGroup(node.id)} className="action-btn add-group" type="button">
                                <Layers size={14} /> 添加分组
                            </button>
                            {relationOptions.length > 0 && (
                                <button onClick={() => addRelation(node.id)} className="action-btn add-relation" type="button">
                                    <Database size={14} /> 添加关联条件
                                </button>
                            )}
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
                                <button onClick={() => removeNode(node.id)} className="action-btn remove" type="button">
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
                        {!collapsed && node.children.length === 0 && (
                            <div className="empty-group">
                                {relationOptions.length > 0 ? '暂无条件，可添加规则、分组或关联条件' : '无条件'}
                            </div>
                        )}
                        {collapsed && (
                            <div
                                className="group-collapsed-hint"
                                onDragOver={(event) => handleDragOver(event, node.id, node.children.length)}
                            >
                                已折叠，拖拽悬停可自动展开
                            </div>
                        )}
                        {renderDropIndicator(node.id, node.children.length, true)}
                    </div>
                </div>
            );
        }

        const fieldFormatKey = node.field_format || '';
        const fieldFormatter = FIELD_FORMAT_PRESETS.find(item => item.insert_text === fieldFormatKey) || null;
        const fieldFormatPreview = fieldFormatter ? buildFormattedText(node.field, fieldFormatter, false) : '';
        const activeSourceType = parentId ? getNodeSourceType(parentId) : effectiveRootSourceType;
        const activeFields = getFieldOptionsForSourceType(activeSourceType);

        return (
            <div key={node.id} className={`condition-rule ${dragState?.nodeId === node.id ? 'is-dragging' : ''}`}>
                {parentId !== undefined && indexInParent !== undefined && (
                    <button
                        className="drag-handle"
                        draggable
                        onDragStart={(event) => handleDragStart(event, node, parentId)}
                        onDragEnd={handleDragEnd}
                        title="拖动规则"
                        type="button"
                    >
                        <GripVertical size={14} />
                    </button>
                )}
                <div className="rule-content">
                    <div className="rule-field-group">
                        {fieldModules && fieldModules.length > 0 ? (
                            <button
                                type="button"
                                className="rule-field cb-adv-field-btn"
                                onClick={() => {
                                    setSourceFieldPickerTarget({ mode: 'field', nodeId: node.id, sourceType: activeSourceType });
                                    setSourceFieldPickerOpen(true);
                                }}
                                title={node.field ? `${getDisplayFieldLabel(node.field, activeSourceType)} (${getDisplayFieldCode(node.field)})` : '选择字段'}
                            >
                                <span className="cb-adv-field-meta">
                                    <span className="cb-adv-field-text">
                                        {getDisplayFieldLabel(node.field, activeSourceType)}
                                    </span>
                                    <span className="cb-adv-field-code">
                                        {getDisplayFieldCode(node.field)}
                                    </span>
                                </span>
                                <Database size={14} />
                            </button>
                        ) : (
                            <Select
                                value={node.field}
                                onChange={(v) => updateNode(node.id, { field: v })}
                                className="rule-field"
                                options={activeFields.map(field => ({ value: field.value, label: getSourceFieldDisplayText(field) }))}
                            />
                        )}
                        <div className="cb-field-format-combo" title={fieldFormatter ? `字段格式：${fieldFormatPreview}` : '对字段值格式化后再比较'}>
                            <Sliders size={11} />
                            <Select
                                className="cb-format-select"
                                value={fieldFormatKey}
                                onChange={(v) => updateNode(node.id, { field_format: v || undefined } as Partial<ConditionNode>)}
                                groups={(() => {
                                    const grouped: Record<string, ExpressionFunctionOption[]> = {};
                                    FIELD_FORMAT_PRESETS.forEach(item => {
                                        const category = item.category || '其他';
                                        if (!grouped[category]) grouped[category] = [];
                                        grouped[category].push(item);
                                    });
                                    return Object.entries(grouped).map(([groupName, items]) => ({
                                        label: groupName,
                                        options: items.map(item => ({ value: item.insert_text || '', label: item.label || '' })),
                                    }));
                                })()}
                                options={[{ value: '', label: '原始值' }]}
                            />
                        </div>
                        {fieldFormatter && (
                            <div className="cb-format-hint">
                                <code>{fieldFormatPreview}</code>
                            </div>
                        )}
                    </div>
                    <Select
                        value={node.operator}
                        onChange={(v) => updateNode(node.id, { operator: v as Operator })}
                        className="rule-operator"
                        options={OPERATORS.map(operator => ({ value: operator.value, label: operator.label }))}
                    />
                    <div className="rule-value-group">
                        <ExpressionInputWithActions
                            size="mini"
                            value={node.value}
                            onChange={(val) => updateNode(node.id, { value: val })}
                            fieldModules={filterModulesForSourceType(activeSourceType)}
                            placeholder="值（支持变量、函数和数据源字段）"
                            editorTitle="编辑触发条件值"
                        />
                    </div>
                </div>
                <button onClick={() => removeNode(node.id)} className="action-btn remove-rule" type="button">
                    <Trash2 size={14} />
                </button>
            </div>
        );
    };

    return (
        <div ref={builderRef} className={`condition-builder ${dragState ? 'drag-active' : ''}`}>
            {renderNode(root)}
            {fieldModules && fieldModules.length > 0 && (
                <SourceFieldPickerModal
                    open={sourceFieldPickerOpen}
                    onClose={() => {
                        setSourceFieldPickerOpen(false);
                        setSourceFieldPickerTarget(null);
                    }}
                    modules={filterModulesForSourceType(sourceFieldPickerTarget?.sourceType || effectiveRootSourceType)}
                    onPick={(field, ctx) => {
                        const target = sourceFieldPickerTarget;
                        if (!target) return;
                        const key = (ctx?.module_id && ctx?.source_id)
                            ? `${ctx.module_id}.${ctx.source_id}.${field.value}`
                            : (ctx?.source_type ? `${ctx.source_type}.${field.value}` : field.value);

                        if (target.mode === 'field') {
                            updateNode(target.nodeId, { field: key });
                        } else {
                            const node = findNode(root, target.nodeId);
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
