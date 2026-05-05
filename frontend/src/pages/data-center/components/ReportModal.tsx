import { useEffect, useMemo, useState, type Dispatch, type SetStateAction } from 'react';
import { ChevronDown, ChevronRight, ChevronUp, Eye, Filter, GripVertical, Loader2, Palette, PanelLeft, Play, Plus, Save, Search, SearchCheck, Trash2 } from 'lucide-react';
import Select from '../../../components/common/Select';
import FormModal from './FormModal';
import type {
    ChartType,
    DataDictionary,
    DataDictionaryItem,
    Dataset,
    QueryColumn,
    QueryResult,
    ReportCategory,
    ReportColumnStyleRule,
    FilterType,
    ReportColumnConfig,
    ReportFilter,
    ReportFormState,
    TableStyleConfig,
} from '../types';
import { formatValueByColumnConfig, getColumnStyleForValue, normalizeFilter } from '../utils';

type ReportModalProps = {
    open: boolean;
    editingReportId: number | null;
    reportForm: ReportFormState;
    setReportForm: Dispatch<SetStateAction<ReportFormState>>;
    datasets: Dataset[];
    categories: ReportCategory[];
    dictionaries: DataDictionary[];
    dictionaryItemsById: Record<number, DataDictionaryItem[]>;
    selectedDataset?: Dataset;
    selectedDatasetColumns: QueryColumn[];
    reportFormFilters: ReportFilter[];
    reportFormColumns: ReportColumnConfig[];
    onClose: () => void;
    onDatasetChange: (datasetId: string) => void;
    onAutoFilters: () => void;
    onAddFilter: () => void;
    onRemoveFilter: (index: number) => void;
    onUpdateFilter: (index: number, patch: Partial<ReportFilter>) => void;
    onAddFilterOption: (filterIndex: number) => void;
    onUpdateFilterOption: (filterIndex: number, optionIndex: number, patch: Partial<{ label: string; value: string }>) => void;
    onRemoveFilterOption: (filterIndex: number, optionIndex: number) => void;
    onUpdateColumn: (index: number, patch: Partial<ReportColumnConfig>) => void;
    onReorderGroups: (sourceGroupKey: string, targetGroupKey: string, position: 'before' | 'after') => void;
    onMoveColumnToGroup: (columnIndex: number, targetGroupKey: string | null) => void;
    onPlaceColumn: (
        sourceIndex: number,
        targetIndex: number,
        position: 'before' | 'after',
        targetGroupKey: string | null
    ) => void;
    onSetParentGroup: (groupKey: string, parentGroupKey: string | null) => void;
    onToggleAllColumns: (visible: boolean) => void;
    onBulkUpdateColumns: (keys: string[], patch: Partial<ReportColumnConfig>) => void;
    onReset: () => void;
    onSave: () => void | Promise<void>;
    reportPreviewResult: QueryResult | null;
    reportPreviewLoading: boolean;
    reportPreviewError: string | null;
    onPreview: () => void | Promise<void>;
};

type ColumnDragPayload = {
    type: 'column';
    columnKey: string;
};

type GroupDragPayload = {
    type: 'group';
    groupKey: string;
};

type DragPayload = ColumnDragPayload | GroupDragPayload;

const createEmptyStyleRule = (): ReportColumnStyleRule => ({
    id: `style-rule-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    operator: 'eq',
    value: '',
    second_value: '',
    text_color: '',
    background_color: '',
    background_mode: 'soft',
});

export default function ReportModal({
    open,
    editingReportId,
    reportForm,
    setReportForm,
    datasets,
    categories,
    dictionaries,
    dictionaryItemsById,
    selectedDataset,
    selectedDatasetColumns,
    reportFormFilters,
    reportFormColumns,
    onClose,
    onDatasetChange,
    onAutoFilters,
    onAddFilter,
    onRemoveFilter,
    onUpdateFilter,
    onAddFilterOption,
    onUpdateFilterOption,
    onRemoveFilterOption,
    onUpdateColumn,
    onMoveColumnToGroup,
    onPlaceColumn,
    onSetParentGroup,
    onToggleAllColumns,
    onBulkUpdateColumns,
    onReset,
    onSave,
    reportPreviewResult,
    reportPreviewLoading,
    reportPreviewError,
    onPreview,
}: ReportModalProps) {
    const [activeTab, setActiveTab] = useState<'basic' | 'filters' | 'columns' | 'style' | 'preview'>('basic');
    const [columnKeyword, setColumnKeyword] = useState('');
    const [columnVisibilityFilter, setColumnVisibilityFilter] = useState<'all' | 'visible' | 'hidden'>('all'); // 保留仅用于快速筛选提示
    const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({});
    const [customGroups, setCustomGroups] = useState<string[]>([]);
    const [customGroupParents, setCustomGroupParents] = useState<Record<string, string | null>>({});
    const [customGroupOrders, setCustomGroupOrders] = useState<Record<string, number>>({});
    const [editingGroupKey, setEditingGroupKey] = useState<string | null>(null);
    const [editingGroupTitle, setEditingGroupTitle] = useState('');
    const [draggingColumnIndex, setDraggingColumnIndex] = useState<number | null>(null);
    const [dragOverColumnIndex, setDragOverColumnIndex] = useState<number | null>(null);
    const [dragOverGroupKey, setDragOverGroupKey] = useState<string | null>(null);
    const [dropPosition, setDropPosition] = useState<'before' | 'after' | null>(null);
    const [draggingGroupKey, setDraggingGroupKey] = useState<string | null>(null);
    const [dragOverGroupPosition, setDragOverGroupPosition] = useState<'before' | 'after' | 'inside' | null>(null);
    const [activeDragPayload, setActiveDragPayload] = useState<DragPayload | null>(null);
    const [openColumnDictionaryIndex, setOpenColumnDictionaryIndex] = useState<number | null>(null);
    const [columnDictionarySearchText, setColumnDictionarySearchText] = useState<Record<number, string>>({});
    const [openFilterDictionaryIndex, setOpenFilterDictionaryIndex] = useState<number | null>(null);
    const [filterDictionarySearchText, setFilterDictionarySearchText] = useState<Record<number, string>>({});
    const [editingStyleRule, setEditingStyleRule] = useState<{ columnKey: string; rule: ReportColumnStyleRule; ruleIndex: number } | null>(null);
    const [previewFiltersVisible, setPreviewFiltersVisible] = useState(true);
    const [previewCurrentPage, setPreviewCurrentPage] = useState(1);
    const [previewCurrentPageSize, setPreviewCurrentPageSize] = useState(20);
    const [previewRuntimeFilters, setPreviewRuntimeFilters] = useState<Record<string, string>>({});
    const visibleColumnCount = reportFormColumns.filter((column) => column.visible).length;
    const hiddenColumnCount = reportFormColumns.length - visibleColumnCount;
    const filteredColumns = useMemo(() => {
        const keyword = columnKeyword.trim().toLowerCase();
        return reportFormColumns
            .map((column, columnIndex) => ({
                column,
                columnIndex,
                datasetColumn: selectedDatasetColumns.find((item) => item.name === column.key),
            }))
            .filter(({ column, datasetColumn }) => {
                // 可见性筛选仅用于快速定位，不影响列表显示
                if (columnVisibilityFilter === 'visible' && !column.visible) return false;
                if (columnVisibilityFilter === 'hidden' && column.visible) return false;
                if (!keyword) return true;
                return [
                    column.key,
                    column.label,
                    column.description,
                    column.type,
                    datasetColumn?.type,
                ]
                    .filter(Boolean)
                    .some((value) => String(value).toLowerCase().includes(keyword));
            });
    }, [columnKeyword, columnVisibilityFilter, reportFormColumns, selectedDatasetColumns]);

    const getActualColumnIndex = (columnKey: string): number => {
        return reportFormColumns.findIndex(col => col.key === columnKey);
    };

    const DRAG_PAYLOAD_MIME = 'application/x-finflow-report-drag';

    const writeDragPayload = (event: React.DragEvent, payload: DragPayload) => {
        const serialized = JSON.stringify(payload);
        event.dataTransfer.setData(DRAG_PAYLOAD_MIME, serialized);
        event.dataTransfer.setData('text/plain', serialized);
    };

    const readDragPayload = (event: React.DragEvent): DragPayload | null => {
        const raw = event.dataTransfer.getData(DRAG_PAYLOAD_MIME) || event.dataTransfer.getData('text/plain');
        if (!raw) return null;
        try {
            const parsed = JSON.parse(raw) as DragPayload;
            if (parsed?.type === 'column' && typeof parsed.columnKey === 'string') return parsed;
            if (parsed?.type === 'group' && typeof parsed.groupKey === 'string') return parsed;
        } catch {
            return null;
        }
        return null;
    };

    const resolveDragPayload = (event: React.DragEvent): DragPayload | null => {
        return readDragPayload(event) || activeDragPayload;
    };

    useEffect(() => {
        const derivedGroupNames = Array.from(
            new Set(
                reportFormColumns
                    .flatMap((column) => [column.group, column.parent_group])
                    .filter((value): value is string => Boolean(value))
            )
        );
        if (!derivedGroupNames.length) return;
        setCustomGroups((prev) => Array.from(new Set([...prev, ...derivedGroupNames])));
        setCustomGroupParents((prev) => {
            const next = { ...prev };
            reportFormColumns.forEach((column) => {
                if (column.group) {
                    next[column.group] = column.parent_group || next[column.group] || null;
                }
            });
            return next;
        });
        setCustomGroupOrders((prev) => {
            const next = { ...prev };
            reportFormColumns.forEach((column) => {
                if (column.group && column.group_order !== undefined) {
                    next[column.group] = column.group_order;
                } else if (column.group && next[column.group] === undefined) {
                    next[column.group] = 0;
                }
            });
            return next;
        });
    }, [reportFormColumns]);

    useEffect(() => {
        if (open) return;
        setOpenColumnDictionaryIndex(null);
        setOpenFilterDictionaryIndex(null);
        setColumnDictionarySearchText({});
        setFilterDictionarySearchText({});
    }, [open]);

    useEffect(() => {
        if (openColumnDictionaryIndex === null && openFilterDictionaryIndex === null) return;

        const handlePointerDown = (event: MouseEvent) => {
            const target = event.target as HTMLElement | null;
            if (target?.closest('.runtime-select-shell')) return;
            setOpenColumnDictionaryIndex(null);
            setOpenFilterDictionaryIndex(null);
        };

        document.addEventListener('mousedown', handlePointerDown);
        return () => {
            document.removeEventListener('mousedown', handlePointerDown);
        };
    }, [openColumnDictionaryIndex, openFilterDictionaryIndex]);

    const resetDragState = () => {
        setDraggingColumnIndex(null);
        setDragOverColumnIndex(null);
        setDragOverGroupKey(null);
        setDropPosition(null);
        setDraggingGroupKey(null);
        setDragOverGroupPosition(null);
        setActiveDragPayload(null);
    };

    const groupedColumns = useMemo(() => {
        type GroupItem = {
            column: ReportColumnConfig;
            columnIndex: number;
            datasetColumn: QueryColumn | undefined;
        };

        type Entry =
            | { kind: 'column'; item: GroupItem; order: number }
            | { kind: 'group'; order: number; node: GroupNode };

        type GroupNode = {
            key: string;
            title: string;
            order: number;
            parent: string | null;
            depth: number;
            columnCount: number;
            entries: Entry[];
        };

        const getGroupOrder = (groupName: string): number => {
            if (customGroupOrders[groupName] !== undefined) {
                return customGroupOrders[groupName];
            }
            const columnsWithGroup = reportFormColumns.filter((col) => col.group === groupName);
            if (columnsWithGroup.length > 0) {
                return columnsWithGroup[0].group_order ?? 0;
            }
            return customGroups.indexOf(groupName);
        };

        const sortEntries = (entries: Entry[]) =>
            [...entries].sort((a, b) => {
                if (a.order !== b.order) return a.order - b.order;
                if (a.kind !== b.kind) return a.kind === 'column' ? -1 : 1;
                if (a.kind === 'column' && b.kind === 'column') {
                    return a.item.columnIndex - b.item.columnIndex;
                }
                if (a.kind === 'group' && b.kind === 'group') {
                    return a.node.key.localeCompare(b.node.key);
                }
                return 0;
            });

        const visibilityGroups = [
            {
                key: 'all',
                title: '全部字段',
                items: filteredColumns,
            },
        ];

        return visibilityGroups.map((visibilityGroup) => {
            const groupMeta = new Map<string, { key: string; title: string; order: number; parent: string | null }>();

            const ensureGroupMeta = (groupKey: string, parentKey: string | null) => {
                const existing = groupMeta.get(groupKey);
                if (existing) {
                    if (parentKey !== null) {
                        existing.parent = parentKey;
                    }
                    return existing;
                }
                const next = {
                    key: groupKey,
                    title: groupKey,
                    order: getGroupOrder(groupKey),
                    parent: parentKey,
                };
                groupMeta.set(groupKey, next);
                return next;
            };

            visibilityGroup.items.forEach((item) => {
                const groupKey = item.column.group;
                if (!groupKey) return;
                const parentKey = item.column.parent_group || customGroupParents[groupKey] || null;
                ensureGroupMeta(groupKey, parentKey);
                if (parentKey) {
                    ensureGroupMeta(parentKey, customGroupParents[parentKey] || null);
                }
            });

            customGroups.forEach((groupName) => {
                ensureGroupMeta(groupName, customGroupParents[groupName] ?? null);
            });

            const buildEntries = (parentGroupName: string | null, depth: number): Entry[] => {
                const columnEntries: Entry[] = visibilityGroup.items
                    .filter(({ column }) => (column.group || null) === parentGroupName)
                    .map((item) => ({
                        kind: 'column',
                        item,
                        order: item.columnIndex,
                    }));

                const childGroupEntries: Entry[] = [...groupMeta.values()]
                    .filter((group) => (group.parent || null) === parentGroupName)
                    .map((group) => {
                        const entries = buildEntries(group.key, depth + 1);
                        const directColumnCount = visibilityGroup.items.filter(({ column }) => column.group === group.key).length;
                        return {
                            kind: 'group',
                            order: group.order,
                            node: {
                                key: group.key,
                                title: group.title,
                                order: group.order,
                                parent: group.parent,
                                depth,
                                columnCount: directColumnCount,
                                entries,
                            },
                        } satisfies Entry;
                    });

                return sortEntries([...columnEntries, ...childGroupEntries]);
            };

            const entries = buildEntries(null, 0);

            return {
                ...visibilityGroup,
                entries,
            };
        });
    }, [filteredColumns, customGroupOrders, customGroupParents, customGroups, reportFormColumns]);

    const toggleGroup = (groupKey: string) => {
        setCollapsedGroups((prev) => ({
            ...prev,
            [groupKey]: !prev[groupKey],
        }));
    };

    const startEditGroup = (groupKey: string, currentTitle: string) => {
        if (groupKey === '__ungrouped__') return;
        setEditingGroupKey(groupKey);
        setEditingGroupTitle(currentTitle);
    };

    const saveEditGroup = () => {
        if (!editingGroupKey || !editingGroupTitle.trim()) {
            setEditingGroupKey(null);
            return;
        }
        const newTitle = editingGroupTitle.trim();
        if (editingGroupKey !== newTitle) {
            setCustomGroups((prev) => prev.map((g) => (g === editingGroupKey ? newTitle : g)));
            setCustomGroupParents((prev) =>
                Object.fromEntries(
                    Object.entries(prev).map(([groupName, parentName]) => [
                        groupName === editingGroupKey ? newTitle : groupName,
                        parentName === editingGroupKey ? newTitle : parentName,
                    ])
                )
            );
            setCustomGroupOrders((prev) =>
                Object.fromEntries(
                    Object.entries(prev).map(([groupName, order]) => [groupName === editingGroupKey ? newTitle : groupName, order])
                )
            );
            onBulkUpdateColumns(
                reportFormColumns.filter((column) => column.group === editingGroupKey).map((column) => column.key),
                { group: newTitle }
            );
            onBulkUpdateColumns(
                reportFormColumns.filter((column) => column.parent_group === editingGroupKey).map((column) => column.key),
                { parent_group: newTitle }
            );
        }
        setEditingGroupKey(null);
    };

    const handleGroupKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
            saveEditGroup();
        } else if (e.key === 'Escape') {
            setEditingGroupKey(null);
        }
    };

    const addNewGroup = () => {
        const newGroupName = `新分组 ${Date.now().toString().slice(-4)}`;
        setCustomGroups((prev) => [...prev, newGroupName]);
        setCustomGroupParents((prev) => ({ ...prev, [newGroupName]: null }));
        setCustomGroupOrders((prev) => ({
            ...prev,
            [newGroupName]: Math.max(-1, ...Object.values(prev), ...reportFormColumns.map((column) => column.group_order ?? 0)) + 1,
        }));
        setEditingGroupKey(newGroupName);
        setEditingGroupTitle(newGroupName);
    };

    const addChildGroup = (parentGroupKey: string) => {
        const parentGroupName = parentGroupKey.split(':type:')[1] || parentGroupKey;
        const newGroupName = `${parentGroupName}-子组${Date.now().toString().slice(-3)}`;
        setCustomGroups((prev) => [...prev, newGroupName]);
        setCustomGroupParents((prev) => ({ ...prev, [newGroupName]: parentGroupName }));
        setCustomGroupOrders((prev) => {
            const siblingOrders = Object.entries(prev)
                .filter(([groupName]) => (customGroupParents[groupName] ?? null) === parentGroupName)
                .map(([, order]) => order);
            return {
                ...prev,
                [newGroupName]: siblingOrders.length ? Math.max(...siblingOrders) + 1 : 0,
            };
        });
        onSetParentGroup(newGroupName, parentGroupName);
        setCollapsedGroups((prev) => ({
            ...prev,
            [parentGroupKey]: false,
        }));
        setEditingGroupKey(newGroupName);
        setEditingGroupTitle(newGroupName);
    };

    // const expandAllGroups = () => {
    //     setCollapsedGroups({});
    // };

    // const collapseAllGroups = () => {
    //     const nextState: Record<string, boolean> = {};
    //     groupedColumns.forEach((visibilityGroup) => {
    //         nextState[`visibility:${visibilityGroup.key}`] = true;
    //         const walk = (entries: (typeof visibilityGroup.entries)) => {
    //             entries.forEach((entry) => {
    //                 if (entry.kind !== 'group') return;
    //                 nextState[`visibility:${visibilityGroup.key}:type:${entry.node.key}`] = true;
    //                 walk(entry.node.entries);
    //             });
    //         };
    //         walk(visibilityGroup.entries);
    //     });
    //     setCollapsedGroups(nextState);
    // };

    // const moveGroupToTopLevel = (groupKey: string) => {
    //     const groupName = groupKey.split(':type:')[1] || groupKey;
    //     if (!groupName) return;
    //     applyGroupPlacement(
    //         groupName,
    //         null,
    //         Math.max(-1, ...Object.values(customGroupOrders), ...reportFormColumns.map((column) => column.group_order ?? 0)) + 1
    //     );
    // };

    const deleteGroup = (groupKey: string) => {
        if (groupKey === '__ungrouped__') return;
        setCustomGroups((prev) => prev.filter((g) => g !== groupKey));
        setCustomGroupParents((prev) =>
            Object.fromEntries(
                Object.entries(prev)
                    .filter(([groupName]) => groupName !== groupKey)
                    .map(([groupName, parentName]) => [groupName, parentName === groupKey ? null : parentName])
            )
        );
        setCustomGroupOrders((prev) => Object.fromEntries(Object.entries(prev).filter(([groupName]) => groupName !== groupKey)));
        onBulkUpdateColumns(
            reportFormColumns.filter((column) => column.group === groupKey).map((column) => column.key),
            { group: undefined, parent_group: undefined }
        );
        onBulkUpdateColumns(
            reportFormColumns.filter((column) => column.parent_group === groupKey).map((column) => column.key),
            { parent_group: undefined }
        );
    };

    const syncLocalGroupOrder = (groupName: string, anchorGroupName: string, position: 'before' | 'after') => {
        const anchorOrder = customGroupOrders[anchorGroupName] ?? 0;
        const delta = position === 'before' ? -0.5 : 0.5;
        setCustomGroupOrders((prev) => ({ ...prev, [groupName]: anchorOrder + delta }));
        setCustomGroupParents((prev) => ({ ...prev, [groupName]: customGroupParents[anchorGroupName] ?? null }));
    };

    const applyGroupPlacement = (groupName: string, parentGroupName: string | null, order: number) => {
        setCustomGroupParents((prev) => ({ ...prev, [groupName]: parentGroupName }));
        setCustomGroupOrders((prev) => ({ ...prev, [groupName]: order }));
        onBulkUpdateColumns(
            reportFormColumns.filter((column) => column.group === groupName).map((column) => column.key),
            {
                parent_group: parentGroupName || undefined,
                group_order: order,
            }
        );
    };

    const moveGroupRelativeToColumn = (groupKey: string, targetColumn: ReportColumnConfig, position: 'before' | 'after') => {
        const groupName = groupKey.split(':type:')[1] || groupKey;
        if (!groupName) return;
        const parentGroupName = targetColumn.group || null;
        const anchorOrder = reportFormColumns.findIndex((column) => column.key === targetColumn.key);
        if (anchorOrder < 0) return;
        applyGroupPlacement(groupName, parentGroupName, anchorOrder + (position === 'before' ? -0.5 : 0.5));
    };

    const collectEntryColumnIndices = (entries: (typeof groupedColumns)[number]['entries']): number[] =>
        entries.flatMap((entry) =>
            entry.kind === 'column' ? [entry.item.columnIndex] : collectEntryColumnIndices(entry.node.entries)
        );

    const findGroupEntryContext = (
        entries: (typeof groupedColumns)[number]['entries'],
        groupName: string
    ): { entry: Extract<(typeof entries)[number], { kind: 'group' }>; parentEntries: typeof entries; index: number } | null => {
        for (let index = 0; index < entries.length; index += 1) {
            const entry = entries[index];
            if (entry.kind !== 'group') continue;
            if (entry.node.key === groupName) {
                return { entry, parentEntries: entries, index };
            }
            const nested = findGroupEntryContext(entry.node.entries, groupName);
            if (nested) return nested;
        }
        return null;
    };

    const moveColumnRelativeToGroup = (
        columnKey: string,
        targetGroupName: string,
        visibilityGroupKey: string,
        position: 'before' | 'after' | 'inside'
    ) => {
        const sourceIndex = getActualColumnIndex(columnKey);
        if (sourceIndex === -1) return;
        if (position === 'inside') {
            onMoveColumnToGroup(sourceIndex, targetGroupName);
            return;
        }

        const visibilityGroup = groupedColumns.find((group) => `visibility:${group.key}` === visibilityGroupKey);
        if (!visibilityGroup) return;
        const context = findGroupEntryContext(visibilityGroup.entries, targetGroupName);
        if (!context) return;

        const siblingParentGroupName = context.entry.node.parent || null;

        const descendantIndices = collectEntryColumnIndices(context.entry.node.entries).sort((a, b) => a - b);
        if (descendantIndices.length > 0) {
            onPlaceColumn(
                sourceIndex,
                position === 'before' ? descendantIndices[0] : descendantIndices[descendantIndices.length - 1],
                position,
                siblingParentGroupName
            );
            return;
        }

        const siblingEntries = context.parentEntries;
        if (position === 'before') {
            for (let index = context.index - 1; index >= 0; index -= 1) {
                const sibling = siblingEntries[index];
                const anchorIndex =
                    sibling.kind === 'column'
                        ? sibling.item.columnIndex
                        : collectEntryColumnIndices(sibling.node.entries).sort((a, b) => a - b).at(-1);
                if (anchorIndex !== undefined) {
                    onPlaceColumn(sourceIndex, anchorIndex, 'after', siblingParentGroupName);
                    return;
                }
            }
            for (let index = context.index + 1; index < siblingEntries.length; index += 1) {
                const sibling = siblingEntries[index];
                const anchorIndex =
                    sibling.kind === 'column'
                        ? sibling.item.columnIndex
                        : collectEntryColumnIndices(sibling.node.entries).sort((a, b) => a - b)[0];
                if (anchorIndex !== undefined) {
                    onPlaceColumn(sourceIndex, anchorIndex, 'before', siblingParentGroupName);
                    return;
                }
            }
        } else {
            for (let index = context.index + 1; index < siblingEntries.length; index += 1) {
                const sibling = siblingEntries[index];
                const anchorIndex =
                    sibling.kind === 'column'
                        ? sibling.item.columnIndex
                        : collectEntryColumnIndices(sibling.node.entries).sort((a, b) => a - b)[0];
                if (anchorIndex !== undefined) {
                    onPlaceColumn(sourceIndex, anchorIndex, 'before', siblingParentGroupName);
                    return;
                }
            }
            for (let index = context.index - 1; index >= 0; index -= 1) {
                const sibling = siblingEntries[index];
                const anchorIndex =
                    sibling.kind === 'column'
                        ? sibling.item.columnIndex
                        : collectEntryColumnIndices(sibling.node.entries).sort((a, b) => a - b).at(-1);
                if (anchorIndex !== undefined) {
                    onPlaceColumn(sourceIndex, anchorIndex, 'after', siblingParentGroupName);
                    return;
                }
            }
        }
    };

    const renderColumnItem = (
        item: { column: ReportColumnConfig; columnIndex: number },
        groupKey: string | null
    ) => {
        const { column, columnIndex } = item;
        const isDragging = draggingColumnIndex === columnIndex;
        const isDragOver = dragOverColumnIndex === columnIndex && draggingColumnIndex !== columnIndex;
        const currentDropPosition = isDragOver ? dropPosition : null;

        const handleDragOver = (event: React.DragEvent) => {
            const payload = resolveDragPayload(event);
            if (!payload) return;
            event.preventDefault();
            event.dataTransfer.dropEffect = 'move';
            const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
            const mouseY = event.clientY;
            const middleY = rect.top + rect.height / 2;
            setDragOverColumnIndex(columnIndex);
            setDropPosition(mouseY < middleY ? 'before' : 'after');
        };

        return (
            <div
                key={`${column.key}-${columnIndex}`}
                className={`column-config-row ${column.visible ? 'is-visible' : 'is-hidden'} ${isDragging ? 'is-dragging' : ''} ${isDragOver ? 'is-drag-over' : ''} ${currentDropPosition ? `drop-${currentDropPosition}` : ''}`}
                onDragOver={handleDragOver}
                onDragLeave={(event) => {
                    const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
                    const mouseX = event.clientX;
                    const mouseY = event.clientY;
                    if (mouseX < rect.left || mouseX > rect.right || mouseY < rect.top || mouseY > rect.bottom) {
                        setDragOverColumnIndex(null);
                        setDropPosition(null);
                    }
                }}
                onDrop={(event) => {
                    event.preventDefault();
                    event.stopPropagation();
                    const payload = resolveDragPayload(event);
                    if (!payload) return;
                    if (payload.type === 'column') {
                        const actualSourceIndex = getActualColumnIndex(payload.columnKey);
                        const actualTargetIndex = getActualColumnIndex(column.key);
                        if (actualSourceIndex === -1 || actualTargetIndex === -1) return;
                        const targetGroupKey = groupKey;
                        onPlaceColumn(actualSourceIndex, actualTargetIndex, currentDropPosition || 'after', targetGroupKey);
                    } else {
                        moveGroupRelativeToColumn(payload.groupKey, column, currentDropPosition || 'after');
                    }
                    resetDragState();
                }}
            >
                <div
                    className="column-config-drag"
                    draggable
                    onDragStart={(event) => {
                        const payload: ColumnDragPayload = { type: 'column', columnKey: column.key };
                        event.dataTransfer.effectAllowed = 'move';
                        writeDragPayload(event, payload);
                        event.dataTransfer.setDragImage(event.currentTarget, 16, 16);
                        setActiveDragPayload(payload);
                        setDraggingColumnIndex(columnIndex);
                    }}
                    onDragEnd={resetDragState}
                    title="拖拽调整列顺序"
                >
                    <GripVertical size={16} />
                </div>
                <label className="column-visibility-label">
                    <input
                        type="checkbox"
                        checked={column.visible}
                        onChange={(e) => onUpdateColumn(columnIndex, { visible: e.target.checked })}
                    />
                </label>
                <div className="column-config-meta">
                    <strong>{column.key}</strong>
                </div>
                <div className="column-config-cell">
                    <input
                        value={column.label}
                        onChange={(e) => onUpdateColumn(columnIndex, { label: e.target.value })}
                        placeholder="列标题"
                    />
                </div>
                <div className="column-config-cell">
                    <select
                        value={column.type || 'auto'}
                        onChange={(e) => onUpdateColumn(columnIndex, { type: e.target.value as ReportColumnConfig['type'] })}
                    >
                        <option value="auto">自动</option>
                        <option value="text">文本</option>
                        <option value="number">数字</option>
                        <option value="date">日期</option>
                        <option value="datetime">日期时间</option>
                        <option value="currency">金额</option>
                        <option value="percent">百分比</option>
                        <option value="boolean">布尔</option>
                    </select>
                </div>
                <div className="column-config-cell">
                    <select
                        value={column.aggregate || 'none'}
                        onChange={(e) => onUpdateColumn(columnIndex, { aggregate: e.target.value as ReportColumnConfig['aggregate'] })}
                    >
                        <option value="none">不聚合</option>
                        <option value="sum">求和</option>
                        <option value="avg">平均值</option>
                        <option value="min">最小值</option>
                        <option value="max">最大值</option>
                        <option value="count">计数</option>
                        <option value="count_distinct">去重计数</option>
                    </select>
                </div>
                <div className="column-config-cell">
                    <div className="runtime-select-shell">
                        <button
                            type="button"
                            className={`runtime-select-trigger ${openColumnDictionaryIndex === columnIndex ? 'is-active' : ''}`}
                            onClick={() => setOpenColumnDictionaryIndex(openColumnDictionaryIndex === columnIndex ? null : columnIndex)}
                        >
                            {column.dictionary_id ? (dictionaries.find((d) => d.id === column.dictionary_id)?.name ?? '已映射') : '未映射'}
                            {column.dictionary_id ? <SearchCheck size={12} /> : <Search size={12} />}
                        </button>
                        {openColumnDictionaryIndex === columnIndex ? (
                            <div className="runtime-select-panel">
                                <div className="runtime-select-search">
                                    <Search size={14} />
                                    <input
                                        value={columnDictionarySearchText[columnIndex] || ''}
                                        onChange={(e) => setColumnDictionarySearchText((prev) => ({ ...prev, [columnIndex]: e.target.value }))}
                                        placeholder="搜索字典..."
                                    />
                                </div>
                                <div className="runtime-select-options">
                                    <button
                                        type="button"
                                        className="runtime-select-option"
                                        onClick={() => {
                                            onUpdateColumn(columnIndex, { dictionary_id: null, dictionary_display: undefined });
                                            setOpenColumnDictionaryIndex(null);
                                        }}
                                    >
                                        清除映射
                                    </button>
                                    {getFilteredColumnDictionaries(columnIndex).map((dict) => (
                                        <button
                                            key={dict.id}
                                            type="button"
                                            className={`runtime-select-option ${column.dictionary_id === dict.id ? 'is-selected' : ''}`}
                                            onClick={() => {
                                                onUpdateColumn(columnIndex, { dictionary_id: dict.id });
                                                setOpenColumnDictionaryIndex(null);
                                            }}
                                        >
                                            <span className="option-label">{dict.name}</span>
                                            <span className="option-category">{dict.category}</span>
                                        </button>
                                    ))}
                                </div>
                            </div>
                        ) : null}
                    </div>
                </div>
                <div className="column-config-cell column-config-cell-pinned">
                    <label className="pinned-radio-label">
                        <input
                            type="radio"
                            name={`pinned-${column.key}-${columnIndex}`}
                            checked={column.pinned === 'left'}
                            onChange={() => onUpdateColumn(columnIndex, { pinned: column.pinned === 'left' ? 'none' : 'left' })}
                        />
                        <PanelLeft size={12} />
                    </label>
                </div>
                <div className="column-config-cell column-config-cell-style">
                    <button
                        className="btn-outline compact-btn"
                        type="button"
                        onClick={(e) => {
                            e.stopPropagation();
                            const rules = column.style_rules || [];
                            const newRule = createEmptyStyleRule();
                            const newRules = [...rules, newRule];
                            const actualIndex = reportFormColumns.findIndex(c => c.key === column.key);
                            if (actualIndex === -1) return;
                            onUpdateColumn(actualIndex, { style_rules: newRules });
                            setEditingStyleRule({ columnKey: column.key, rule: newRule, ruleIndex: rules.length });
                        }}
                        title="添加样式规则"
                    >
                        <Plus size={14} />
                    </button>
                    {(column.style_rules || []).length > 0 && (
                        <span
                            className="style-rule-count"
                            onClick={(e) => {
                                e.stopPropagation();
                                const firstRule = column.style_rules?.[0];
                                if (firstRule) {
                                    setEditingStyleRule({ columnKey: column.key, rule: firstRule, ruleIndex: 0 });
                                }
                            }}
                            style={{ cursor: 'pointer' }}
                        >
                            {(column.style_rules || []).length}
                        </span>
                    )}
                </div>
                <div className="column-config-cell">
                    <input
                        value={column.width || ''}
                        onChange={(e) => onUpdateColumn(columnIndex, { width: e.target.value })}
                        placeholder="宽度"
                    />
                </div>
                <div className="column-config-cell">
                    <select
                        value={column.sort_order || 'none'}
                        onChange={(e) => onUpdateColumn(columnIndex, { sort_order: e.target.value === 'none' ? null : e.target.value as 'asc' | 'desc' })}
                    >
                        <option value="none">无</option>
                        <option value="asc">升序 ↑</option>
                        <option value="desc">降序 ↓</option>
                    </select>
                </div>
                {currentDropPosition && (
                    <div className={`drop-indicator drop-indicator-${currentDropPosition}`} />
                )}
            </div>
        );
    };

    const renderEntries = (
        entries: (typeof groupedColumns)[number]['entries'],
        visibilityGroupKey: string,
        parentGroupKey: string | null
    ) =>
        entries.map((entry) =>
            entry.kind === 'column'
                ? renderColumnItem(entry.item, parentGroupKey)
                : renderGroupNode(entry.node, visibilityGroupKey)
        );

    const renderGroupNode = (
        typeGroup: Extract<(typeof groupedColumns)[number]['entries'][number], { kind: 'group' }>['node'],
        visibilityGroupKey: string
    ) => {
        const typeGroupKey = `${visibilityGroupKey}:type:${typeGroup.key}`;
        const typeCollapsed = Boolean(collapsedGroups[typeGroupKey]);
        const isEditing = editingGroupKey === typeGroup.key;
        const isGroupDragging = draggingGroupKey === typeGroupKey;
        const isGroupDragOver = dragOverGroupKey === typeGroupKey && draggingGroupKey !== typeGroupKey;
        const depth = typeGroup.depth;

        const handleGroupDragOver = (event: React.DragEvent) => {
            const payload = resolveDragPayload(event);
            if (!payload || (payload.type !== 'group' && payload.type !== 'column')) return;
            if (payload.type === 'group' && payload.groupKey === typeGroupKey) {
                event.preventDefault();
                return;
            }
            event.preventDefault();
            event.dataTransfer.dropEffect = 'move';
            const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
            const mouseY = event.clientY;
            const upperBound = rect.top + rect.height * 0.3;
            const lowerBound = rect.bottom - rect.height * 0.3;
            setDragOverGroupKey(typeGroupKey);
            if (mouseY <= upperBound) {
                setDragOverGroupPosition('before');
            } else if (mouseY >= lowerBound) {
                setDragOverGroupPosition('after');
            } else {
                setDragOverGroupPosition('inside');
            }
        };

        const handleGroupDrop = (event: React.DragEvent) => {
            event.preventDefault();
            event.stopPropagation();
            const payload = resolveDragPayload(event);
            if (!payload) return;
            if (payload.type === 'group') {
                if (payload.groupKey !== typeGroupKey) {
                    if (dragOverGroupPosition === 'inside') {
                        applyGroupPlacement(
                            payload.groupKey.split(':type:')[1] || payload.groupKey,
                            typeGroup.key,
                            Math.max(
                                -1,
                                ...Object.values(customGroupOrders),
                                ...reportFormColumns
                                    .filter((column) => column.group === typeGroup.key)
                                    .map((column) => column.group_order ?? 0)
                            ) + 1
                        );
                    } else {
                        syncLocalGroupOrder(
                            payload.groupKey.split(':type:')[1] || payload.groupKey,
                            typeGroup.key,
                            dragOverGroupPosition === 'before' ? 'before' : 'after'
                        );
                        onBulkUpdateColumns(
                            reportFormColumns
                                .filter((column) => column.group === (payload.groupKey.split(':type:')[1] || payload.groupKey))
                                .map((column) => column.key),
                            {
                                parent_group: customGroupParents[typeGroup.key] || undefined,
                                group_order:
                                    (customGroupOrders[typeGroup.key] ?? typeGroup.order) +
                                    (dragOverGroupPosition === 'before' ? -0.5 : 0.5),
                            }
                        );
                    }
                }
            } else if (payload.type === 'column') {
                moveColumnRelativeToGroup(payload.columnKey, typeGroup.key, visibilityGroupKey, dragOverGroupPosition || 'inside');
            }
            resetDragState();
        };

        const handleGroupDragEnd = () => {
            resetDragState();
        };

        const canDragGroup = typeGroup.key !== '__ungrouped__';

        return (
            <div key={typeGroupKey}>
                <div
                    className={`column-config-subgroup depth-${depth} ${isGroupDragging ? 'is-dragging' : ''} ${isGroupDragOver ? `is-drag-over drag-over-${dragOverGroupPosition}` : ''}`}
                    onDragOver={handleGroupDragOver}
                    onDragLeave={(event) => {
                        const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
                        const mouseX = event.clientX;
                        const mouseY = event.clientY;
                        if (mouseX < rect.left || mouseX > rect.right || mouseY < rect.top || mouseY > rect.bottom) {
                            setDragOverGroupKey(null);
                            setDragOverGroupPosition(null);
                        }
                    }}
                    onDrop={handleGroupDrop}
                >
                    <div className="column-config-subgroup-toggle">
                        {canDragGroup ? (
                            <div
                                className="group-drag-handle"
                                draggable
                                onDragStart={(event) => {
                                    const payload: GroupDragPayload = { type: 'group', groupKey: typeGroupKey };
                                    event.dataTransfer.effectAllowed = 'move';
                                    writeDragPayload(event, payload);
                                    event.dataTransfer.setDragImage(event.currentTarget, 16, 16);
                                    setActiveDragPayload(payload);
                                    setDraggingGroupKey(typeGroupKey);
                                }}
                                onDragEnd={handleGroupDragEnd}
                                title="拖拽调整分组顺序，拖到组中间可嵌套到当前组内"
                            >
                                <GripVertical size={14} />
                            </div>
                        ) : (
                            <span className="group-drag-placeholder" />
                        )}
                        <button
                            type="button"
                            className="column-config-subgroup-toggle-btn"
                            onClick={() => toggleGroup(typeGroupKey)}
                        >
                            {typeCollapsed ? <ChevronRight size={14} /> : <ChevronDown size={14} />}
                            {isEditing ? (
                                <input
                                    className="group-name-input"
                                    value={editingGroupTitle}
                                    onChange={(e) => setEditingGroupTitle(e.target.value)}
                                    onBlur={saveEditGroup}
                                    onKeyDown={handleGroupKeyDown}
                                    autoFocus
                                    onClick={(e) => e.stopPropagation()}
                                />
                            ) : (
                                <span className="column-config-group-title">
                                    {typeGroup.title}
                                </span>
                            )}
                        </button>
                        <span className="resource-meta">{typeGroup.columnCount} 列</span>
                        {typeGroup.key !== '__ungrouped__' && (
                            <div className="column-config-group-actions">
                                <button
                                    type="button"
                                    className="column-config-group-action-btn"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        addChildGroup(typeGroup.key);
                                    }}
                                    title="新增子组"
                                >
                                    子组
                                </button>
                                <button
                                    type="button"
                                    className="column-config-group-action-btn"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        startEditGroup(typeGroup.key, typeGroup.title);
                                    }}
                                    title="重命名分组"
                                >
                                    重命名
                                </button>
                                <button
                                    type="button"
                                    className="column-config-group-action-btn column-config-group-action-btn-danger"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        deleteGroup(typeGroup.key);
                                    }}
                                    title="删除分组"
                                >
                                    删除
                                </button>
                            </div>
                        )}
                    </div>
                    {isGroupDragOver && dragOverGroupPosition ? (
                        <div className={`group-drop-hint group-drop-hint-${dragOverGroupPosition}`}>
                            {dragOverGroupPosition === 'inside'
                                ? '释放后嵌套到当前组内'
                                : dragOverGroupPosition === 'before'
                                  ? '释放后移动到当前组前'
                                  : '释放后移动到当前组后'}
                        </div>
                    ) : null}
                    {!typeCollapsed && (
                        <>
                            <div className="column-config-subgroup-list">
                                {typeGroup.entries.length ? (
                                    renderEntries(typeGroup.entries, visibilityGroupKey, typeGroup.key)
                                ) : (
                                    <div className="ungrouped-drop-zone is-empty">拖到这里可放入当前组内</div>
                                )}
                            </div>
                        </>
                    )}
                </div>
            </div>
        );
    };

    const getFilteredDictionaries = (filterIndex: number) => {
        const keyword = String(filterDictionarySearchText[filterIndex] || '').trim().toLowerCase();
        if (!keyword) return dictionaries;
        return dictionaries.filter((dictionary) =>
            [dictionary.name, dictionary.key, dictionary.description, dictionary.category]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(keyword))
        );
    };

    const getFilteredColumnDictionaries = (columnIndex: number) => {
        const keyword = String(columnDictionarySearchText[columnIndex] || '').trim().toLowerCase();
        if (!keyword) return dictionaries;
        return dictionaries.filter((dictionary) =>
            [dictionary.name, dictionary.key, dictionary.description, dictionary.category]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(keyword))
        );
    };

    // const addColumnStyleRule = (columnIndex: number) => {
    //     onUpdateColumn(columnIndex, {
    //         style_rules: [...(reportFormColumns[columnIndex]?.style_rules || []), createEmptyStyleRule()],
    //     });
    // };

    return (
        <FormModal
            open={open}
            title={editingReportId ? '编辑报表' : '新建报表'}
            subtitle="在弹窗里完成报表定义，页面只保留目录和运行区。"
            width="1180px"
            closeOnBackdrop={false}
            onClose={onClose}
        >
            <div className="report-modal-actions report-modal-actions-top">
                <div className="editor-actions report-modal-inline-actions">
                    <button className="btn-outline" onClick={onReset}>重置</button>
                    <button
                        className="btn-outline"
                        disabled={reportPreviewLoading || !reportForm.dataset_id}
                        onClick={() => {
                            setActiveTab('preview');
                            void onPreview();
                        }}
                    >
                        {reportPreviewLoading ? <Loader2 size={14} className="spin-icon" /> : <Eye size={14} />}
                        预览效果
                    </button>
                    <button className="btn-primary" onClick={() => void onSave()}>
                        <Save size={14} />
                        保存
                    </button>
                </div>
            </div>
            <div className="report-modal-shell">
                <div className="report-modal-tabs">
                    <button
                        type="button"
                        className={`report-modal-tab ${activeTab === 'basic' ? 'active' : ''}`}
                        onClick={() => setActiveTab('basic')}
                    >
                        基础设置
                    </button>
                    <button
                        type="button"
                        className={`report-modal-tab ${activeTab === 'filters' ? 'active' : ''}`}
                        onClick={() => setActiveTab('filters')}
                    >
                        筛选器
                        <span>{reportFormFilters.length}</span>
                    </button>
                    <button
                        type="button"
                        className={`report-modal-tab ${activeTab === 'columns' ? 'active' : ''}`}
                        onClick={() => setActiveTab('columns')}
                    >
                        列配置
                        <span>{reportFormColumns.length}</span>
                    </button>
                    <button
                        type="button"
                        className={`report-modal-tab ${activeTab === 'style' ? 'active' : ''}`}
                        onClick={() => setActiveTab('style')}
                    >
                        <Palette size={14} />
                        表格样式
                    </button>
                    <button
                        type="button"
                        className={`report-modal-tab ${activeTab === 'preview' ? 'active' : ''}`}
                        onClick={() => setActiveTab('preview')}
                    >
                        <Eye size={14} />
                        效果预览
                    </button>
                </div>
                {activeTab === 'basic' ? (
                    <div className="report-modal-panel">
                        <div className="report-modal-compact-grid">
                            <div className="report-modal-section report-modal-section-intro">
                                <div className="report-inherit-banner">
                                    <div className="report-inherit-copy">
                                        <span className="inherit-banner-label">数据集筛选继承</span>
                                        <span className="inherit-banner-text">
                                            {selectedDataset ? selectedDataset.name : '未选择数据集'}
                                        </span>
                                    </div>
                                    <button className="btn-outline btn-sm" onClick={onAutoFilters}>
                                        <SearchCheck size={14} />
                                        继承筛选器
                                    </button>
                                </div>
                            </div>
                            <div className="report-modal-section">
                                <div className="filter-builder-head report-modal-card-head">
                                    <div>
                                        <div className="filter-builder-title">基础信息</div>
                                        <div className="resource-meta">先完成数据集和名称配置。</div>
                                    </div>
                                </div>
                                <div className="form-grid two compact-form-grid">
                                    <label>
                                        <span>数据集</span>
                                        <Select
                                            value={reportForm.dataset_id}
                                            onChange={(v) => onDatasetChange(v)}
                                            options={[
                                                { value: '', label: '请选择' },
                                                ...datasets.map((item) => ({ value: String(item.id), label: item.name })),
                                            ]}
                                        />
                                    </label>
                                    <label>
                                        <span>分类</span>
                                        <Select
                                            value={reportForm.category_id}
                                            onChange={(v) => setReportForm((prev) => ({ ...prev, category_id: v }))}
                                            options={[
                                                { value: '', label: '未分类' },
                                                ...(() => {
                                                    const parentIds = new Set(categories.map((c) => c.parent_id).filter(Boolean) as number[]);
                                                    return categories
                                                        .filter((item) => !parentIds.has(item.id))
                                                        .map((item) => ({ value: String(item.id), label: item.path || item.name }));
                                                })(),
                                            ]}
                                        />
                                        {reportForm.category_id && (
                                            <span className="resource-meta" style={{ marginTop: 4, display: 'block', color: '#3b82f6' }}>
                                                已选择：{categories.find((c) => String(c.id) === reportForm.category_id)?.path || categories.find((c) => String(c.id) === reportForm.category_id)?.name || ''}
                                            </span>
                                        )}
                                    </label>
                                    <label>
                                        <span>类型</span>
                                        <Select
                                            value={reportForm.report_type}
                                            onChange={(v) => setReportForm((prev) => ({ ...prev, report_type: v }))}
                                            options={[
                                                { value: 'table', label: '明细报表' },
                                                { value: 'summary', label: '汇总报表' },
                                            ]}
                                        />
                                    </label>
                                </div>
                                <div className="form-grid two compact-form-grid">
                                    <label>
                                        <span>名称</span>
                                        <input value={reportForm.name} onChange={(e) => setReportForm((prev) => ({ ...prev, name: e.target.value }))} />
                                    </label>
                                    <label>
                                        <span>描述</span>
                                        <input value={reportForm.description} onChange={(e) => setReportForm((prev) => ({ ...prev, description: e.target.value }))} />
                                    </label>
                                </div>
                            </div>
                            <div className="report-modal-section">
                                <div className="filter-builder-head report-modal-card-head">
                                    <div>
                                        <div className="filter-builder-title">运行设置</div>
                                        <div className="resource-meta">控制聚合口径和图表摘要。</div>
                                    </div>
                                </div>
                                <div className="form-grid two compact-form-grid">
                                    <label>
                                        <span>聚合统计范围</span>
                                        <Select
                                            value={reportForm.aggregate_scope}
                                            onChange={(v) => setReportForm((prev) => ({ ...prev, aggregate_scope: v as ReportFormState['aggregate_scope'] }))}
                                            options={[
                                                { value: 'returned', label: '当前返回结果' },
                                                { value: 'filtered', label: '筛选后结果（最多 5000 行）' },
                                            ]}
                                        />
                                    </label>
                                </div>
                                <div className="chart-config-grid compact-chart-config-grid">
                                    <label className="switch-chip">
                                        <input type="checkbox" checked={reportForm.chart_enabled} onChange={(e) => setReportForm((prev) => ({ ...prev, chart_enabled: e.target.checked }))} />
                                        启用图表
                                    </label>
                                    <label>
                                        <span>图表类型</span>
                                        <Select
                                            value={reportForm.chart_type}
                                            onChange={(v) => setReportForm((prev) => ({ ...prev, chart_type: v as ChartType }))}
                                            options={[
                                                { value: 'bar', label: '柱状图' },
                                                { value: 'line', label: '折线图' },
                                                { value: 'pie', label: '饼图' },
                                            ]}
                                        />
                                    </label>
                                    <label><span>分类字段</span><input value={reportForm.category_field} onChange={(e) => setReportForm((prev) => ({ ...prev, category_field: e.target.value }))} placeholder="例如 order_date" /></label>
                                    <label><span>数值字段</span><input value={reportForm.value_field} onChange={(e) => setReportForm((prev) => ({ ...prev, value_field: e.target.value }))} placeholder="例如 amount" /></label>
                                    <label>
                                        <span>聚合方式</span>
                                        <Select
                                            value={reportForm.aggregate}
                                            onChange={(v) => setReportForm((prev) => ({ ...prev, aggregate: v as 'sum' | 'count' }))}
                                            options={[
                                                { value: 'sum', label: '求和' },
                                                { value: 'count', label: '计数' },
                                            ]}
                                        />
                                    </label>
                                </div>
                            </div>
                        </div>
                    </div>
                ) : null}
                {activeTab === 'filters' ? (
                    <div className="report-modal-panel">
                        <div className="filter-builder report-modal-section">
                    <div className="filter-builder-head">
                        <div>
                            <div className="filter-builder-title">筛选器定义</div>
                            <div className="resource-meta">配置运行时过滤条件，替代手工编辑 JSON。</div>
                        </div>
                        <button className="btn-outline" type="button" onClick={onAddFilter}>
                            <Plus size={14} />
                            添加筛选器
                        </button>
                    </div>
                    <div className="filter-config-list-wrapper">
                        <div className="filter-config-header">
                            <span>操作</span>
                            <span>参数键</span>
                            <span>显示名</span>
                            <span>类型</span>
                            <span>默认值</span>
                            <span>字典绑定</span>
                            <span>占位提示</span>
                            <span>宽度</span>
                            <span>下拉选项</span>
                        </div>
                    {reportFormFilters.length ? (
                        <div className="filter-config-list">
                            {reportFormFilters.map((filter, filterIndex) => (
                                <div key={`filter-${filterIndex}`} className="filter-config-row">
                                    <div className="filter-config-cell filter-config-cell-action">
                                        <button className="danger ghost-btn" type="button" onClick={() => onRemoveFilter(filterIndex)} title="删除筛选器">
                                            <Trash2 size={14} />
                                        </button>
                                    </div>
                                    <div className="filter-config-cell">
                                        <select
                                            value={filter.key}
                                            onChange={(e) => {
                                                const selectedKey = e.target.value;
                                                const patch: Partial<ReportFilter> = { key: selectedKey };
                                                const col = selectedDatasetColumns.find((c) => c.name === selectedKey);
                                                if (col && !filter.label) {
                                                    patch.label = selectedKey;
                                                }
                                                if (col) {
                                                    if (col.type === 'integer' || col.type === 'number') {
                                                        patch.type = 'number';
                                                    } else if (col.type === 'date' || col.type === 'datetime') {
                                                        patch.type = 'date';
                                                    }
                                                }
                                                onUpdateFilter(filterIndex, patch);
                                            }}
                                        >
                                            <option value="">选择字段</option>
                                            {selectedDatasetColumns.length > 0 ? (
                                                selectedDatasetColumns.map((col) => (
                                                    <option key={col.name} value={col.name}>
                                                        {col.name} ({col.type})
                                                    </option>
                                                ))
                                            ) : (
                                                <option value="" disabled>请先选择数据集</option>
                                            )}
                                            {filter.key && !selectedDatasetColumns.some((c) => c.name === filter.key) && (
                                                <option value={filter.key}>{filter.key} (自定义)</option>
                                            )}
                                        </select>
                                    </div>
                                    <div className="filter-config-cell">
                                        <input value={filter.label} onChange={(e) => onUpdateFilter(filterIndex, { label: e.target.value })} placeholder="例如 项目" />
                                    </div>
                                    <div className="filter-config-cell">
                                        <select
                                            value={filter.type}
                                            onChange={(e) =>
                                                onUpdateFilter(filterIndex, {
                                                    type: e.target.value as FilterType,
                                                    ...(e.target.value !== 'select' ? { dictionary_id: null } : {}),
                                                })
                                            }
                                        >
                                            <option value="text">文本</option>
                                            <option value="number">数字</option>
                                            <option value="date">日期</option>
                                            <option value="select">下拉</option>
                                        </select>
                                    </div>
                                    <div className="filter-config-cell">
                                        <input value={filter.default_value || ''} onChange={(e) => onUpdateFilter(filterIndex, { default_value: e.target.value })} placeholder="可选" />
                                    </div>
                                    <div className="filter-config-cell">
                                        <div className="runtime-select-shell">
                                            <button
                                                type="button"
                                                className={`runtime-select-trigger ${openFilterDictionaryIndex === filterIndex ? 'active' : ''}`}
                                                onClick={() => setOpenFilterDictionaryIndex((prev) => (prev === filterIndex ? null : filterIndex))}
                                            >
                                                <span className="runtime-select-trigger-text">
                                                    {filter.dictionary_id
                                                        ? dictionaries.find((dictionary) => dictionary.id === filter.dictionary_id)?.name || `字典 #${filter.dictionary_id}`
                                                        : '无'}
                                                </span>
                                                <ChevronDown size={14} />
                                            </button>
                                            {openFilterDictionaryIndex === filterIndex ? (
                                                <div className="runtime-select-panel">
                                                    <div className="runtime-select-search">
                                                        <Search size={14} />
                                                        <input
                                                            value={filterDictionarySearchText[filterIndex] || ''}
                                                            onChange={(e) =>
                                                                setFilterDictionarySearchText((prev) => ({
                                                                    ...prev,
                                                                    [filterIndex]: e.target.value,
                                                                }))
                                                            }
                                                            placeholder="搜索字典名称、键或分类"
                                                        />
                                                    </div>
                                                    <div className="runtime-select-options">
                                                        <button
                                                            type="button"
                                                            className={`runtime-select-option ${!filter.dictionary_id ? 'active' : ''}`}
                                                            onClick={() => {
                                                                onUpdateFilter(filterIndex, { dictionary_id: null });
                                                                setOpenFilterDictionaryIndex(null);
                                                            }}
                                                        >
                                                            <span>不使用字典</span>
                                                            <em>manual</em>
                                                        </button>
                                                        {getFilteredDictionaries(filterIndex).map((dictionary) => (
                                                            <button
                                                                key={`filter-dict-${dictionary.id}`}
                                                                type="button"
                                                                className={`runtime-select-option ${filter.dictionary_id === dictionary.id ? 'active' : ''}`}
                                                                onClick={() => {
                                                                    onUpdateFilter(filterIndex, {
                                                                        dictionary_id: dictionary.id,
                                                                        type: 'select',
                                                                    });
                                                                    setOpenFilterDictionaryIndex(null);
                                                                }}
                                                            >
                                                                <span>{dictionary.name}</span>
                                                                <em>{dictionary.key}</em>
                                                            </button>
                                                        ))}
                                                        {!getFilteredDictionaries(filterIndex).length ? (
                                                            <div className="runtime-select-empty">没有匹配的字典</div>
                                                        ) : null}
                                                    </div>
                                                </div>
                                            ) : null}
                                        </div>
                                    </div>
                                    <div className="filter-config-cell">
                                        <input value={filter.placeholder || ''} onChange={(e) => onUpdateFilter(filterIndex, { placeholder: e.target.value })} placeholder="例如 输入项目名称" />
                                    </div>
                                    <div className="filter-config-cell filter-config-cell-width">
                                        <input value={filter.width || ''} onChange={(e) => onUpdateFilter(filterIndex, { width: e.target.value })} placeholder="例如 200px 或 50%" />
                                    </div>
                                    <div className="filter-config-cell filter-config-cell-options">
                                        {filter.type === 'select' && !filter.dictionary_id ? (
                                            <div className="filter-options-inline">
                                                <button
                                                    className="btn-outline compact-btn"
                                                    type="button"
                                                    onClick={() => onAddFilterOption(filterIndex)}
                                                    title="添加选项"
                                                >
                                                    <Plus size={14} />
                                                </button>
                                                {(filter.options || []).length > 0 && (
                                                    <span className="filter-options-count" title={`${(filter.options || []).length} 个选项`}>
                                                        {(filter.options || []).length}
                                                    </span>
                                                )}
                                            </div>
                                        ) : filter.type === 'select' && filter.dictionary_id ? (
                                            <span className="filter-dictionary-bound" title="已绑定字典">
                                                <SearchCheck size={14} />
                                            </span>
                                        ) : (
                                            <span className="filter-options-na">-</span>
                                        )}
                                    </div>
                                    {filter.type === 'select' && !filter.dictionary_id && (filter.options || []).length > 0 && (
                                        <div className="filter-options-row">
                                            <div className="filter-options-list-inline">
                                                {(filter.options || []).map((option, optionIndex) => (
                                                    <div key={`filter-${filterIndex}-option-${optionIndex}`} className="filter-option-item">
                                                        <input value={option.label} onChange={(e) => onUpdateFilterOption(filterIndex, optionIndex, { label: e.target.value })} placeholder="显示文本" />
                                                        <input value={option.value} onChange={(e) => onUpdateFilterOption(filterIndex, optionIndex, { value: e.target.value })} placeholder="实际值" />
                                                        <button className="danger ghost-btn" type="button" onClick={() => onRemoveFilterOption(filterIndex, optionIndex)}>
                                                            <Trash2 size={12} />
                                                        </button>
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="empty-box compact-empty">暂无筛选器，报表将直接按默认参数运行。</div>
                    )}
                    </div>
                </div>
                    </div>
                ) : null}
                {activeTab === 'columns' ? (
                    <div className="report-modal-panel">
                        <div className="column-config-panel report-modal-section">
                <div className="column-config-toolbar">
                    <div className="column-config-search">
                        <Search size={14} />
                        <input
                            value={columnKeyword}
                            onChange={(e) => setColumnKeyword(e.target.value)}
                            placeholder="搜索字段名、标题、类型、说明"
                        />
                    </div>
                    <div className="column-config-filter-tabs">
                        <button
                            type="button"
                            className={columnVisibilityFilter === 'all' ? 'active' : ''}
                            onClick={() => setColumnVisibilityFilter('all')}
                        >
                            全部 {reportFormColumns.length}
                        </button>
                        <button
                            type="button"
                            className={columnVisibilityFilter === 'visible' ? 'active' : ''}
                            onClick={() => setColumnVisibilityFilter('visible')}
                        >
                            已显示 {visibleColumnCount}
                        </button>
                        <button
                            type="button"
                            className={columnVisibilityFilter === 'hidden' ? 'active' : ''}
                            onClick={() => setColumnVisibilityFilter('hidden')}
                        >
                            已隐藏 {hiddenColumnCount}
                        </button>
                    </div>
                    <button type="button" className="btn-outline add-group-btn" onClick={addNewGroup}>
                        <Plus size={14} />
                        添加分组
                    </button>
                </div>
                <div className="column-config-list-wrapper">
                {reportFormColumns.length ? (
                    <div className="column-config-list">
                        <div className="column-config-header">
                            <span>拖拽</span>
                            <span><input type="checkbox" checked={visibleColumnCount === reportFormColumns.length && reportFormColumns.length > 0} onChange={(e) => onToggleAllColumns(e.target.checked)} title="全选/取消" /></span>
                            <span>字段</span>
                            <span>标题</span>
                            <span>类型</span>
                            <span>聚合</span>
                            <span>字典映射</span>
                            <span>固定</span>
                            <span>条件样式</span>
                            <span>宽度</span>
                            <span>排序</span>
                        </div>
                        {renderEntries(groupedColumns[0]?.entries ?? [], 'all', null)}
                    </div>
                ) : (
                    <div className="empty-box compact-empty">当前数据集还没有可用字段，请先在数据集里预览或校验生成字段结构。</div>
                )}
                </div>
                </div>
                    </div>
                ) : null}
                {activeTab === 'style' ? (
                    <TableStylePanel
                        tableStyleJson={reportForm.table_style_json}
                        onChange={(json) => setReportForm((prev) => ({ ...prev, table_style_json: json }))}
                    />
                ) : null}
                {activeTab === 'preview' ? (
                    <div className="report-modal-panel report-preview-panel">
                        <div className="report-preview-toolbar">
                            <div className="report-preview-toolbar-left">
                                <span className="report-preview-title">效果预览</span>
                                <span className="report-preview-hint">基于当前配置预览报表展示效果，与最终呈现一致。</span>
                            </div>
                            <button
                                className="btn-outline btn-sm"
                                disabled={reportPreviewLoading || !reportForm.dataset_id}
                                onClick={() => void onPreview()}
                            >
                                {reportPreviewLoading ? <Loader2 size={14} className="spin-icon" /> : <Eye size={14} />}
                                刷新预览
                            </button>
                        </div>
                        <div className="report-preview-content">
                            {reportPreviewError ? (
                                <div className="preview-status error">{reportPreviewError}</div>
                            ) : reportPreviewLoading ? (
                                <div className="preview-status loading">
                                    <Loader2 size={16} className="spin-icon" />
                                    正在执行预览查询，请稍候...
                                </div>
                            ) : reportPreviewResult ? (
                                (() => {
                                    const ts = (() => { try { return JSON.parse(reportForm.table_style_json || '{}') as TableStyleConfig; } catch { return {} as TableStyleConfig; } })();
                                    const previewFilters = reportFormFilters.map(normalizeFilter);
                                    const visibleColumns = reportFormColumns.filter((c) => c.visible);
                                    const resolvedColumns = visibleColumns.map((col) => {
                                        const sourceCol = reportPreviewResult.columns.find((sc) => sc.name === col.key);
                                        return { config: col, source: sourceCol };
                                    }).filter((item) => item.source);
                                    const paginationEnabled = ts.pagination_enabled ?? false;
                                    const pageSizeOptions = ts.page_size_options || [10, 20, 50, 100];
                                    const pagedRows = paginationEnabled
                                        ? reportPreviewResult.rows.slice((previewCurrentPage - 1) * previewCurrentPageSize, previewCurrentPage * previewCurrentPageSize)
                                        : reportPreviewResult.rows;
                                    const totalPages = paginationEnabled && reportPreviewResult.rows.length
                                        ? Math.max(1, Math.ceil(reportPreviewResult.rows.length / previewCurrentPageSize))
                                        : 1;
                                    const aggregateSummaries = visibleColumns
                                        .filter((col) => col.aggregate && col.aggregate !== 'none')
                                        .map((col) => {
                                            const values = reportPreviewResult.rows.map((r) => r[col.key]);
                                            let value = '-';
                                            const nums = values.map((v) => { const n = typeof v === 'number' ? v : Number(v); return Number.isFinite(n) ? n : null; }).filter((n): n is number => n !== null);
                                            if (col.aggregate === 'count') value = String(values.filter((v) => v !== null && v !== undefined && v !== '').length);
                                            else if (col.aggregate === 'count_distinct') value = String(new Set(values.filter((v) => v !== null && v !== undefined && v !== '').map(String)).size);
                                            else if (nums.length) {
                                                if (col.aggregate === 'sum') value = nums.reduce((a, b) => a + b, 0).toFixed(2);
                                                else if (col.aggregate === 'avg') value = (nums.reduce((a, b) => a + b, 0) / nums.length).toFixed(2);
                                                else if (col.aggregate === 'min') value = Math.min(...nums).toFixed(2);
                                                else if (col.aggregate === 'max') value = Math.max(...nums).toFixed(2);
                                            }
                                            return { key: col.key, label: col.label || col.key, method: col.aggregate!, value };
                                        });

                                    return (
                                        <>
                                            {/* 筛选面板 */}
                                            {previewFilters.length > 0 && previewFiltersVisible && (
                                                <div className="report-viewer-filters" style={{ marginBottom: '0.75rem' }}>
                                                    <div className="report-viewer-filters-body">
                                                        {previewFilters.map((filter) => {
                                                            const dictItems = typeof filter.dictionary_id === 'number'
                                                                ? dictionaryItemsById[filter.dictionary_id] || [] : [];
                                                            const filterStyle = filter.width ? { width: filter.width, minWidth: undefined } : undefined;
                                                            return (
                                                                <label key={filter.key} className="report-viewer-filter-item" style={filterStyle}>
                                                                    <span>{filter.label || filter.key}</span>
                                                                    {filter.type === 'select' && (filter.options?.length || dictItems.length) ? (
                                                                        <select
                                                                            value={previewRuntimeFilters[filter.key] || ''}
                                                                            onChange={(e) => setPreviewRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}
                                                                        >
                                                                            <option value="">全部</option>
                                                                            {dictItems.length
                                                                                ? dictItems.map((item) => (
                                                                                    <option key={item.key} value={item.key}>{item.path || item.label}</option>
                                                                                ))
                                                                                : (filter.options || []).map((opt) => (
                                                                                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                                                                                ))}
                                                                        </select>
                                                                    ) : filter.type === 'date' ? (
                                                                        <input
                                                                            type="date"
                                                                            value={previewRuntimeFilters[filter.key] || ''}
                                                                            onChange={(e) => setPreviewRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}
                                                                        />
                                                                    ) : (
                                                                        <input
                                                                            type={filter.type === 'number' ? 'number' : 'text'}
                                                                            value={previewRuntimeFilters[filter.key] || ''}
                                                                            onChange={(e) => setPreviewRuntimeFilters((prev) => ({ ...prev, [filter.key]: e.target.value }))}
                                                                            placeholder={filter.placeholder || `输入${filter.label || filter.key}`}
                                                                        />
                                                                    )}
                                                                </label>
                                                            );
                                                        })}
                                                    </div>
                                                    <div className="report-viewer-filters-actions">
                                                        <button className="btn-primary btn-sm" onClick={() => void onPreview()}>
                                                            <Play size={14} />
                                                            查询
                                                        </button>
                                                        <button className="btn-outline btn-sm" onClick={() => setPreviewFiltersVisible(false)}>
                                                            <ChevronUp size={14} />
                                                            收起
                                                        </button>
                                                    </div>
                                                </div>
                                            )}
                                            {/* 筛选器展开按钮（收起时） */}
                                            {previewFilters.length > 0 && !previewFiltersVisible && (
                                                <div style={{ marginBottom: '0.75rem' }}>
                                                    <button
                                                        className={`btn-outline btn-sm${previewFiltersVisible ? ' active' : ''}`}
                                                        onClick={() => setPreviewFiltersVisible(true)}
                                                    >
                                                        <Filter size={14} />
                                                        筛选
                                                    </button>
                                                </div>
                                            )}
                                            {/* 表格 */}
                                            <div
                                                className="report-preview-table-wrapper"
                                                style={{
                                                    borderRadius: ts.border_radius || undefined,
                                                    border: ts.border_style === 'none' ? 'none' : ts.border_style ? `${ts.border_style} 1px ${ts.border_color || 'rgba(226, 232, 240, 0.6)'}` : undefined,
                                                } as React.CSSProperties}
                                            >
                                                <table
                                                    className={`report-preview-table${ts.striped === false ? ' no-stripes' : ''}`}
                                                    style={{ fontSize: ts.font_size || undefined } as React.CSSProperties}
                                                >
                                                    <thead>
                                                        <tr>
                                                            {ts.show_row_number && (
                                                                <th style={{ width: '48px', textAlign: 'center', background: ts.header_background || undefined, color: ts.header_color || undefined, fontSize: ts.header_font_size || undefined, fontWeight: ts.header_font_weight ? Number(ts.header_font_weight) : undefined }}>#</th>
                                                            )}
                                                            {resolvedColumns.map((item) => (
                                                                <th key={item.config.key} style={{
                                                                    width: item.config.width || undefined,
                                                                    background: ts.header_background || undefined,
                                                                    color: ts.header_color || undefined,
                                                                    fontSize: ts.header_font_size || undefined,
                                                                    fontWeight: ts.header_font_weight ? Number(ts.header_font_weight) : undefined,
                                                                } as React.CSSProperties}>
                                                                    {item.config.label || item.config.key}
                                                                </th>
                                                            ))}
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {pagedRows.map((row, ri) => (
                                                            <tr key={ri} style={ts.row_height ? { height: ts.row_height } : undefined}>
                                                                {ts.show_row_number && (
                                                                    <td style={{ textAlign: 'center', color: '#94a3b8', fontSize: '0.78rem', background: ts.body_background || undefined }}>
                                                                        {paginationEnabled ? (previewCurrentPage - 1) * previewCurrentPageSize + ri + 1 : ri + 1}
                                                                    </td>
                                                                )}
                                                                {resolvedColumns.map((item) => {
                                                                    const value = row[item.config.key];
                                                                    const dictItems = typeof item.config.dictionary_id === 'number'
                                                                        ? dictionaryItemsById[item.config.dictionary_id] || [] : [];
                                                                    const displayText = formatValueByColumnConfig(value, {
                                                                        configuredType: item.config.type,
                                                                        columnType: item.source?.type,
                                                                        sample: item.source?.sample,
                                                                        dictionaryItems: dictItems,
                                                                        dictionaryDisplay: item.config.dictionary_display,
                                                                    });
                                                                    const cellStyle = getColumnStyleForValue(value, item.config.style_rules, row);
                                                                    return (
                                                                        <td key={item.config.key} style={{
                                                                            background: ts.body_background || undefined,
                                                                            color: ts.body_color || undefined,
                                                                            fontSize: ts.body_font_size || undefined,
                                                                        } as React.CSSProperties}>
                                                                            <span title={displayText} style={cellStyle || undefined}>{displayText}</span>
                                                                        </td>
                                                                    );
                                                                })}
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                    {aggregateSummaries.length > 0 && (
                                                        <tfoot>
                                                            <tr>
                                                                {ts.show_row_number && (
                                                                    <td style={{ background: ts.footer_background || undefined, color: ts.footer_color || undefined }}></td>
                                                                )}
                                                                {resolvedColumns.map((item) => {
                                                                    const summary = aggregateSummaries.find((s) => s.key === item.config.key);
                                                                    return (
                                                                        <td key={item.config.key} style={{
                                                                            background: ts.footer_background || undefined,
                                                                            color: ts.footer_color || undefined,
                                                                        } as React.CSSProperties}>
                                                                            {summary ? <span className="aggregate-footer-value">{summary.value}</span> : null}
                                                                        </td>
                                                                    );
                                                                })}
                                                            </tr>
                                                        </tfoot>
                                                    )}
                                                </table>
                                            </div>
                                            {/* 分页 */}
                                            {paginationEnabled && reportPreviewResult.rows.length > 0 && (
                                                <div className="report-viewer-pagination">
                                                    <div className="pagination-info">
                                                        第 {(previewCurrentPage - 1) * previewCurrentPageSize + 1}–{Math.min(previewCurrentPage * previewCurrentPageSize, reportPreviewResult.rows.length)} 行，共 {reportPreviewResult.rows.length} 行
                                                    </div>
                                                    <div className="pagination-controls">
                                                        <select
                                                            className="pagination-page-size"
                                                            value={previewCurrentPageSize}
                                                            onChange={(e) => {
                                                                setPreviewCurrentPageSize(Number(e.target.value));
                                                                setPreviewCurrentPage(1);
                                                            }}
                                                        >
                                                            {pageSizeOptions.map((size) => (
                                                                <option key={size} value={size}>{size} 行/页</option>
                                                            ))}
                                                        </select>
                                                        <button
                                                            className="pagination-btn"
                                                            disabled={previewCurrentPage <= 1}
                                                            onClick={() => setPreviewCurrentPage(1)}
                                                            title="首页"
                                                        >
                                                            ⟪
                                                        </button>
                                                        <button
                                                            className="pagination-btn"
                                                            disabled={previewCurrentPage <= 1}
                                                            onClick={() => setPreviewCurrentPage((p) => Math.max(1, p - 1))}
                                                            title="上一页"
                                                        >
                                                            ‹
                                                        </button>
                                                        <span className="pagination-page-num">
                                                            {previewCurrentPage} / {totalPages}
                                                        </span>
                                                        <button
                                                            className="pagination-btn"
                                                            disabled={previewCurrentPage >= totalPages}
                                                            onClick={() => setPreviewCurrentPage((p) => Math.min(totalPages, p + 1))}
                                                            title="下一页"
                                                        >
                                                            ›
                                                        </button>
                                                        <button
                                                            className="pagination-btn"
                                                            disabled={previewCurrentPage >= totalPages}
                                                            onClick={() => setPreviewCurrentPage(totalPages)}
                                                            title="末页"
                                                        >
                                                            ⟫
                                                        </button>
                                                    </div>
                                                </div>
                                            )}
                                        </>
                                    );
                                })()
                            ) : (
                                <div className="empty-box">
                                    选择数据集后，点击"刷新预览"查看报表展示效果。
                                </div>
                            )}
                        </div>
                    </div>
                ) : null}
            </div>
            {editingStyleRule && (
                <StyleRuleModal
                    columnKey={editingStyleRule.columnKey}
                    rule={editingStyleRule.rule}
                    ruleIndex={editingStyleRule.ruleIndex}
                    reportFormColumns={reportFormColumns}
                    onUpdateColumn={onUpdateColumn}
                    onClose={() => setEditingStyleRule(null)}
                />
            )}
        </FormModal>
    );
}

function StyleRuleModal({
    columnKey,
    rule,
    ruleIndex,
    reportFormColumns,
    onUpdateColumn,
    onClose,
}: {
    columnKey: string;
    rule: ReportColumnStyleRule;
    ruleIndex: number;
    reportFormColumns: ReportColumnConfig[];
    onUpdateColumn: (index: number, patch: Partial<ReportColumnConfig>) => void;
    onClose: () => void;
}) {
    const columnIndex = reportFormColumns.findIndex(c => c.key === columnKey);
    const column = columnIndex !== -1 ? reportFormColumns[columnIndex] : null;
    if (!column) return null;

    const updateRule = (updates: Partial<ReportColumnStyleRule>) => {
        const currentRules = column.style_rules || [];
        const newRules = [...currentRules];
        while (newRules.length <= ruleIndex) {
            newRules.push(createEmptyStyleRule());
        }
        newRules[ruleIndex] = { ...newRules[ruleIndex], ...updates };
        onUpdateColumn(columnIndex, { style_rules: newRules });
    };

    return (
        <FormModal
            open
            title={`样式规则 - ${column.key}`}
            onClose={onClose}
            width="600px"
        >
            <div className="style-rule-editor">
                <div className="style-rule-section">
                    <div className="style-rule-section-title">条件设置</div>
                    <div className="style-rule-row">
                        <div className="style-rule-field">
                            <label>条件字段</label>
                            <input
                                type="text"
                                value={rule.compare_field || ''}
                                onChange={(e) => updateRule({ compare_field: e.target.value })}
                                placeholder="留空则使用当前列"
                            />
                        </div>
                        <div className="style-rule-field">
                            <label>条件</label>
                            <select
                                value={rule.operator}
                                onChange={(e) => updateRule({ operator: e.target.value as typeof rule.operator })}
                            >
                                <option value="eq">等于</option>
                                <option value="ne">不等于</option>
                                <option value="contains">包含</option>
                                <option value="gt">大于</option>
                                <option value="gte">大于等于</option>
                                <option value="lt">小于</option>
                                <option value="lte">小于等于</option>
                                <option value="between">区间</option>
                                <option value="empty">为空</option>
                                <option value="not_empty">不为空</option>
                            </select>
                        </div>
                    </div>
                    {!['empty', 'not_empty'].includes(rule.operator) && (
                        <div className="style-rule-row">
                            <div className="style-rule-field">
                                <label>值</label>
                                <input
                                    type="text"
                                    value={rule.value || ''}
                                    onChange={(e) => updateRule({ value: e.target.value })}
                                    placeholder="条件值"
                                />
                            </div>
                            {rule.operator === 'between' && (
                                <div className="style-rule-field">
                                    <label>结束值</label>
                                    <input
                                        type="text"
                                        value={rule.second_value || ''}
                                        onChange={(e) => updateRule({ second_value: e.target.value })}
                                        placeholder="区间结束值"
                                    />
                                </div>
                            )}
                        </div>
                    )}
                </div>

                <div className="style-rule-section">
                    <div className="style-rule-section-title">文字样式</div>
                    <div className="style-rule-row">
                        <div className="style-rule-field">
                            <label>文字颜色</label>
                            <div className="color-input-wrapper">
                                <input
                                    type="color"
                                    value={rule.text_color || '#000000'}
                                    onChange={(e) => updateRule({ text_color: e.target.value })}
                                />
                                <input
                                    type="text"
                                    value={rule.text_color || ''}
                                    onChange={(e) => updateRule({ text_color: e.target.value })}
                                    placeholder="#000000"
                                />
                            </div>
                        </div>
                        <div className="style-rule-field">
                            <label>字体大小</label>
                            <input
                                type="text"
                                value={rule.font_size || ''}
                                onChange={(e) => updateRule({ font_size: e.target.value })}
                                placeholder="如: 14px, 1.2em"
                            />
                        </div>
                    </div>
                    <div className="style-rule-row">
                        <div className="style-rule-field">
                            <label>字体粗细</label>
                            <select
                                value={rule.font_weight || ''}
                                onChange={(e) => updateRule({ font_weight: e.target.value as typeof rule.font_weight || undefined })}
                            >
                                <option value="">默认</option>
                                <option value="normal">正常</option>
                                <option value="bold">粗体</option>
                                <option value="lighter">细体</option>
                                <option value="100">100</option>
                                <option value="200">200</option>
                                <option value="300">300</option>
                                <option value="400">400</option>
                                <option value="500">500</option>
                                <option value="600">600</option>
                                <option value="700">700</option>
                                <option value="800">800</option>
                                <option value="900">900</option>
                            </select>
                        </div>
                        <div className="style-rule-field">
                            <label>字体样式</label>
                            <select
                                value={rule.font_style || ''}
                                onChange={(e) => updateRule({ font_style: e.target.value as typeof rule.font_style || undefined })}
                            >
                                <option value="">默认</option>
                                <option value="normal">正常</option>
                                <option value="italic">斜体</option>
                                <option value="oblique">倾斜</option>
                            </select>
                        </div>
                    </div>
                    <div className="style-rule-row">
                        <div className="style-rule-field">
                            <label>文字装饰</label>
                            <select
                                value={rule.text_decoration || ''}
                                onChange={(e) => updateRule({ text_decoration: e.target.value as typeof rule.text_decoration || undefined })}
                            >
                                <option value="">无</option>
                                <option value="underline">下划线</option>
                                <option value="line-through">删除线</option>
                                <option value="overline">上划线</option>
                            </select>
                        </div>
                        <div className="style-rule-field">
                            <label>文字对齐</label>
                            <select
                                value={rule.text_align || ''}
                                onChange={(e) => updateRule({ text_align: e.target.value as typeof rule.text_align || undefined })}
                            >
                                <option value="">默认</option>
                                <option value="left">左对齐</option>
                                <option value="center">居中</option>
                                <option value="right">右对齐</option>
                            </select>
                        </div>
                    </div>
                </div>

                <div className="style-rule-section">
                    <div className="style-rule-section-title">背景与边框</div>
                    <div className="style-rule-row">
                        <div className="style-rule-field">
                            <label>背景颜色</label>
                            <div className="color-input-wrapper">
                                <input
                                    type="color"
                                    value={rule.background_color || '#ffffff'}
                                    onChange={(e) => updateRule({ background_color: e.target.value })}
                                />
                                <input
                                    type="text"
                                    value={rule.background_color || ''}
                                    onChange={(e) => updateRule({ background_color: e.target.value })}
                                    placeholder="#ffffff"
                                />
                            </div>
                        </div>
                        <div className="style-rule-field">
                            <label>背景样式</label>
                            <select
                                value={rule.background_mode || 'soft'}
                                onChange={(e) => updateRule({ background_mode: e.target.value as typeof rule.background_mode })}
                            >
                                <option value="solid">实心</option>
                                <option value="soft">柔和</option>
                                <option value="pill">药丸</option>
                                <option value="outline">边框</option>
                            </select>
                        </div>
                    </div>
                    <div className="style-rule-row">
                        <div className="style-rule-field">
                            <label>边框颜色</label>
                            <div className="color-input-wrapper">
                                <input
                                    type="color"
                                    value={rule.border_color || '#cccccc'}
                                    onChange={(e) => updateRule({ border_color: e.target.value })}
                                />
                                <input
                                    type="text"
                                    value={rule.border_color || ''}
                                    onChange={(e) => updateRule({ border_color: e.target.value })}
                                    placeholder="#cccccc"
                                />
                            </div>
                        </div>
                        <div className="style-rule-field">
                            <label>边框宽度</label>
                            <input
                                type="text"
                                value={rule.border_width || ''}
                                onChange={(e) => updateRule({ border_width: e.target.value })}
                                placeholder="如: 1px, 2px"
                            />
                        </div>
                    </div>
                    <div className="style-rule-row">
                        <div className="style-rule-field">
                            <label>圆角大小</label>
                            <input
                                type="text"
                                value={rule.border_radius || ''}
                                onChange={(e) => updateRule({ border_radius: e.target.value })}
                                placeholder="如: 4px, 8px"
                            />
                        </div>
                        <div className="style-rule-field">
                            <label>透明度</label>
                            <div className="opacity-input-wrapper">
                                <input
                                    type="range"
                                    min="0"
                                    max="100"
                                    value={(rule.opacity ?? 1) * 100}
                                    onChange={(e) => updateRule({ opacity: parseInt(e.target.value) / 100 })}
                                />
                                <span>{Math.round((rule.opacity ?? 1) * 100)}%</span>
                            </div>
                        </div>
                    </div>
                </div>

                <div className="style-rule-section">
                    <div className="style-rule-section-title">其他</div>
                    <div className="style-rule-row">
                        <div className="style-rule-field" style={{ flex: 1 }}>
                            <label>图标 (Lucide图标名称)</label>
                            <input
                                type="text"
                                value={rule.icon || ''}
                                onChange={(e) => updateRule({ icon: e.target.value })}
                                placeholder="如: Check, X, Star"
                            />
                        </div>
                    </div>
                </div>

                <div className="style-rule-actions">
                    <button
                        type="button"
                        className="btn-outline"
                        onClick={() => {
                            const newRules = (column.style_rules || []).filter((_, i) => i !== ruleIndex);
                            onUpdateColumn(columnIndex, { style_rules: newRules.length > 0 ? newRules : undefined });
                            onClose();
                        }}
                    >
                        <Trash2 size={14} />
                        删除规则
                    </button>
                </div>
            </div>
        </FormModal>
    );
}

function TableStylePanel({
    tableStyleJson,
    onChange,
}: {
    tableStyleJson: string;
    onChange: (json: string) => void;
}) {
    const style = useMemo<TableStyleConfig>(() => {
        try { return JSON.parse(tableStyleJson || '{}'); } catch { return {}; }
    }, [tableStyleJson]);

    const patch = (partial: Partial<TableStyleConfig>) => {
        onChange(JSON.stringify({ ...style, ...partial }, null, 2));
    };

    return (
        <div className="report-modal-panel table-style-panel">
            {/* 整体 */}
            <div className="report-modal-section">
                <div className="filter-builder-head report-modal-card-head">
                    <div>
                        <div className="filter-builder-title">整体样式</div>
                        <div className="resource-meta">表格边框、字号、行高等基础外观。</div>
                    </div>
                </div>
                <div className="form-grid three compact-form-grid">
                    <label>
                        <span>边框样式</span>
                        <Select
                            value={style.border_style || ''}
                            onChange={(v) => patch({ border_style: (v || undefined) as TableStyleConfig['border_style'] })}
                            options={[
                                { value: '', label: '默认' },
                                { value: 'solid', label: '实线' },
                                { value: 'dashed', label: '虚线' },
                                { value: 'dotted', label: '点线' },
                                { value: 'none', label: '无边框' },
                            ]}
                        />
                    </label>
                    <label>
                        <span>边框颜色</span>
                        <div className="color-input-row">
                            <input type="color" value={style.border_color || '#e2e8f0'} onChange={(e) => patch({ border_color: e.target.value })} />
                            <input type="text" value={style.border_color || ''} onChange={(e) => patch({ border_color: e.target.value || undefined })} placeholder="#e2e8f0" />
                        </div>
                    </label>
                    <label>
                        <span>圆角</span>
                        <input value={style.border_radius || ''} onChange={(e) => patch({ border_radius: e.target.value || undefined })} placeholder="14px" />
                    </label>
                    <label>
                        <span>字号</span>
                        <input value={style.font_size || ''} onChange={(e) => patch({ font_size: e.target.value || undefined })} placeholder="0.84rem" />
                    </label>
                    <label>
                        <span>行高</span>
                        <input value={style.row_height || ''} onChange={(e) => patch({ row_height: e.target.value || undefined })} placeholder="2.2" />
                    </label>
                    <label className="switch-chip">
                        <input type="checkbox" checked={style.striped ?? true} onChange={(e) => patch({ striped: e.target.checked })} />
                        斑马纹
                    </label>
                    <label className="switch-chip">
                        <input type="checkbox" checked={style.show_row_number ?? false} onChange={(e) => patch({ show_row_number: e.target.checked })} />
                        序号列
                    </label>
                </div>
            </div>

            {/* 表头 */}
            <div className="report-modal-section">
                <div className="filter-builder-head report-modal-card-head">
                    <div>
                        <div className="filter-builder-title">表头样式</div>
                        <div className="resource-meta">控制表头区域的背景、文字颜色和字号。</div>
                    </div>
                </div>
                <div className="form-grid three compact-form-grid">
                    <label>
                        <span>背景色</span>
                        <div className="color-input-row">
                            <input type="color" value={style.header_background || '#f8fafc'} onChange={(e) => patch({ header_background: e.target.value })} />
                            <input type="text" value={style.header_background || ''} onChange={(e) => patch({ header_background: e.target.value || undefined })} placeholder="#f8fafc" />
                        </div>
                    </label>
                    <label>
                        <span>文字颜色</span>
                        <div className="color-input-row">
                            <input type="color" value={style.header_color || '#475569'} onChange={(e) => patch({ header_color: e.target.value })} />
                            <input type="text" value={style.header_color || ''} onChange={(e) => patch({ header_color: e.target.value || undefined })} placeholder="#475569" />
                        </div>
                    </label>
                    <label>
                        <span>字号</span>
                        <input value={style.header_font_size || ''} onChange={(e) => patch({ header_font_size: e.target.value || undefined })} placeholder="0.76rem" />
                    </label>
                    <label>
                        <span>字重</span>
                        <Select
                            value={style.header_font_weight || ''}
                            onChange={(v) => patch({ header_font_weight: v || undefined })}
                            options={[
                                { value: '', label: '默认 (700)' },
                                { value: '400', label: '常规 (400)' },
                                { value: '500', label: '中等 (500)' },
                                { value: '600', label: '半粗 (600)' },
                                { value: '700', label: '粗体 (700)' },
                                { value: '800', label: '特粗 (800)' },
                                { value: '900', label: '极粗 (900)' },
                            ]}
                        />
                    </label>
                </div>
            </div>

            {/* 表体 */}
            <div className="report-modal-section">
                <div className="filter-builder-head report-modal-card-head">
                    <div>
                        <div className="filter-builder-title">表体样式</div>
                        <div className="resource-meta">控制数据行区域的背景、文字颜色和字号。</div>
                    </div>
                </div>
                <div className="form-grid three compact-form-grid">
                    <label>
                        <span>背景色</span>
                        <div className="color-input-row">
                            <input type="color" value={style.body_background || '#ffffff'} onChange={(e) => patch({ body_background: e.target.value })} />
                            <input type="text" value={style.body_background || ''} onChange={(e) => patch({ body_background: e.target.value || undefined })} placeholder="#ffffff" />
                        </div>
                    </label>
                    <label>
                        <span>文字颜色</span>
                        <div className="color-input-row">
                            <input type="color" value={style.body_color || '#334155'} onChange={(e) => patch({ body_color: e.target.value })} />
                            <input type="text" value={style.body_color || ''} onChange={(e) => patch({ body_color: e.target.value || undefined })} placeholder="#334155" />
                        </div>
                    </label>
                    <label>
                        <span>字号</span>
                        <input value={style.body_font_size || ''} onChange={(e) => patch({ body_font_size: e.target.value || undefined })} placeholder="0.84rem" />
                    </label>
                </div>
            </div>

            {/* 表尾 */}
            <div className="report-modal-section">
                <div className="filter-builder-head report-modal-card-head">
                    <div>
                        <div className="filter-builder-title">表尾样式</div>
                        <div className="resource-meta">控制底部汇总信息区域的显示和外观。</div>
                    </div>
                </div>
                <div className="form-grid three compact-form-grid">
                    <label className="switch-chip">
                        <input type="checkbox" checked={style.footer_visible ?? true} onChange={(e) => patch({ footer_visible: e.target.checked })} />
                        显示表尾
                    </label>
                    <label>
                        <span>背景色</span>
                        <div className="color-input-row">
                            <input type="color" value={style.footer_background || '#f8fafc'} onChange={(e) => patch({ footer_background: e.target.value })} />
                            <input type="text" value={style.footer_background || ''} onChange={(e) => patch({ footer_background: e.target.value || undefined })} placeholder="#f8fafc" />
                        </div>
                    </label>
                    <label>
                        <span>文字颜色</span>
                        <div className="color-input-row">
                            <input type="color" value={style.footer_color || '#64748b'} onChange={(e) => patch({ footer_color: e.target.value })} />
                            <input type="text" value={style.footer_color || ''} onChange={(e) => patch({ footer_color: e.target.value || undefined })} placeholder="#64748b" />
                        </div>
                    </label>
                </div>
            </div>

            {/* 空态 */}
            <div className="report-modal-section">
                <div className="filter-builder-head report-modal-card-head">
                    <div>
                        <div className="filter-builder-title">空态提示</div>
                        <div className="resource-meta">无数据时显示的提示文案。</div>
                    </div>
                </div>
                <div className="form-grid one compact-form-grid">
                    <label>
                        <span>空数据提示文案</span>
                        <input value={style.empty_text || ''} onChange={(e) => patch({ empty_text: e.target.value || undefined })} placeholder="当前查询没有返回数据。" />
                    </label>
                </div>
            </div>

            {/* 分页 */}
            <div className="report-modal-section">
                <div className="filter-builder-head report-modal-card-head">
                    <div>
                        <div className="filter-builder-title">分页设置</div>
                        <div className="resource-meta">控制表格数据的分页展示方式。</div>
                    </div>
                </div>
                <div className="form-grid three compact-form-grid">
                    <label className="switch-chip">
                        <input type="checkbox" checked={style.pagination_enabled ?? false} onChange={(e) => patch({ pagination_enabled: e.target.checked })} />
                        启用分页
                    </label>
                    <label>
                        <span>每页行数</span>
                        <select
                            value={style.page_size || 20}
                            onChange={(e) => patch({ page_size: Number(e.target.value) })}
                        >
                            {(style.page_size_options || [10, 20, 50, 100]).map((size) => (
                                <option key={size} value={size}>{size} 行/页</option>
                            ))}
                        </select>
                    </label>
                    <label>
                        <span>行数选项</span>
                        <input
                            value={(style.page_size_options || [10, 20, 50, 100]).join(', ')}
                            onChange={(e) => {
                                const parsed = e.target.value.split(/[,，\s]+/).map(Number).filter((n) => n > 0);
                                if (parsed.length > 0) patch({ page_size_options: parsed });
                            }}
                            placeholder="10, 20, 50, 100"
                        />
                    </label>
                </div>
            </div>
        </div>
    );
}
