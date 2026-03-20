import { useEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties, ReactNode } from 'react';
import { ArrowUpDown, ArrowUp, ArrowDown, Settings2, GripVertical, Eye, EyeOff, ChevronUp, ChevronDown, RotateCcw } from 'lucide-react';
import classNames from 'classnames';
import { getMyTableColumnPreference, updateMyTableColumnPreference } from '../../services/api';
import './DataTable.css';

interface Column<T> {
    key: keyof T | string;
    title: ReactNode;
    render?: (value: any, record: T, index: number) => ReactNode;
    width?: number | string;
    fixed?: 'left' | 'right';
    className?: string;
    sortable?: boolean;
    hideable?: boolean;
    reorderable?: boolean;
    displayLabel?: string;
}

interface DataTableProps<T> {
    columns: Column<T>[];
    data: T[];
    title?: ReactNode;
    showHeader?: boolean;
    striped?: boolean;
    hoverable?: boolean;
    loading?: boolean;
    onRowClick?: (record: T) => void;
    tableId?: string;
    enableColumnSettings?: boolean;
    showSerialColumn?: boolean;
    serialStart?: number;
}

interface StoredColumnSettings {
    order: string[];
    hidden: string[];
}

interface ManagedColumn<T> extends Column<T> {
    normalizedKey: string;
    label: string;
    hideableResolved: boolean;
    reorderableResolved: boolean;
    fixedResolved?: 'left' | 'right';
    isSerialColumn: boolean;
    stickyOffset?: number;
}

const TABLE_COLUMN_SETTINGS_PREFIX = 'finflow:table-columns:';
const DEFAULT_COLUMN_SETTINGS: StoredColumnSettings = { order: [], hidden: [] };
const DEFAULT_SELECTION_COLUMN_WIDTH = 48;
const DEFAULT_SERIAL_COLUMN_WIDTH = 72;
const DEFAULT_ACTION_COLUMN_WIDTH = 120;
const SERIAL_COLUMN_KEYS = new Set([
    '_serial',
    '__serial',
    'serial',
    'serialno',
    'serial_no',
    'rowindex',
    'row_index',
    'index',
    '_index',
    'no',
]);

const normalizeStoredSettings = (settings?: Partial<StoredColumnSettings> | null): StoredColumnSettings => ({
    order: Array.isArray(settings?.order) ? Array.from(new Set(settings.order.map(String))) : [],
    hidden: Array.isArray(settings?.hidden) ? Array.from(new Set(settings.hidden.map(String))) : [],
});

const hasStoredSettings = (settings: StoredColumnSettings) => settings.order.length > 0 || settings.hidden.length > 0;

const readLocalSettings = (tableId: string): StoredColumnSettings => {
    if (typeof window === 'undefined') return DEFAULT_COLUMN_SETTINGS;

    try {
        const raw = window.localStorage.getItem(`${TABLE_COLUMN_SETTINGS_PREFIX}${tableId}`);
        if (!raw) {
            return DEFAULT_COLUMN_SETTINGS;
        }

        return normalizeStoredSettings(JSON.parse(raw) as Partial<StoredColumnSettings>);
    } catch {
        return DEFAULT_COLUMN_SETTINGS;
    }
};

const writeLocalSettings = (tableId: string, settings: StoredColumnSettings) => {
    if (typeof window === 'undefined') return;
    window.localStorage.setItem(`${TABLE_COLUMN_SETTINGS_PREFIX}${tableId}`, JSON.stringify(settings));
};

const removeLocalSettings = (tableId: string) => {
    if (typeof window === 'undefined') return;
    window.localStorage.removeItem(`${TABLE_COLUMN_SETTINGS_PREFIX}${tableId}`);
};

const normalizeColumnTitle = (title: ReactNode) => (typeof title === 'string' ? title.trim() : '');

const isSerialColumnDefinition = <T extends Record<string, any>>(column: Column<T>) => {
    const normalizedKey = String(column.key).trim().toLowerCase().replace(/[^a-z0-9_]/g, '');
    const displayLabel = String(column.displayLabel || '').trim();
    const title = normalizeColumnTitle(column.title);
    return SERIAL_COLUMN_KEYS.has(normalizedKey) || displayLabel === '序号' || title === '序号';
};

const resolveStickyWidth = <T extends Record<string, any>>(column: ManagedColumn<T>) => {
    if (typeof column.width === 'number') return column.width;
    if (typeof column.width === 'string') {
        const pxMatch = column.width.trim().match(/^(\d+(?:\.\d+)?)px$/i);
        if (pxMatch) {
            return Number(pxMatch[1]);
        }
    }
    if (column.normalizedKey === '_selection') return DEFAULT_SELECTION_COLUMN_WIDTH;
    if (column.normalizedKey === 'actions') return DEFAULT_ACTION_COLUMN_WIDTH;
    if (column.isSerialColumn) return DEFAULT_SERIAL_COLUMN_WIDTH;
    return 0;
};

const DataTable = <T extends Record<string, any>>({
    columns,
    data,
    title,
    showHeader = true,
    striped = true,
    hoverable = true,
    loading = false,
    onRowClick,
    tableId,
    enableColumnSettings,
    showSerialColumn = true,
    serialStart = 1,
}: DataTableProps<T>) => {
    const [sortConfig, setSortConfig] = useState<{ key: string; direction: 'asc' | 'desc' } | null>(null);
    const [settingsOpen, setSettingsOpen] = useState(false);
    const [storedSettings, setStoredSettings] = useState<StoredColumnSettings>({ order: [], hidden: [] });
    const [draggingColumnKey, setDraggingColumnKey] = useState<string | null>(null);
    const [dragOverColumnKey, setDragOverColumnKey] = useState<string | null>(null);
    const settingsRef = useRef<HTMLDivElement>(null);
    const hasLocalSettingsChangeRef = useRef(false);
    const settingsEnabled = (enableColumnSettings ?? Boolean(tableId)) && Boolean(tableId);

    const normalizedColumns = useMemo<Column<T>[]>(() => {
        const hasSerialColumn = columns.some((column) => isSerialColumnDefinition(column));
        if (!showSerialColumn || hasSerialColumn) {
            return columns;
        }

        const serialColumn: Column<T> = {
            key: '_serial',
            title: '序号',
            width: DEFAULT_SERIAL_COLUMN_WIDTH,
            fixed: 'left',
            sortable: false,
            hideable: false,
            reorderable: false,
            render: (_value: any, _record: T, index: number) => serialStart + index,
        };
        const nextColumns = [...columns];
        const selectionIndex = nextColumns.findIndex((column) => String(column.key) === '_selection');
        const insertIndex = selectionIndex >= 0 ? selectionIndex + 1 : 0;
        nextColumns.splice(insertIndex, 0, serialColumn);
        return nextColumns;
    }, [columns, serialStart, showSerialColumn]);

    const managedColumns = useMemo<ManagedColumn<T>[]>(() => normalizedColumns.map((column) => {
        const normalizedKey = String(column.key);
        const isSelectionColumn = normalizedKey === '_selection';
        const isActionColumn = normalizedKey === 'actions';
        const isSerialColumn = isSerialColumnDefinition(column);
        const isLocked = isSelectionColumn || isActionColumn || isSerialColumn;
        const fixedResolved = isSelectionColumn || isSerialColumn
            ? 'left'
            : isActionColumn
                ? 'right'
                : column.fixed;

        return {
            ...column,
            normalizedKey,
            label: column.displayLabel || (typeof column.title === 'string' ? column.title : normalizedKey),
            sortable: column.sortable ?? !isLocked,
            hideableResolved: column.hideable ?? !isLocked,
            reorderableResolved: column.reorderable ?? !(isLocked || Boolean(column.fixed)),
            fixedResolved,
            isSerialColumn,
        };
    }), [normalizedColumns]);

    const configurableColumns = useMemo(
        () => managedColumns.filter((column) => column.hideableResolved || column.reorderableResolved),
        [managedColumns]
    );

    useEffect(() => {
        if (!settingsEnabled || !tableId || typeof window === 'undefined') {
            setStoredSettings(DEFAULT_COLUMN_SETTINGS);
            return;
        }

        const localSettings = readLocalSettings(tableId);
        hasLocalSettingsChangeRef.current = false;
        setStoredSettings(localSettings);

        const token = window.localStorage.getItem('token');
        if (!token) {
            return;
        }

        let cancelled = false;

        void getMyTableColumnPreference(tableId)
            .then((serverPreference) => {
                if (cancelled) return;
                if (hasLocalSettingsChangeRef.current) return;

                const serverSettings = normalizeStoredSettings(serverPreference);
                const hasPersistedServerSettings = Boolean(serverPreference.updated_at);

                if (hasPersistedServerSettings) {
                    setStoredSettings(serverSettings);
                    writeLocalSettings(tableId, serverSettings);
                    return;
                }

                if (hasStoredSettings(localSettings)) {
                    void updateMyTableColumnPreference(tableId, localSettings).catch(() => {
                        console.warn(`Failed to migrate local table settings for ${tableId}`);
                    });
                }
            })
            .catch(() => {
                if (!cancelled) {
                    setStoredSettings(localSettings);
                }
            });

        return () => {
            cancelled = true;
        };
    }, [settingsEnabled, tableId]);

    useEffect(() => {
        if (!settingsOpen) return;

        const handleOutsideClick = (event: MouseEvent) => {
            if (!settingsRef.current?.contains(event.target as Node)) {
                setSettingsOpen(false);
            }
        };

        document.addEventListener('mousedown', handleOutsideClick);
        return () => document.removeEventListener('mousedown', handleOutsideClick);
    }, [settingsOpen]);

    const persistSettings = (nextSettings: StoredColumnSettings) => {
        const normalizedSettings = normalizeStoredSettings(nextSettings);
        hasLocalSettingsChangeRef.current = true;
        setStoredSettings(normalizedSettings);
        if (!settingsEnabled || !tableId || typeof window === 'undefined') return;

        writeLocalSettings(tableId, normalizedSettings);

        const token = window.localStorage.getItem('token');
        if (!token) return;

        void updateMyTableColumnPreference(tableId, normalizedSettings).catch(() => {
            console.warn(`Failed to save table settings for ${tableId}`);
        });
    };

    const resetColumnSettings = () => {
        const nextSettings = DEFAULT_COLUMN_SETTINGS;
        hasLocalSettingsChangeRef.current = true;
        setStoredSettings(nextSettings);
        if (!settingsEnabled || !tableId || typeof window === 'undefined') return;

        removeLocalSettings(tableId);

        const token = window.localStorage.getItem('token');
        if (!token) return;

        void updateMyTableColumnPreference(tableId, nextSettings).catch(() => {
            console.warn(`Failed to reset table settings for ${tableId}`);
        });
    };

    const handleToggleColumn = (columnKey: string) => {
        const hidden = new Set(storedSettings.hidden);
        if (hidden.has(columnKey)) {
            hidden.delete(columnKey);
        } else {
            hidden.add(columnKey);
        }
        persistSettings({ ...storedSettings, hidden: Array.from(hidden) });
    };

    const handleMoveColumn = (columnKey: string, direction: 'up' | 'down') => {
        const reorderableKeys = managedColumns
            .filter((column) => column.reorderableResolved)
            .map((column) => column.normalizedKey);
        const currentOrder = storedSettings.order.length
            ? storedSettings.order.filter((key) => reorderableKeys.includes(key))
            : reorderableKeys;
        const completeOrder = [
            ...currentOrder,
            ...reorderableKeys.filter((key) => !currentOrder.includes(key)),
        ];
        const index = completeOrder.indexOf(columnKey);
        if (index === -1) return;

        const targetIndex = direction === 'up' ? index - 1 : index + 1;
        if (targetIndex < 0 || targetIndex >= completeOrder.length) return;

        const nextOrder = [...completeOrder];
        [nextOrder[index], nextOrder[targetIndex]] = [nextOrder[targetIndex], nextOrder[index]];
        persistSettings({ ...storedSettings, order: nextOrder });
    };

    const handleReorderColumn = (sourceKey: string, targetKey: string) => {
        if (sourceKey === targetKey) return;

        const reorderableKeys = managedColumns
            .filter((column) => column.reorderableResolved)
            .map((column) => column.normalizedKey);
        const currentOrder = storedSettings.order.length
            ? storedSettings.order.filter((key) => reorderableKeys.includes(key))
            : reorderableKeys;
        const completeOrder = [
            ...currentOrder,
            ...reorderableKeys.filter((key) => !currentOrder.includes(key)),
        ];
        const sourceIndex = completeOrder.indexOf(sourceKey);
        const targetIndex = completeOrder.indexOf(targetKey);

        if (sourceIndex === -1 || targetIndex === -1) return;

        const nextOrder = [...completeOrder];
        const [movedKey] = nextOrder.splice(sourceIndex, 1);
        const insertIndex = sourceIndex < targetIndex ? targetIndex : targetIndex;
        nextOrder.splice(insertIndex, 0, movedKey);
        persistSettings({ ...storedSettings, order: nextOrder });
    };

    const handleDragStart = (columnKey: string) => {
        setDraggingColumnKey(columnKey);
        setDragOverColumnKey(columnKey);
    };

    const handleDragEnter = (columnKey: string) => {
        if (!draggingColumnKey || draggingColumnKey === columnKey) return;
        setDragOverColumnKey(columnKey);
    };

    const handleDragEnd = () => {
        setDraggingColumnKey(null);
        setDragOverColumnKey(null);
    };

    const handleDropColumn = (targetKey: string) => {
        if (!draggingColumnKey) return;
        handleReorderColumn(draggingColumnKey, targetKey);
        setDraggingColumnKey(null);
        setDragOverColumnKey(null);
    };

    const effectiveColumns = useMemo(() => {
        const leftColumns = managedColumns.filter((column) => column.fixedResolved === 'left');
        const rightColumns = managedColumns.filter((column) => column.fixedResolved === 'right');
        const middleColumns = managedColumns.filter((column) => !leftColumns.includes(column) && !rightColumns.includes(column));

        const reorderableKeys = middleColumns
            .filter((column) => column.reorderableResolved)
            .map((column) => column.normalizedKey);
        const savedOrder = storedSettings.order.filter((key) => reorderableKeys.includes(key));
        const finalOrder = [
            ...savedOrder,
            ...reorderableKeys.filter((key) => !savedOrder.includes(key)),
        ];
        const orderMap = new Map(finalOrder.map((key, index) => [key, index]));
        const hiddenKeys = new Set(storedSettings.hidden);

        const orderedMiddleColumns = [...middleColumns].sort((a, b) => {
            const aOrder = orderMap.get(a.normalizedKey);
            const bOrder = orderMap.get(b.normalizedKey);
            if (aOrder == null && bOrder == null) return 0;
            if (aOrder == null) return 1;
            if (bOrder == null) return -1;
            return aOrder - bOrder;
        });

        const visibleMiddleColumns = orderedMiddleColumns.filter((column) => !column.hideableResolved || !hiddenKeys.has(column.normalizedKey));
        const orderedColumns = [...leftColumns, ...visibleMiddleColumns, ...rightColumns];
        let leftOffset = 0;
        let rightOffset = 0;

        const stickyRightOffsets = new Map<string, number>();
        [...rightColumns].reverse().forEach((column) => {
            stickyRightOffsets.set(column.normalizedKey, rightOffset);
            rightOffset += resolveStickyWidth(column);
        });

        return orderedColumns.map((column) => {
            if (column.fixedResolved === 'left') {
                const stickyOffset = leftOffset;
                leftOffset += resolveStickyWidth(column);
                return { ...column, stickyOffset };
            }
            if (column.fixedResolved === 'right') {
                return { ...column, stickyOffset: stickyRightOffsets.get(column.normalizedKey) ?? 0 };
            }
            return { ...column, stickyOffset: undefined };
        });
    }, [managedColumns, storedSettings.hidden, storedSettings.order]);

    const orderedConfigurableColumns = useMemo(() => {
        const reorderableKeys = configurableColumns
            .filter((column) => column.reorderableResolved)
            .map((column) => column.normalizedKey);
        const savedOrder = storedSettings.order.filter((key) => reorderableKeys.includes(key));
        const finalOrder = [
            ...savedOrder,
            ...reorderableKeys.filter((key) => !savedOrder.includes(key)),
        ];
        const orderMap = new Map(finalOrder.map((key, index) => [key, index]));

        return [...configurableColumns].sort((a, b) => {
            const aOrder = orderMap.get(a.normalizedKey);
            const bOrder = orderMap.get(b.normalizedKey);
            if (aOrder == null && bOrder == null) return 0;
            if (aOrder == null) return 1;
            if (bOrder == null) return -1;
            return aOrder - bOrder;
        });
    }, [configurableColumns, storedSettings.order]);

    const handleSort = (key: string) => {
        let direction: 'asc' | 'desc' = 'asc';
        if (sortConfig && sortConfig.key === key && sortConfig.direction === 'asc') {
            direction = 'desc';
        }
        setSortConfig({ key, direction });
    };

    const sortedData = useMemo(() => {
        if (!sortConfig) return data;

        return [...data].sort((a, b) => {
            const aValue = a[sortConfig.key as keyof T];
            const bValue = b[sortConfig.key as keyof T];
            if (aValue < bValue) {
                return sortConfig.direction === 'asc' ? -1 : 1;
            }
            if (aValue > bValue) {
                return sortConfig.direction === 'asc' ? 1 : -1;
            }
            return 0;
        });
    }, [data, sortConfig]);

    const getSortIcon = (column: ManagedColumn<T>) => {
        if (column.sortable === false) return null;
        if (!sortConfig || sortConfig.key !== column.normalizedKey) {
            return <ArrowUpDown size={14} className="sort-icon inactive" />;
        }
        return sortConfig.direction === 'asc'
            ? <ArrowUp size={14} className="sort-icon active" />
            : <ArrowDown size={14} className="sort-icon active" />;
    };

    const getStickyStyle = (column: ManagedColumn<T>) => {
        const style: CSSProperties = {};
        if (column.width != null) {
            style.width = column.width;
        }
        if (column.fixedResolved === 'left') {
            style.left = `${column.stickyOffset ?? 0}px`;
        }
        if (column.fixedResolved === 'right') {
            style.right = `${column.stickyOffset ?? 0}px`;
        }
        return style;
    };

    if (loading) {
        return <div className="table-loading">Loading...</div>;
    }

    return (
        <div className="data-table-wrapper glass">
            {(title || (settingsEnabled && configurableColumns.length > 0)) && (
                <div className="table-toolbar">
                    {title ? <div className="table-header"><h3>{title}</h3></div> : <div />}
                    {settingsEnabled && configurableColumns.length > 0 && (
                        <div className="table-settings" ref={settingsRef}>
                            <button
                                type="button"
                                className={classNames('table-settings-btn', { active: settingsOpen })}
                                onClick={() => setSettingsOpen((open) => !open)}
                            >
                                <Settings2 size={15} />
                                <span>列设置</span>
                            </button>
                            {settingsOpen && (
                                <div className="table-settings-panel">
                                    <div className="table-settings-panel-header">
                                        <div>
                                            <strong>表格列配置</strong>
                                            <div className="table-settings-subtitle">勾选控制显示，使用上下按钮调整顺序</div>
                                        </div>
                                        <button type="button" className="table-settings-reset" onClick={resetColumnSettings}>
                                            <RotateCcw size={14} />
                                            <span>重置</span>
                                        </button>
                                    </div>
                                    <div className="table-settings-list">
                                        {orderedConfigurableColumns.map((column, index) => {
                                            const isHidden = storedSettings.hidden.includes(column.normalizedKey);
                                            const canMoveUp = column.reorderableResolved && index > 0;
                                            const canMoveDown = column.reorderableResolved && index < orderedConfigurableColumns.length - 1;

                                            return (
                                                <div
                                                    key={column.normalizedKey}
                                                    className={classNames('table-settings-item', {
                                                        'is-dragging': draggingColumnKey === column.normalizedKey,
                                                        'is-drag-over': dragOverColumnKey === column.normalizedKey && draggingColumnKey !== column.normalizedKey,
                                                        'is-reorderable': column.reorderableResolved,
                                                    })}
                                                    onDragOver={(event) => {
                                                        if (!column.reorderableResolved || !draggingColumnKey) return;
                                                        event.preventDefault();
                                                        event.dataTransfer.dropEffect = 'move';
                                                    }}
                                                    onDragEnter={() => column.reorderableResolved && handleDragEnter(column.normalizedKey)}
                                                    onDrop={(event) => {
                                                        if (!column.reorderableResolved) return;
                                                        event.preventDefault();
                                                        handleDropColumn(column.normalizedKey);
                                                    }}
                                                >
                                                    <button
                                                        type="button"
                                                        className={classNames('table-settings-visibility', { active: !isHidden, disabled: !column.hideableResolved })}
                                                        onClick={() => column.hideableResolved && handleToggleColumn(column.normalizedKey)}
                                                        disabled={!column.hideableResolved}
                                                        title={column.hideableResolved ? (isHidden ? '显示列' : '隐藏列') : '该列不可隐藏'}
                                                    >
                                                        {isHidden ? <EyeOff size={14} /> : <Eye size={14} />}
                                                    </button>
                                                    <div
                                                        className={classNames('table-settings-item-label', { draggable: column.reorderableResolved })}
                                                        draggable={column.reorderableResolved}
                                                        onDragStart={() => column.reorderableResolved && handleDragStart(column.normalizedKey)}
                                                        onDragEnd={handleDragEnd}
                                                        title={column.reorderableResolved ? '拖动调整顺序' : undefined}
                                                    >
                                                        <GripVertical size={14} className="table-settings-grip" />
                                                        <span>{column.label}</span>
                                                    </div>
                                                    <div className="table-settings-order">
                                                        <button
                                                            type="button"
                                                            className="table-settings-order-btn"
                                                            onClick={() => handleMoveColumn(column.normalizedKey, 'up')}
                                                            disabled={!canMoveUp}
                                                            title="上移"
                                                        >
                                                            <ChevronUp size={14} />
                                                        </button>
                                                        <button
                                                            type="button"
                                                            className="table-settings-order-btn"
                                                            onClick={() => handleMoveColumn(column.normalizedKey, 'down')}
                                                            disabled={!canMoveDown}
                                                            title="下移"
                                                        >
                                                            <ChevronDown size={14} />
                                                        </button>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}
            <div className="table-container">
                <table className={classNames('modern-table', { 'table-striped': striped, 'table-hover': hoverable })}>
                    {showHeader && (
                        <thead>
                            <tr>
                                {effectiveColumns.map((column) => (
                                    <th
                                        key={column.normalizedKey}
                                        onClick={() => column.sortable !== false && handleSort(column.normalizedKey)}
                                        style={getStickyStyle(column)}
                                        className={classNames(
                                            'sortable-header',
                                            column.className,
                                            {
                                                'is-not-sortable': column.sortable === false,
                                                'dt-sticky-left': column.fixedResolved === 'left',
                                                'dt-sticky-right': column.fixedResolved === 'right',
                                            }
                                        )}
                                    >
                                        <div className="th-content">
                                            {column.title}
                                            {getSortIcon(column)}
                                        </div>
                                    </th>
                                ))}
                            </tr>
                        </thead>
                    )}
                    <tbody>
                        {sortedData.length > 0 ? (
                            sortedData.map((row, rowIndex) => (
                                <tr
                                    key={rowIndex}
                                    onClick={() => onRowClick && onRowClick(row)}
                                    className={classNames({ 'clickable-row': !!onRowClick })}
                                >
                                    {effectiveColumns.map((column) => (
                                        <td
                                            key={column.normalizedKey}
                                            style={getStickyStyle(column)}
                                            className={classNames(
                                                column.className,
                                                {
                                                    'dt-sticky-left': column.fixedResolved === 'left',
                                                    'dt-sticky-right': column.fixedResolved === 'right',
                                                }
                                            )}
                                        >
                                            {column.render
                                                ? column.render(row[column.key as keyof T], row, rowIndex)
                                                : String(row[column.key as keyof T] ?? '-')
                                            }
                                        </td>
                                    ))}
                                </tr>
                            ))
                        ) : (
                            <tr>
                                <td colSpan={effectiveColumns.length || 1} className="empty-state">No Data Available</td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default DataTable;
