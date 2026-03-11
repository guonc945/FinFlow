import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react';
import classNames from 'classnames';
import './DataTable.css';

interface Column<T> {
    key: keyof T | string;
    title: ReactNode;
    render?: (value: any, record: T, index: number) => ReactNode;
    width?: number | string;
    fixed?: 'left' | 'right';
    className?: string;
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
}

const DataTable = <T extends Record<string, any>>({
    columns,
    data,
    title,
    showHeader = true,
    striped = true,
    hoverable = true,
    loading = false,
    onRowClick
}: DataTableProps<T>) => {
    const [sortConfig, setSortConfig] = useState<{ key: keyof T; direction: 'asc' | 'desc' } | null>(null);

    const handleSort = (key: keyof T) => {
        let direction: 'asc' | 'desc' = 'asc';
        if (sortConfig && sortConfig.key === key && sortConfig.direction === 'asc') {
            direction = 'desc';
        }
        setSortConfig({ key, direction });
    };

    const sortedData = useMemo(() => {
        if (!sortConfig) return data;

        return [...data].sort((a, b) => {
            if (a[sortConfig.key] < b[sortConfig.key]) {
                return sortConfig.direction === 'asc' ? -1 : 1;
            }
            if (a[sortConfig.key] > b[sortConfig.key]) {
                return sortConfig.direction === 'asc' ? 1 : -1;
            }
            return 0;
        });
    }, [data, sortConfig]);

    const getSortIcon = (columnKey: keyof T) => {
        if (!sortConfig || sortConfig.key !== columnKey) {
            return <ArrowUpDown size={14} className="sort-icon inactive" />;
        }
        return sortConfig.direction === 'asc'
            ? <ArrowUp size={14} className="sort-icon active" />
            : <ArrowDown size={14} className="sort-icon active" />;
    };

    if (loading) {
        return <div className="table-loading">Loading...</div>;
    }

    return (
        <div className="data-table-wrapper glass">
            {title && <div className="table-header"><h3>{title}</h3></div>}
            <div className="table-container">
                <table className={classNames('modern-table', { 'table-striped': striped, 'table-hover': hoverable })}>
                    {showHeader && (
                        <thead>
                            <tr>
                                {columns.map((column) => (
                                    <th
                                        key={String(column.key)}
                                        onClick={() => handleSort(column.key)}
                                        style={{ width: column.width }}
                                        className={classNames(
                                            'sortable-header',
                                            column.className,
                                            {
                                                'dt-sticky-left': column.fixed === 'left',
                                                'dt-sticky-right': column.fixed === 'right',
                                            }
                                        )}
                                    >
                                        <div className="th-content">
                                            {column.title}
                                            {getSortIcon(column.key)}
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
                                    {columns.map((column) => (
                                        <td
                                            key={String(column.key)}
                                            className={classNames(
                                                column.className,
                                                {
                                                    'dt-sticky-left': column.fixed === 'left',
                                                    'dt-sticky-right': column.fixed === 'right',
                                                }
                                            )}
                                        >
                                            {column.render
                                                ? column.render(row[column.key], row, rowIndex)
                                                : String(row[column.key] || '-')
                                            }
                                        </td>
                                    ))}
                                </tr>
                            ))
                        ) : (
                            <tr>
                                <td colSpan={columns.length} className="empty-state">No Data Available</td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default DataTable;
