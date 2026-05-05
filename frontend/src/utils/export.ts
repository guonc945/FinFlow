export const formatCsvCell = (value: unknown): string => {
    if (value === null || value === undefined) return '';
    if (typeof value === 'number') return String(value);
    if (typeof value === 'boolean') return value ? '是' : '否';
    return String(value);
};

export const buildCsvContent = (headers: string[], rows: Record<string, unknown>[]) => {
    const escapeCsvValue = (value: unknown) => {
        const text = formatCsvCell(value);
        return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
    };

    return [
        headers.map((header) => escapeCsvValue(header)).join(','),
        ...rows.map((row) => headers.map((header) => escapeCsvValue(row[header])).join(',')),
    ].join('\r\n');
};

export const downloadCsv = (csvContent: string, filename: string) => {
    const blob = new Blob([`\ufeff${csvContent}`], { type: 'text/csv;charset=utf-8;' });
    const downloadUrl = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => {
        URL.revokeObjectURL(downloadUrl);
    }, 1000);
};

export const exportTableToCsv = (
    columns: Array<{ key: string; title: string | React.ReactNode }>,
    data: Record<string, unknown>[],
    filename: string
) => {
    if (!data.length) {
        alert('当前没有可导出的数据');
        return;
    }

    const exportColumns = columns.filter((col) => !String(col.key).startsWith('_'));
    const headers = exportColumns.map((col) => String(col.title));
    const keys = exportColumns.map((col) => String(col.key));

    const rows = data.map((row) => {
        const result: Record<string, unknown> = {};
        keys.forEach((key, index) => {
            result[headers[index]] = row[key] ?? '';
        });
        return result;
    });

    const csvContent = buildCsvContent(headers, rows);
    const timestamp = new Date().toISOString().slice(0, 10);
    downloadCsv(csvContent, `${filename}_${timestamp}.csv`);
};
