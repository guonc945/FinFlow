import type { VoucherFieldModule, VoucherSourceFieldOption } from '../../types';

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

export const getUnifiedSourceFieldLabel = (
    field: Pick<VoucherSourceFieldOption, 'label' | 'value'> | null | undefined
) => {
    const rawValue = String(field?.value || '').trim();
    const rawLabel = String(field?.label || rawValue).trim() || rawValue;
    if (!rawValue) return rawLabel;

    const suffixPattern = new RegExp(`\\s*\\(${escapeRegExp(rawValue)}\\)$`);
    return rawLabel.replace(suffixPattern, '').trim() || rawLabel;
};

export const normalizeVoucherSourceFieldOption = <T extends VoucherSourceFieldOption>(field: T): T => ({
    ...field,
    label: getUnifiedSourceFieldLabel(field),
});

export const getSourceFieldDisplayCode = (
    field: Pick<VoucherSourceFieldOption, 'value'> | null | undefined
) => String(field?.value || '').trim();

export const getSourceFieldDisplayText = (
    field: Pick<VoucherSourceFieldOption, 'label' | 'value'> | null | undefined
) => {
    const label = getUnifiedSourceFieldLabel(field);
    const code = getSourceFieldDisplayCode(field);
    if (!code || label === code) return label;
    return `${label} (${code})`;
};

export const normalizeVoucherFieldModules = (modules: VoucherFieldModule[] = []) => (
    modules.map(module => ({
        ...module,
        sources: (module.sources || []).map(source => ({
            ...source,
            fields: (source.fields || []).map(field => normalizeVoucherSourceFieldOption(field)),
        })),
    }))
);
