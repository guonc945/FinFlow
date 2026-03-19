type VariableLike = {
    key: string;
    description?: string | null;
};

const VARIABLE_DESCRIPTION_OVERRIDES: Record<string, string> = {
    ARCHIVE_TYPE_REGISTRY: '归档类型注册表，用于维护归档接口管理中的归档类型清单',
    ACCOUNTING_SUBJECT_CONFIG: '会计科目同步配置，用于维护会计科目归档与同步所需的接口参数',
};

export const getLocalizedVariableDescription = (key: string, description?: string | null) => {
    return VARIABLE_DESCRIPTION_OVERRIDES[key] ?? description ?? '';
};

export const localizeVariableItem = <T extends VariableLike>(item: T): T => {
    return {
        ...item,
        description: getLocalizedVariableDescription(item.key, item.description),
    } as T;
};
