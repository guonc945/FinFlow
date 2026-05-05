import { useEffect, useMemo, useState } from 'react';
import { RefreshCw, Save, ShieldCheck } from 'lucide-react';
import { ToastContainer, useToast } from '../../components/Toast';
import { getMenuPermissions, updateMenuPermissions } from '../../services/api';
import type {
    ApiPermissionItem,
    MenuPermissionMenuItem,
    MenuPermissionOverview,
    MenuPermissionRoleState,
} from '../../types';
import './MenuPermissions.css';

type PermissionItem = MenuPermissionMenuItem | ApiPermissionItem;

type PermissionGroup = {
    group: string;
    items: PermissionItem[];
};

type PermissionSection = {
    section: string;
    groups: PermissionGroup[];
};

type ApiErrorLike = {
    response?: {
        data?: {
            detail?: string;
        };
    };
};

const normalizeMenuPermissionOverview = (payload: unknown): MenuPermissionOverview => {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        return {
            menus: [],
            apis: [],
            roles: [],
        };
    }

    const candidate = payload as Partial<MenuPermissionOverview>;
    return {
        menus: Array.isArray(candidate.menus) ? candidate.menus : [],
        apis: Array.isArray(candidate.apis) ? candidate.apis : [],
        roles: Array.isArray(candidate.roles) ? candidate.roles : [],
    };
};

const buildSections = (items: PermissionItem[]): PermissionSection[] => {
    const sectionMap = new Map<string, Map<string, PermissionItem[]>>();

    items.forEach((item) => {
        const sectionName = item.section || '未分组';
        const groupName = item.group || '其他';
        if (!sectionMap.has(sectionName)) {
            sectionMap.set(sectionName, new Map<string, PermissionItem[]>());
        }

        const groupMap = sectionMap.get(sectionName)!;
        if (!groupMap.has(groupName)) {
            groupMap.set(groupName, []);
        }
        groupMap.get(groupName)!.push(item);
    });

    return Array.from(sectionMap.entries()).map(([section, groupMap]) => ({
        section,
        groups: Array.from(groupMap.entries()).map(([group, groupItems]) => ({
            group,
            items: groupItems,
        })),
    }));
};

const hasSameKeys = (left: string[], right: string[]) => {
    if (left.length !== right.length) {
        return false;
    }
    return left.every((item) => right.includes(item));
};

const PermissionSectionPanel = ({
    title,
    description,
    sections,
    selectedKeys,
    editable,
    onToggle,
    onSelectAll,
    onReset,
    loading,
    showRequiredTag,
}: {
    title: string;
    description: string;
    sections: PermissionSection[];
    selectedKeys: string[];
    editable: boolean;
    onToggle: (key: string) => void;
    onSelectAll: () => void;
    onReset: () => void;
    loading: boolean;
    showRequiredTag?: boolean;
}) => {
    const selectedKeySet = useMemo(() => new Set(selectedKeys), [selectedKeys]);

    return (
        <div className="permission-panel">
            <div className="panel-header">
                <div>
                    <h2>{title}</h2>
                    <p>{description}</p>
                </div>
                <div className="permission-toolbar">
                    <button
                        type="button"
                        className="menu-btn menu-btn-secondary"
                        onClick={onSelectAll}
                        disabled={!editable || loading}
                    >
                        全选
                    </button>
                    <button
                        type="button"
                        className="menu-btn menu-btn-secondary"
                        onClick={onReset}
                        disabled={loading}
                    >
                        重置
                    </button>
                </div>
            </div>

            {loading ? (
                <div className="menu-permissions-empty">权限数据加载中...</div>
            ) : sections.length === 0 ? (
                <div className="menu-permissions-empty">当前角色没有可配置的权限。</div>
            ) : (
                <div className="section-list">
                    {sections.map((section) => (
                        <div key={section.section} className="section-card">
                            <div className="section-card-header">
                                <h3>{section.section}</h3>
                                <span>{section.groups.reduce((sum, group) => sum + group.items.length, 0)} 项</span>
                            </div>

                            {section.groups.map((group) => (
                                <div key={`${section.section}-${group.group}`} className="menu-group">
                                    <div className="menu-group-title">{group.group}</div>
                                    <div className="menu-item-list">
                                        {group.items.map((item) => {
                                            const checked = selectedKeySet.has(item.key);
                                            const required = showRequiredTag && 'required' in item ? item.required : false;
                                            const disabled = !editable || required;

                                            return (
                                                <label
                                                    key={item.key}
                                                    className={`menu-item-card ${checked ? 'checked' : ''} ${disabled ? 'disabled' : ''}`}
                                                >
                                                    <input
                                                        type="checkbox"
                                                        checked={checked}
                                                        disabled={disabled}
                                                        onChange={() => onToggle(item.key)}
                                                    />
                                                    <div className="menu-item-body">
                                                        <div className="menu-item-top">
                                                            <strong>{item.label}</strong>
                                                            <div className="menu-item-tags">
                                                                {required && <span className="tag fixed">必选</span>}
                                                                {item.default_enabled && <span className="tag fixed">默认</span>}
                                                                {item.admin_only && <span className="tag admin">仅管理员</span>}
                                                            </div>
                                                        </div>
                                                        <code>{item.key}</code>
                                                        {item.description ? <p>{item.description}</p> : null}
                                                    </div>
                                                </label>
                                            );
                                        })}
                                    </div>
                                </div>
                            ))}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

const MenuPermissionsPage = () => {
    const { toasts, showToast, removeToast } = useToast();
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [overview, setOverview] = useState<MenuPermissionOverview | null>(null);
    const [selectedRole, setSelectedRole] = useState('user');
    const [draftMenuKeys, setDraftMenuKeys] = useState<string[]>([]);
    const [draftApiKeys, setDraftApiKeys] = useState<string[]>([]);

    const fetchData = async () => {
        setLoading(true);
        try {
            const data = normalizeMenuPermissionOverview(await getMenuPermissions());
            setOverview(data);
            setSelectedRole((current) => {
                if (data.roles.some((role) => role.role === current)) {
                    return current;
                }
                return data.roles.find((role) => role.editable)?.role || data.roles[0]?.role || 'user';
            });
        } catch (error: unknown) {
            const apiError = error as ApiErrorLike;
            console.error(error);
            showToast('error', '加载失败', apiError.response?.data?.detail || '无法获取权限配置');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void fetchData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const selectedRoleState = useMemo<MenuPermissionRoleState | null>(
        () => overview?.roles.find((role) => role.role === selectedRole) || null,
        [overview, selectedRole]
    );

    useEffect(() => {
        if (!selectedRoleState) {
            setDraftMenuKeys([]);
            setDraftApiKeys([]);
            return;
        }
        setDraftMenuKeys(selectedRoleState.menu_keys || []);
        setDraftApiKeys(selectedRoleState.api_keys || []);
    }, [selectedRoleState]);

    const visibleMenus = useMemo(() => {
        const menus = overview?.menus || [];
        if (selectedRoleState?.role === 'admin') {
            return menus;
        }
        return menus.filter((item) => !item.admin_only);
    }, [overview, selectedRoleState]);

    const visibleApis = useMemo(() => {
        const apis = overview?.apis || [];
        if (selectedRoleState?.role === 'admin') {
            return apis;
        }
        return apis.filter((item) => !item.admin_only);
    }, [overview, selectedRoleState]);

    const menuSections = useMemo(() => buildSections(visibleMenus), [visibleMenus]);
    const apiSections = useMemo(() => buildSections(visibleApis), [visibleApis]);

    const hasChanges = useMemo(() => {
        const sourceMenuKeys = selectedRoleState?.menu_keys || [];
        const sourceApiKeys = selectedRoleState?.api_keys || [];
        return !hasSameKeys(sourceMenuKeys, draftMenuKeys) || !hasSameKeys(sourceApiKeys, draftApiKeys);
    }, [draftApiKeys, draftMenuKeys, selectedRoleState]);

    const toggleMenuKey = (menuKey: string) => {
        if (!selectedRoleState?.editable) {
            return;
        }

        const targetMenu = visibleMenus.find((item) => item.key === menuKey);
        if (!targetMenu || targetMenu.required) {
            return;
        }

        setDraftMenuKeys((current) => {
            const next = new Set(current);
            if (next.has(menuKey)) {
                next.delete(menuKey);
            } else {
                next.add(menuKey);
            }
            return visibleMenus
                .filter((item) => next.has(item.key) || item.required)
                .map((item) => item.key);
        });
    };

    const toggleApiKey = (apiKey: string) => {
        if (!selectedRoleState?.editable) {
            return;
        }

        setDraftApiKeys((current) => {
            const next = new Set(current);
            if (next.has(apiKey)) {
                next.delete(apiKey);
            } else {
                next.add(apiKey);
            }
            return visibleApis.filter((item) => next.has(item.key)).map((item) => item.key);
        });
    };

    const handleSave = async () => {
        if (!selectedRoleState?.editable) {
            return;
        }

        setSaving(true);
        try {
            const saved = await updateMenuPermissions(selectedRoleState.role, {
                menu_keys: draftMenuKeys,
                api_keys: draftApiKeys,
            });
            setOverview((current) => {
                if (!current) {
                    return current;
                }
                return {
                    ...current,
                    roles: current.roles.map((role) => (role.role === saved.role ? saved : role)),
                };
            });
            showToast('success', '保存成功', `${selectedRoleState.label} 的菜单和接口权限已更新`);
        } catch (error: unknown) {
            const apiError = error as ApiErrorLike;
            console.error(error);
            showToast('error', '保存失败', apiError.response?.data?.detail || '权限保存失败');
        } finally {
            setSaving(false);
        }
    };

    return (
        <div className="menu-permissions-page fade-in">
            <ToastContainer toasts={toasts} removeToast={removeToast} />

            <section className="menu-permissions-hero">
                <div>
                    <p className="menu-permissions-eyebrow">Role Permission Control</p>
                    <h1>菜单权限管理</h1>
                    <p className="menu-permissions-subtitle">
                        统一维护角色的菜单可见范围与后端管理接口权限。菜单决定页面入口是否展示，接口权限决定对应管理能力是否可调用。
                    </p>
                </div>
                <div className="menu-permissions-actions">
                    <button className="menu-btn menu-btn-secondary" onClick={() => void fetchData()} disabled={loading}>
                        <RefreshCw size={16} />
                        刷新
                    </button>
                    <button
                        className="menu-btn menu-btn-primary"
                        onClick={handleSave}
                        disabled={loading || saving || !selectedRoleState?.editable || !hasChanges}
                    >
                        <Save size={16} />
                        {saving ? '保存中...' : '保存权限'}
                    </button>
                </div>
            </section>

            <section className="menu-permissions-summary">
                <div className="summary-card">
                    <span>当前角色</span>
                    <strong>{selectedRoleState?.label || '-'}</strong>
                    <small>{selectedRoleState?.description || '请选择一个角色进行配置。'}</small>
                </div>
                <div className="summary-card">
                    <span>已选菜单</span>
                    <strong>{draftMenuKeys.length}</strong>
                    <small>当前角色可见的菜单数量</small>
                </div>
                <div className="summary-card">
                    <span>已选接口</span>
                    <strong>{draftApiKeys.length}</strong>
                    <small>当前角色可调用的管理接口数量</small>
                </div>
                <div className="summary-card accent">
                    <span>权限模式</span>
                    <strong>{selectedRoleState?.editable ? '可编辑' : '只读'}</strong>
                    <small>{selectedRoleState?.editable ? '支持自定义角色权限' : '管理员角色固定为全量权限'}</small>
                </div>
            </section>

            <section className="menu-permissions-layout">
                <aside className="role-panel">
                    <div className="panel-header">
                        <div>
                            <h2>角色列表</h2>
                            <p>选择需要配置权限的角色。</p>
                        </div>
                        <div className="panel-badge">
                            <ShieldCheck size={14} />
                            角色视角
                        </div>
                    </div>

                    <div className="role-list">
                        {(overview?.roles || []).map((role) => (
                            <button
                                key={role.role}
                                className={`role-card ${selectedRole === role.role ? 'active' : ''}`}
                                onClick={() => setSelectedRole(role.role)}
                            >
                                <div className="role-card-header">
                                    <strong>{role.label}</strong>
                                    <span className={`role-mode ${role.editable ? 'editable' : 'readonly'}`}>
                                        {role.editable ? '可编辑' : '只读'}
                                    </span>
                                </div>
                                <p>{role.description || '暂无角色说明。'}</p>
                                <small>{role.menu_keys.length} 个菜单，{role.api_keys.length} 个接口权限</small>
                            </button>
                        ))}
                    </div>
                </aside>

                <div className="section-list">
                    <PermissionSectionPanel
                        title="菜单权限"
                        description="控制角色能否在左侧导航中看到对应页面入口。"
                        sections={menuSections}
                        selectedKeys={draftMenuKeys}
                        editable={Boolean(selectedRoleState?.editable)}
                        onToggle={toggleMenuKey}
                        onSelectAll={() => setDraftMenuKeys(visibleMenus.map((item) => item.key))}
                        onReset={() => setDraftMenuKeys(selectedRoleState?.menu_keys || [])}
                        loading={loading}
                        showRequiredTag
                    />

                    <PermissionSectionPanel
                        title="接口权限"
                        description="控制角色能否调用对应模块的后端管理接口。"
                        sections={apiSections}
                        selectedKeys={draftApiKeys}
                        editable={Boolean(selectedRoleState?.editable)}
                        onToggle={toggleApiKey}
                        onSelectAll={() => setDraftApiKeys(visibleApis.map((item) => item.key))}
                        onReset={() => setDraftApiKeys(selectedRoleState?.api_keys || [])}
                        loading={loading}
                    />
                </div>
            </section>
        </div>
    );
};

export default MenuPermissionsPage;
