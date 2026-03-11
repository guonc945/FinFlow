import { useState, useEffect } from 'react';
import { Plus, Edit2, Trash2, Save, X, Search, Hash, Tag, Eye, EyeOff, Building2, CheckSquare, Square } from 'lucide-react';
import axios from 'axios';
import { API_BASE_URL } from '../../services/apiBase';
import './Variables.css';

// 园区多选变量的 key
const COMMUNITY_VAR_KEY = 'MARKI_COMMUNITY_IDS';
// Deal time range variable key (minDealTime/maxDealTime)
// Stored as a JSON fragment so it can be embedded into request bodies:
// "maxDealTime":1773331199,"minDealTime":1772985600
const DEAL_TIME_RANGE_VAR_KEY = 'MARKI_DEAL_TIME_RANGE';

interface Project {
    proj_id: number;
    proj_name: string;
}

interface GlobalVariable {
    id: number;
    key: string;
    value: string;
    description: string;
    category: string;
    is_secret: boolean;
}

const GlobalVariables = () => {
    const [variables, setVariables] = useState<GlobalVariable[]>([]);
    const [searchQuery, setSearchQuery] = useState('');
    const [isEditing, setIsEditing] = useState(false);
    const [currentVar, setCurrentVar] = useState<Partial<GlobalVariable>>({});
    const [showValues, setShowValues] = useState<Record<number, boolean>>({});
    const [projects, setProjects] = useState<Project[]>([]);
    const [selectedCommunityIds, setSelectedCommunityIds] = useState<Set<string>>(new Set());
    const [projectSearch, setProjectSearch] = useState('');

    const [dealStartDate, setDealStartDate] = useState('');
    const [dealEndDate, setDealEndDate] = useState('');
    const [dealMinTs, setDealMinTs] = useState<number | null>(null);
    const [dealMaxTs, setDealMaxTs] = useState<number | null>(null);

    useEffect(() => {
        fetchVariables();
        fetchProjects();
    }, []);

    const fetchVariables = async () => {
        try {
            const res = await axios.get(`${API_BASE_URL}/settings/variables`);
            setVariables(res.data);
        } catch (error) {
            console.error('Failed to fetch variables:', error);
        }
    };

    const fetchProjects = async () => {
        try {
            const res = await axios.get(`${API_BASE_URL}/projects?limit=500`);
            setProjects(res.data);
        } catch (error) {
            console.error('Failed to fetch projects:', error);
        }
    };

    // 当打开编辑弹窗时，如果是园区变量，解析已选 ID
    const pad2 = (n: number) => String(n).padStart(2, '0');

    const toLocalDateString = (d: Date) => {
        return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    };

    const parseDealTimeRangeValue = (value: string | null | undefined) => {
        const v = String(value || '').trim();
        if (!v) return null;

        // Accept JSON fragment: "maxDealTime":177...,"minDealTime":177...
        // Or JSON object: {"maxDealTime":...,"minDealTime":...}
        const minMatch = v.match(/"minDealTime"\s*:\s*(\d+)/);
        const maxMatch = v.match(/"maxDealTime"\s*:\s*(\d+)/);
        const min = minMatch ? Number(minMatch[1]) : NaN;
        const max = maxMatch ? Number(maxMatch[1]) : NaN;
        if (!Number.isFinite(min) || !Number.isFinite(max)) return null;

        const start = toLocalDateString(new Date(min * 1000));
        const end = toLocalDateString(new Date(max * 1000));
        return { start, end };
    };

    const computeDealTimeRange = (start: string, end: string) => {
        const startVal = (start || '').trim();
        const endVal = (end || '').trim() || startVal;
        if (!startVal) return null;

        // Local time boundary: start 00:00:00, end 23:59:59
        const minMs = new Date(`${startVal}T00:00:00`).getTime();
        const maxMs = new Date(`${endVal}T23:59:59`).getTime();
        if (!Number.isFinite(minMs) || !Number.isFinite(maxMs)) return null;

        let min = Math.floor(minMs / 1000);
        let max = Math.floor(maxMs / 1000);
        if (min > max) {
            const tmp = min;
            min = max;
            max = tmp;
        }

        const fragment = `"maxDealTime":${max},"minDealTime":${min}`;
        return { min, max, fragment, normalizedEnd: endVal };
    };

    const updateDealTimeRange = (start: string, end: string) => {
        const computed = computeDealTimeRange(start, end);
        setDealStartDate(start);
        setDealEndDate(computed?.normalizedEnd || end);
        setDealMinTs(computed?.min ?? null);
        setDealMaxTs(computed?.max ?? null);
        setCurrentVar(cv => ({ ...cv, value: computed?.fragment || '' }));
    };

    const openEditor = (varData: Partial<GlobalVariable>) => {
        setCurrentVar(varData);
        if (varData.key === COMMUNITY_VAR_KEY && varData.value) {
            const ids = varData.value.split(',').map(s => s.trim()).filter(Boolean);
            setSelectedCommunityIds(new Set(ids));
        } else {
            setSelectedCommunityIds(new Set());
        }

        if (varData.key === DEAL_TIME_RANGE_VAR_KEY) {
            const parsed = parseDealTimeRangeValue(varData.value || '');
            if (parsed) {
                updateDealTimeRange(parsed.start, parsed.end);
            } else {
                const today = new Date();
                const end = toLocalDateString(today);
                const start = toLocalDateString(new Date(today.getTime() - 6 * 24 * 60 * 60 * 1000));
                updateDealTimeRange(start, end);
            }
        } else {
            setDealStartDate('');
            setDealEndDate('');
            setDealMinTs(null);
            setDealMaxTs(null);
        }
        setProjectSearch('');
        setIsEditing(true);
    };

    const toggleCommunityId = (id: string) => {
        setSelectedCommunityIds(prev => {
            const next = new Set(prev);
            if (next.has(id)) {
                next.delete(id);
            } else {
                next.add(id);
            }
            // 同步更新 currentVar.value
            setCurrentVar(cv => ({ ...cv, value: Array.from(next).join(',') }));
            return next;
        });
    };

    const selectAllCommunities = () => {
        const allIds = new Set(filteredProjects.map(p => String(p.proj_id)));
        setSelectedCommunityIds(allIds);
        setCurrentVar(cv => ({ ...cv, value: Array.from(allIds).join(',') }));
    };

    const deselectAllCommunities = () => {
        setSelectedCommunityIds(new Set());
        setCurrentVar(cv => ({ ...cv, value: '' }));
    };

    const filteredProjects = projects.filter(p =>
        p.proj_name.toLowerCase().includes(projectSearch.toLowerCase()) ||
        String(p.proj_id).includes(projectSearch)
    );

    // 根据 value 中的 ID 解析出园区名称列表
    const getCommunityNames = (value: string) => {
        if (!value) return [];
        const ids = value.split(',').map(s => s.trim()).filter(Boolean);
        return ids.map(id => {
            const p = projects.find(proj => String(proj.proj_id) === id);
            return p ? p.proj_name : `ID:${id}`;
        });
    };

    const isCommunityVar = currentVar.key === COMMUNITY_VAR_KEY;
    const isDealTimeRangeVar = currentVar.key === DEAL_TIME_RANGE_VAR_KEY;

    const handleSave = async () => {
        try {
            if (currentVar.id) {
                await axios.put(`${API_BASE_URL}/settings/variables/${currentVar.id}`, currentVar);
            } else {
                await axios.post(`${API_BASE_URL}/settings/variables`, currentVar);
            }
            setIsEditing(false);
            fetchVariables();
        } catch (error) {
            alert('保存失败: ' + ((error as any).response?.data?.detail || '发生未知错误'));
        }
    };

    const handleDelete = async (id: number) => {
        if (!confirm('确定要删除这个全局变量吗?')) return;
        try {
            await axios.delete(`${API_BASE_URL}/settings/variables/${id}`);
            fetchVariables();
        } catch (error) {
            alert('删除失败');
        }
    };

    const toggleShowValue = (id: number) => {
        setShowValues(prev => ({ ...prev, [id]: !prev[id] }));
    };

    const filteredVariables = variables.filter(v =>
        v.key.toLowerCase().includes(searchQuery.toLowerCase()) ||
        v.description?.toLowerCase().includes(searchQuery.toLowerCase())
    );

    return (
        <div className="variables-page">
            <header className="page-header-pro">
                <div>
                    <h2 className="text-2xl font-bold text-slate-900 tracking-tight">全局变量管理</h2>
                    <p className="text-sm text-slate-500 mt-1">定义系统范围内的动态参数，支持在接口地址、授权信息、数据模板等位置通过 {'{key}'} 调用。</p>
                </div>
                <button
                    className="btn-primary-clean flex items-center gap-2"
                    onClick={() => openEditor({ category: 'common', is_secret: false })}
                >
                    <Plus size={18} />
                    新增变量
                </button>
            </header>

            <div className="variables-toolbar mt-6">
                <div className="search-box-pro">
                    <Search size={18} className="text-slate-400" />
                    <input
                        type="text"
                        placeholder="搜索变量名或描述..."
                        value={searchQuery}
                        onChange={e => setSearchQuery(e.target.value)}
                    />
                </div>
            </div>

            <div className="variables-grid mt-6">
                {filteredVariables.map(v => (
                    <div key={v.id} className={`variable-card-pro ${v.is_secret ? 'is-secret' : ''}`}>
                        <div className="var-header">
                            <div className="flex items-center gap-2">
                                <div className="var-icon">
                                    <Hash size={16} />
                                </div>
                                <span className="var-key font-mono">{v.key}</span>
                            </div>
                            <div className="var-actions">
                                <button onClick={() => openEditor(v)} className="action-btn-pro hover:bg-slate-100">
                                    <Edit2 size={14} />
                                </button>
                                <button onClick={() => handleDelete(v.id)} className="action-btn-pro text-red-400 hover:bg-red-50">
                                    <Trash2 size={14} />
                                </button>
                            </div>
                        </div>

                        <div className="var-body">
                            <div className="var-value-box">
                                <span className="text-[10px] text-slate-400 uppercase font-bold tracking-widest mb-1 block">当前值</span>
                                {v.key === COMMUNITY_VAR_KEY && v.value ? (
                                    <div className="flex flex-wrap gap-1 mt-1">
                                        {getCommunityNames(v.value).map((name, idx) => (
                                            <span key={idx} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium" style={{ background: 'linear-gradient(135deg, #dbeafe, #e0e7ff)', color: '#3730a3' }}>
                                                <Building2 size={10} />
                                                {name}
                                            </span>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="flex items-center justify-between">
                                        <code className="text-sm font-mono truncate mr-2">
                                            {v.is_secret && !showValues[v.id] ? '••••••••' : v.value}
                                        </code>
                                        {v.is_secret && (
                                            <button onClick={() => toggleShowValue(v.id)} className="text-slate-400 hover:text-slate-600">
                                                {showValues[v.id] ? <EyeOff size={14} /> : <Eye size={14} />}
                                            </button>
                                        )}
                                    </div>
                                )}
                            </div>

                            <div className="var-meta mt-3">
                                <div className="flex items-center gap-1 text-[11px] text-slate-400">
                                    <Tag size={12} />
                                    <span>{v.category || '未分类'}</span>
                                </div>
                                <p className="text-xs text-slate-500 mt-2 line-clamp-2 leading-relaxed">
                                    {v.description || '暂无描述信息'}
                                </p>
                            </div>
                        </div>

                        <div className="var-usage-hint mt-3 pt-3 border-t border-slate-100">
                            <span className="text-[9px] text-slate-300 font-mono">调用方式: {'{' + v.key + '}'}</span>
                        </div>

                    </div>
                ))}

                {filteredVariables.length === 0 && (
                    <div className="col-span-full py-20 text-center bg-slate-50/50 rounded-2xl border-2 border-dashed border-slate-200">
                        <div className="bg-white w-12 h-12 rounded-full flex items-center justify-center shadow-sm mx-auto mb-4">
                            <Hash size={24} className="text-slate-300" />
                        </div>
                        <p className="text-slate-400">未找到任何变量</p>
                    </div>
                )}
            </div>

            {/* Edit Modal */}
            {isEditing && (
                <div className="modal-overlay">
                    <div className="modal-content-pro w-[450px]">
                        <header className="modal-header-clean">
                            <h3 className="font-bold text-slate-900">{currentVar.id ? '编辑变量' : '新增变量'}</h3>
                            <button onClick={() => setIsEditing(false)} className="text-slate-400 hover:text-slate-600">
                                <X size={20} />
                            </button>
                        </header>
                        <div className="p-6 space-y-4">
                            <div className="field-container">
                                <label className="modern-label text-xs">变量标识 (Key)</label>
                                <input
                                    className="modern-input-pro font-mono text-sm"
                                    value={currentVar.key || ''}
                                    onChange={e => {
                                        const nextKey = e.target.value;
                                        setCurrentVar(cv => ({ ...cv, key: nextKey }));

                                        // When creating a new variable, typing the special key should initialize the helper UI.
                                        if (!currentVar.id && nextKey === DEAL_TIME_RANGE_VAR_KEY) {
                                            const today = new Date();
                                            const end = toLocalDateString(today);
                                            const start = toLocalDateString(new Date(today.getTime() - 6 * 24 * 60 * 60 * 1000));
                                            updateDealTimeRange(start, end);
                                        }
                                    }}
                                    placeholder="e.g. ERP_BASE_URL"
                                    disabled={!!currentVar.id}
                                />
                                <p className="text-[10px] text-slate-400 mt-1">全局唯一标识，通过 {'{' + (currentVar.key || 'key') + '}'} 调用</p>
                            </div>
                            {isCommunityVar ? (
                                <div className="field-container">
                                    <label className="modern-label text-xs">选择园区项目</label>
                                    <div style={{ border: '1px solid #e2e8f0', borderRadius: '10px', overflow: 'hidden', background: '#fff' }}>
                                        <div style={{ padding: '0.5rem 0.75rem', borderBottom: '1px solid #f1f5f9', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                            <Search size={14} className="text-slate-400" />
                                            <input
                                                type="text"
                                                placeholder="搜索园区..."
                                                value={projectSearch}
                                                onChange={e => setProjectSearch(e.target.value)}
                                                style={{ border: 'none', outline: 'none', flex: 1, fontSize: '0.8rem', background: 'transparent' }}
                                            />
                                            <span style={{ fontSize: '0.7rem', color: '#94a3b8', whiteSpace: 'nowrap' }}>
                                                已选 {selectedCommunityIds.size}/{projects.length}
                                            </span>
                                        </div>
                                        <div style={{ padding: '0.4rem 0.75rem', borderBottom: '1px solid #f1f5f9', display: 'flex', gap: '0.75rem', background: '#f8fafc' }}>
                                            <button type="button" onClick={selectAllCommunities} style={{ fontSize: '0.7rem', color: '#3b82f6', background: 'none', border: 'none', cursor: 'pointer', fontWeight: 600 }}>全选</button>
                                            <button type="button" onClick={deselectAllCommunities} style={{ fontSize: '0.7rem', color: '#ef4444', background: 'none', border: 'none', cursor: 'pointer', fontWeight: 600 }}>全不选</button>
                                        </div>
                                        <div style={{ maxHeight: '200px', overflowY: 'auto', padding: '0.25rem 0' }} className="custom-scrollbar">
                                            {filteredProjects.map(p => {
                                                const isSelected = selectedCommunityIds.has(String(p.proj_id));
                                                return (
                                                    <div
                                                        key={p.proj_id}
                                                        onClick={() => toggleCommunityId(String(p.proj_id))}
                                                        style={{
                                                            display: 'flex', alignItems: 'center', gap: '0.5rem',
                                                            padding: '0.4rem 0.75rem', cursor: 'pointer',
                                                            background: isSelected ? '#eff6ff' : 'transparent',
                                                            transition: 'background 0.15s',
                                                            fontSize: '0.8rem'
                                                        }}
                                                        onMouseEnter={e => { if (!isSelected) (e.currentTarget as HTMLDivElement).style.background = '#f8fafc'; }}
                                                        onMouseLeave={e => { (e.currentTarget as HTMLDivElement).style.background = isSelected ? '#eff6ff' : 'transparent'; }}
                                                    >
                                                        {isSelected ? <CheckSquare size={14} style={{ color: '#3b82f6', flexShrink: 0 }} /> : <Square size={14} style={{ color: '#cbd5e1', flexShrink: 0 }} />}
                                                        <Building2 size={13} style={{ color: isSelected ? '#3b82f6' : '#94a3b8', flexShrink: 0 }} />
                                                        <span style={{ color: isSelected ? '#1e40af' : '#475569', fontWeight: isSelected ? 600 : 400 }}>{p.proj_name}</span>
                                                        <span style={{ marginLeft: 'auto', fontSize: '0.65rem', color: '#94a3b8', fontFamily: 'monospace' }}>ID: {p.proj_id}</span>
                                                    </div>
                                                );
                                            })}
                                            {filteredProjects.length === 0 && (
                                                <div style={{ padding: '1rem', textAlign: 'center', color: '#94a3b8', fontSize: '0.8rem' }}>无匹配园区</div>
                                            )}
                                        </div>
                                    </div>
                                    <p className="text-[10px] text-slate-400 mt-1">已选园区 ID 将以逗号分隔存储，可在接口请求体中通过 {'{MARKI_COMMUNITY_IDS}'} 引用</p>
                                </div>
                            ) : isDealTimeRangeVar ? (
                                <div className="field-container">
                                    <label className="modern-label text-xs">Deal Time Range (minDealTime / maxDealTime)</label>
                                    <div className="grid grid-cols-2 gap-3">
                                        <div>
                                            <div className="text-[11px] text-slate-500 mb-1">Start Date</div>
                                            <input
                                                type="date"
                                                className="modern-input-pro text-sm"
                                                value={dealStartDate}
                                                onChange={(e) => updateDealTimeRange(e.target.value, dealEndDate)}
                                            />
                                        </div>
                                        <div>
                                            <div className="text-[11px] text-slate-500 mb-1">End Date</div>
                                            <input
                                                type="date"
                                                className="modern-input-pro text-sm"
                                                value={dealEndDate}
                                                onChange={(e) => updateDealTimeRange(dealStartDate, e.target.value)}
                                            />
                                        </div>
                                    </div>

                                    <div className="mt-3" style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '10px', padding: '10px 12px' }}>
                                        <div className="text-[10px] text-slate-400 uppercase font-bold tracking-widest mb-1">Preview</div>
                                        <code className="text-xs font-mono break-all">{currentVar.value || ''}</code>
                                        <div className="mt-2 text-[11px] text-slate-500 flex gap-4 flex-wrap">
                                            <span>minDealTime: <span className="font-mono">{dealMinTs ?? '-'}</span></span>
                                            <span>maxDealTime: <span className="font-mono">{dealMaxTs ?? '-'}</span></span>
                                        </div>
                                    </div>

                                    <p className="text-[10px] text-slate-400 mt-1">
                                        Stored as a JSON fragment for embedding. Use: <span className="font-mono">{'{MARKI_DEAL_TIME_RANGE}'}</span>
                                    </p>
                                </div>
                            ) : (
                                <div className="field-container">
                                    <label className="modern-label text-xs">变量值 (Value)</label>
                                    <textarea
                                        className="modern-textarea-pro min-h-[80px]"
                                        value={currentVar.value || ''}
                                        onChange={e => setCurrentVar({ ...currentVar, value: e.target.value })}
                                        placeholder="输入变量的具体内容"
                                    />
                                </div>
                            )}
                            <div className="grid grid-cols-2 gap-4">
                                <div className="field-container">
                                    <label className="modern-label text-xs">分组/列别</label>
                                    <select
                                        className="modern-input-pro text-sm"
                                        value={currentVar.category || 'common'}
                                        onChange={e => setCurrentVar({ ...currentVar, category: e.target.value })}
                                    >
                                        <option value="common">通用</option>
                                        <option value="api">接口相关</option>
                                        <option value="auth">认证相关</option>
                                        <option value="system">系统设置</option>
                                    </select>
                                </div>
                                <div className="field-container justify-end flex flex-col pt-6">
                                    <label className="flex items-center gap-2 cursor-pointer">
                                        <input
                                            type="checkbox"
                                            checked={currentVar.is_secret || false}
                                            onChange={e => setCurrentVar({ ...currentVar, is_secret: e.target.checked })}
                                        />
                                        <span className="text-xs font-bold text-slate-600">设为敏感信息</span>
                                    </label>
                                </div>
                            </div>
                            <div className="field-container">
                                <label className="modern-label text-xs">描述信息</label>
                                <input
                                    className="modern-input-pro text-sm"
                                    value={currentVar.description || ''}
                                    onChange={e => setCurrentVar({ ...currentVar, description: e.target.value })}
                                    placeholder="该变量的作用说明"
                                />
                            </div>
                        </div>
                        <footer className="modal-footer-sticky p-4 flex justify-end gap-3 bg-slate-50">
                            <button className="btn-secondary-clean px-4 text-sm" onClick={() => setIsEditing(false)}>取消</button>
                            <button className="btn-primary-clean px-6 flex items-center gap-2" onClick={handleSave}>
                                <Save size={16} />
                                保存变量
                            </button>
                        </footer>
                    </div>
                </div>
            )}
        </div>
    );
};

export default GlobalVariables;
