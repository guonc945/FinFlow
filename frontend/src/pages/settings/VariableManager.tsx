import { useEffect, useMemo, useState } from 'react';
import { Building2, CheckSquare, Edit2, Eye, EyeOff, Hash, Plus, Save, Search, Square, Tag, Trash2, X } from 'lucide-react';
import axios from 'axios';
import { API_BASE_URL } from '../../services/apiBase';

const COMMUNITY_VAR_KEY = 'MARKI_COMMUNITY_IDS';
const DEAL_TIME_RANGE_VAR_KEY = 'MARKI_DEAL_TIME_RANGE';

interface Project {
    proj_id: number;
    proj_name: string;
}

interface GlobalVariable {
    id: number;
    key: string;
    value: string;
    description?: string;
    category: string;
    is_secret: boolean;
}

const VariableManager = () => {
    const [variables, setVariables] = useState<GlobalVariable[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [searchQuery, setSearchQuery] = useState('');
    const [isEditing, setIsEditing] = useState(false);
    const [currentVar, setCurrentVar] = useState<Partial<GlobalVariable>>({});
    const [showValues, setShowValues] = useState<Record<number, boolean>>({});
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
        const res = await axios.get(`${API_BASE_URL}/settings/variables`);
        setVariables(res.data);
    };

    const fetchProjects = async () => {
        const res = await axios.get(`${API_BASE_URL}/projects?limit=500`);
        setProjects(res.data);
    };

    const pad2 = (n: number) => String(n).padStart(2, '0');
    const toLocalDateString = (d: Date) => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;

    const parseDealTimeRangeValue = (value: string | null | undefined) => {
        const v = String(value || '').trim();
        if (!v) return null;
        const minMatch = v.match(/"minDealTime"\s*:\s*(\d+)/);
        const maxMatch = v.match(/"maxDealTime"\s*:\s*(\d+)/);
        const min = minMatch ? Number(minMatch[1]) : NaN;
        const max = maxMatch ? Number(maxMatch[1]) : NaN;
        if (!Number.isFinite(min) || !Number.isFinite(max)) return null;
        return {
            start: toLocalDateString(new Date(min * 1000)),
            end: toLocalDateString(new Date(max * 1000)),
        };
    };

    const updateDealTimeRange = (start: string, end: string) => {
        const startVal = (start || '').trim();
        const endVal = (end || '').trim() || startVal;
        if (!startVal) return;
        let min = Math.floor(new Date(`${startVal}T00:00:00`).getTime() / 1000);
        let max = Math.floor(new Date(`${endVal}T23:59:59`).getTime() / 1000);
        if (min > max) [min, max] = [max, min];
        setDealStartDate(startVal);
        setDealEndDate(endVal);
        setDealMinTs(min);
        setDealMaxTs(max);
        setCurrentVar(cv => ({ ...cv, value: `"maxDealTime":${max},"minDealTime":${min}` }));
    };

    const getErrorMessage = (error: unknown) => {
        const detail = (error as any)?.response?.data?.detail;
        if (typeof detail === 'string') return detail;
        if (Array.isArray(detail?.errors) && detail.errors.length) return detail.errors.join('\n');
        return detail?.message || '发生未知错误';
    };

    const openEditor = (varData: Partial<GlobalVariable>) => {
        setCurrentVar(varData);
        if (varData.key === COMMUNITY_VAR_KEY && varData.value) {
            setSelectedCommunityIds(new Set(varData.value.split(',').map(s => s.trim()).filter(Boolean)));
        } else {
            setSelectedCommunityIds(new Set());
        }
        if (varData.key === DEAL_TIME_RANGE_VAR_KEY) {
            const parsed = parseDealTimeRangeValue(varData.value);
            if (parsed) updateDealTimeRange(parsed.start, parsed.end);
        } else {
            setDealStartDate('');
            setDealEndDate('');
            setDealMinTs(null);
            setDealMaxTs(null);
        }
        setIsEditing(true);
    };

    const toggleCommunityId = (id: string) => {
        setSelectedCommunityIds(prev => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            setCurrentVar(cv => ({ ...cv, value: Array.from(next).join(',') }));
            return next;
        });
    };

    const filteredProjects = useMemo(() => projects.filter(p =>
        p.proj_name.toLowerCase().includes(projectSearch.toLowerCase()) || String(p.proj_id).includes(projectSearch)
    ), [projectSearch, projects]);

    const filteredVariables = useMemo(() => variables.filter(v =>
        v.key.toLowerCase().includes(searchQuery.toLowerCase()) ||
        (v.description || '').toLowerCase().includes(searchQuery.toLowerCase()) ||
        (v.value || '').toLowerCase().includes(searchQuery.toLowerCase())
    ), [searchQuery, variables]);

    const getCommunityNames = (value: string) => value.split(',').map(s => s.trim()).filter(Boolean).map(id => projects.find(p => String(p.proj_id) === id)?.proj_name || `ID:${id}`);

    const isCommunityVar = currentVar.key === COMMUNITY_VAR_KEY;
    const isDealTimeRangeVar = currentVar.key === DEAL_TIME_RANGE_VAR_KEY;

    const handleSave = async () => {
        try {
            if (currentVar.id) await axios.put(`${API_BASE_URL}/settings/variables/${currentVar.id}`, currentVar);
            else await axios.post(`${API_BASE_URL}/settings/variables`, currentVar);
            setIsEditing(false);
            fetchVariables();
        } catch (error) {
            alert(`保存失败: ${getErrorMessage(error)}`);
        }
    };

    const handleDelete = async (id: number) => {
        if (!window.confirm('确定要删除这个全局变量吗?')) return;
        try {
            await axios.delete(`${API_BASE_URL}/settings/variables/${id}`);
            fetchVariables();
        } catch (error) {
            alert(`删除失败: ${getErrorMessage(error)}`);
        }
    };

    return (
        <>
            <div className="section-toolbar mt-6">
                <div className="search-box-pro">
                    <Search size={18} className="text-slate-400" />
                    <input value={searchQuery} onChange={e => setSearchQuery(e.target.value)} placeholder="搜索变量名、描述或值..." />
                </div>
                <button className="btn-primary-clean flex items-center gap-2" onClick={() => openEditor({ category: 'common', is_secret: false })}>
                    <Plus size={18} />新增变量
                </button>
            </div>

            <div className="variables-grid mt-6">
                {filteredVariables.map(v => (
                    <div key={v.id} className={`variable-card-pro ${v.is_secret ? 'is-secret' : ''}`}>
                        <div className="var-header">
                            <div className="flex items-center gap-2"><div className="var-icon"><Hash size={16} /></div><span className="var-key font-mono">{v.key}</span></div>
                            <div className="var-actions">
                                <button onClick={() => openEditor(v)} className="action-btn-pro hover:bg-slate-100"><Edit2 size={14} /></button>
                                <button onClick={() => handleDelete(v.id)} className="action-btn-pro text-red-400 hover:bg-red-50"><Trash2 size={14} /></button>
                            </div>
                        </div>
                        <div className="var-value-box">
                            <span className="text-[10px] text-slate-400 uppercase font-bold tracking-widest mb-1 block">当前值</span>
                            {v.key === COMMUNITY_VAR_KEY && v.value ? (
                                <div className="flex flex-wrap gap-1 mt-1">{getCommunityNames(v.value).map((name, idx) => <span key={idx} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium variable-chip"><Building2 size={10} />{name}</span>)}</div>
                            ) : (
                                <div className="flex items-center justify-between gap-2">
                                    <code className="text-sm font-mono break-all">{v.is_secret && !showValues[v.id] ? '••••••••' : v.value}</code>
                                    {v.is_secret && <button onClick={() => setShowValues(prev => ({ ...prev, [v.id]: !prev[v.id] }))} className="text-slate-400 hover:text-slate-600">{showValues[v.id] ? <EyeOff size={14} /> : <Eye size={14} />}</button>}
                                </div>
                            )}
                        </div>
                        <div className="var-meta mt-3">
                            <div className="flex items-center gap-1 text-[11px] text-slate-400"><Tag size={12} /><span>{v.category || '未分类'}</span></div>
                            <p className="text-xs text-slate-500 mt-2 line-clamp-2 leading-relaxed">{v.description || '暂无描述信息'}</p>
                        </div>
                        <div className="var-usage-hint mt-3 pt-3 border-t border-slate-100"><span className="text-[9px] text-slate-300 font-mono">调用方式: {'{' + v.key + '}'}</span></div>
                    </div>
                ))}
            </div>

            {isEditing && (
                <div className="modal-overlay">
                    <div className="modal-content-pro w-[460px]">
                        <header className="modal-header-clean">
                            <h3 className="font-bold text-slate-900">{currentVar.id ? '编辑变量' : '新增变量'}</h3>
                            <button onClick={() => setIsEditing(false)} className="text-slate-400 hover:text-slate-600"><X size={20} /></button>
                        </header>
                        <div className="p-6 space-y-4">
                            <div className="field-container">
                                <label className="modern-label text-xs">变量标识 (Key)</label>
                                <input className="modern-input-pro font-mono text-sm" value={currentVar.key || ''} onChange={e => setCurrentVar(cv => ({ ...cv, key: e.target.value }))} disabled={!!currentVar.id} />
                            </div>
                            {isCommunityVar ? (
                                <div className="field-container">
                                    <label className="modern-label text-xs">选择园区项目</label>
                                    <div className="community-picker-box">
                                        <div className="community-picker-toolbar">
                                            <Search size={14} className="text-slate-400" />
                                            <input value={projectSearch} onChange={e => setProjectSearch(e.target.value)} placeholder="搜索园区..." />
                                        </div>
                                        <div className="community-picker-list">
                                            {filteredProjects.map(project => {
                                                const isSelected = selectedCommunityIds.has(String(project.proj_id));
                                                return <button type="button" key={project.proj_id} className={`community-picker-item ${isSelected ? 'active' : ''}`} onClick={() => toggleCommunityId(String(project.proj_id))}>{isSelected ? <CheckSquare size={14} /> : <Square size={14} />}<span>{project.proj_name}</span><small>ID:{project.proj_id}</small></button>;
                                            })}
                                        </div>
                                    </div>
                                </div>
                            ) : isDealTimeRangeVar ? (
                                <div className="field-container">
                                    <label className="modern-label text-xs">Deal Time Range</label>
                                    <div className="grid grid-cols-2 gap-3">
                                        <input type="date" className="modern-input-pro text-sm" value={dealStartDate} onChange={e => updateDealTimeRange(e.target.value, dealEndDate)} />
                                        <input type="date" className="modern-input-pro text-sm" value={dealEndDate} onChange={e => updateDealTimeRange(dealStartDate, e.target.value)} />
                                    </div>
                                    <div className="formula-preview-box mt-3"><code>{currentVar.value || ''}</code><div className="formula-inline-meta">minDealTime: {dealMinTs ?? '-'} | maxDealTime: {dealMaxTs ?? '-'}</div></div>
                                </div>
                            ) : (
                                <div className="field-container">
                                    <label className="modern-label text-xs">变量值 (Value)</label>
                                    <textarea className="modern-textarea-pro min-h-[90px]" value={currentVar.value || ''} onChange={e => setCurrentVar({ ...currentVar, value: e.target.value })} />
                                </div>
                            )}
                            <div className="grid grid-cols-2 gap-4">
                                <div className="field-container">
                                    <label className="modern-label text-xs">分组/类别</label>
                                    <select className="modern-input-pro text-sm" value={currentVar.category || 'common'} onChange={e => setCurrentVar({ ...currentVar, category: e.target.value })}>
                                        <option value="common">通用</option><option value="api">接口相关</option><option value="auth">认证相关</option><option value="system">系统设置</option>
                                    </select>
                                </div>
                                <div className="field-container justify-end flex flex-col pt-6">
                                    <label className="flex items-center gap-2 cursor-pointer"><input type="checkbox" checked={currentVar.is_secret || false} onChange={e => setCurrentVar({ ...currentVar, is_secret: e.target.checked })} /><span className="text-xs font-bold text-slate-600">设为敏感信息</span></label>
                                </div>
                            </div>
                            <div className="field-container">
                                <label className="modern-label text-xs">描述信息</label>
                                <input className="modern-input-pro text-sm" value={currentVar.description || ''} onChange={e => setCurrentVar({ ...currentVar, description: e.target.value })} />
                            </div>
                        </div>
                        <footer className="modal-footer-sticky p-4 flex justify-end gap-3 bg-slate-50">
                            <button className="btn-secondary-clean px-4 text-sm" onClick={() => setIsEditing(false)}>取消</button>
                            <button className="btn-primary-clean px-6 flex items-center gap-2" onClick={handleSave}><Save size={16} />保存变量</button>
                        </footer>
                    </div>
                </div>
            )}
        </>
    );
};

export default VariableManager;
