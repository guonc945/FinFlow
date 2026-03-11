import { useState, useEffect } from 'react';
import { Search, Landmark, X, ChevronLeft, ChevronRight, Loader2 } from 'lucide-react';
import { getBankAccounts } from '../../services/api';
import type { BankAccountBrief } from '../../types';
import './KingdeeProjectSelector.css'; // 复用相同样式

interface BankAccountItem {
    id: string;
    name: string;
    bankaccountnumber: string;
    acctname?: string;
    bank_name?: string;
    accttype?: string;
    acctstatus?: string;
}

interface BankAccountSelectorProps {
    value?: string; // 显示文本
    onSelect: (account: BankAccountBrief | null) => void;
    label?: string;
    placeholder?: string;
}

const BankAccountSelector = ({
    value,
    onSelect,
    label,
    placeholder = '点击选择银行账户...'
}: BankAccountSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [inputValue, setInputValue] = useState(value || '');

    // 搜索 & 数据
    const [searchTerm, setSearchTerm] = useState('');
    const [debouncedSearch, setDebouncedSearch] = useState('');
    const [accounts, setAccounts] = useState<BankAccountItem[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    // 分页
    const [page, setPage] = useState(1);
    const [total, setTotal] = useState(0);
    const pageSize = 50;

    useEffect(() => {
        setInputValue(value || '');
    }, [value]);

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearch(searchTerm);
            setPage(1);
        }, 500);
        return () => clearTimeout(timer);
    }, [searchTerm]);

    useEffect(() => {
        if (isModalOpen) {
            fetchAccounts();
        }
    }, [isModalOpen, debouncedSearch, page]);

    const fetchAccounts = async () => {
        setIsLoading(true);
        try {
            const res = await getBankAccounts({
                search: debouncedSearch || undefined,
                skip: (page - 1) * pageSize,
                limit: pageSize
            });
            setAccounts(res.items || []);
            setTotal(res.total || 0);
        } catch (error) {
            console.error('获取银行账户列表失败:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSelect = (account: BankAccountItem) => {
        const brief: BankAccountBrief = {
            id: account.id,
            name: account.name,
            bankaccountnumber: account.bankaccountnumber,
            bank_name: account.bank_name
        };
        setInputValue(`${account.name} (${account.bankaccountnumber})`);
        onSelect(brief);
        setIsModalOpen(false);
    };

    const handleClear = () => {
        setInputValue('');
        onSelect(null);
    };

    const totalPages = Math.ceil(total / pageSize);

    // 账户性质映射
    const acctTypeLabel = (t?: string) => {
        if (t === 'in_out') return '收支户';
        if (t === 'in') return '收入户';
        if (t === 'out') return '支出户';
        return t || '-';
    };

    return (
        <div className="project-selector-container">
            {label && <label className="selector-label">{label}</label>}
            <div className="selector-input-wrapper">
                <input
                    type="text"
                    className="selector-input"
                    placeholder={placeholder}
                    value={inputValue}
                    readOnly
                    onClick={() => setIsModalOpen(true)}
                    style={{ cursor: 'pointer' }}
                />
                <div className="selector-actions">
                    {inputValue && (
                        <button type="button" className="clear-btn" onClick={handleClear} title="清除绑定">
                            <X size={14} />
                        </button>
                    )}
                    <button type="button" className="search-trigger-btn" onClick={() => setIsModalOpen(true)}>
                        <Landmark size={14} />
                    </button>
                </div>
            </div>

            {isModalOpen && (
                <div className="project-modal-overlay" onClick={() => setIsModalOpen(false)}>
                    <div className="project-modal-content animate-scale-in" onClick={e => e.stopPropagation()} style={{ maxWidth: '720px' }}>
                        <div className="modal-header border-b border-slate-100/50 pb-4">
                            <div className="flex items-center gap-2">
                                <Landmark size={20} className="text-primary" />
                                <h3 className="text-lg font-bold">选择银行账户</h3>
                            </div>
                            <button className="modal-close-btn" onClick={() => setIsModalOpen(false)}>
                                <X size={20} />
                            </button>
                        </div>

                        <div className="project-modal-body p-4 flex flex-col gap-4">
                            <div className="search-bar-wrapper">
                                <Search size={16} className="search-icon" />
                                <input
                                    type="text"
                                    placeholder="搜索账户名称、银行账号、开户行..."
                                    value={searchTerm}
                                    onChange={(e) => setSearchTerm(e.target.value)}
                                    autoFocus
                                />
                            </div>

                            <div className="project-list-wrapper custom-scrollbar">
                                <table className="project-table">
                                    <thead>
                                        <tr>
                                            <th>账户简称</th>
                                            <th>银行账号</th>
                                            <th>开户行</th>
                                            <th>账户性质</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {isLoading && accounts.length === 0 ? (
                                            <tr>
                                                <td colSpan={4}>
                                                    <div className="flex justify-center py-8">
                                                        <Loader2 className="animate-spin text-slate-300" size={24} />
                                                    </div>
                                                </td>
                                            </tr>
                                        ) : accounts.length > 0 ? (
                                            accounts.map(acct => (
                                                <tr key={acct.id} onClick={() => handleSelect(acct)} className="project-row">
                                                    <td className="text-slate-700 font-medium">{acct.name || '-'}</td>
                                                    <td className="font-mono text-primary font-medium" style={{ fontSize: '0.8rem' }}>{acct.bankaccountnumber || '-'}</td>
                                                    <td className="text-sm text-slate-500">{acct.bank_name || '-'}</td>
                                                    <td>
                                                        <span style={{
                                                            fontSize: '0.7rem', fontWeight: 600,
                                                            padding: '0.15rem 0.4rem', borderRadius: '4px',
                                                            background: acct.accttype === 'in' ? '#dcfce7' : acct.accttype === 'out' ? '#fef3c7' : '#dbeafe',
                                                            color: acct.accttype === 'in' ? '#15803d' : acct.accttype === 'out' ? '#b45309' : '#1d4ed8'
                                                        }}>
                                                            {acctTypeLabel(acct.accttype)}
                                                        </span>
                                                    </td>
                                                </tr>
                                            ))
                                        ) : (
                                            <tr>
                                                <td colSpan={4}>
                                                    <div className="text-center py-8 text-slate-400">暂无银行账户数据</div>
                                                </td>
                                            </tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>

                            <div className="modal-footer pt-3 border-t border-slate-100 flex justify-between items-center">
                                <div className="text-xs text-slate-500">
                                    共找到 <span className="font-bold text-slate-700">{total}</span> 个银行账户
                                </div>
                                <div className="pagination flex items-center gap-2">
                                    <button
                                        disabled={page <= 1 || isLoading}
                                        onClick={() => setPage(p => p - 1)}
                                        className="btn-icon"
                                    >
                                        <ChevronLeft size={16} />
                                    </button>
                                    <span className="text-sm px-2">{page} / {totalPages || 1}</span>
                                    <button
                                        disabled={page >= totalPages || isLoading}
                                        onClick={() => setPage(p => p + 1)}
                                        className="btn-icon"
                                    >
                                        <ChevronRight size={16} />
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default BankAccountSelector;
