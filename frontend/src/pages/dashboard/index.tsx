import { useEffect, useMemo, useState } from 'react';
import { ArrowRight, TrendingUp } from 'lucide-react';
import RevenueChart from '../../components/charts/RevenueChart';
import ExpenseChart from '../../components/charts/ExpenseChart';
import { getBills, getChargeItemsRanking, getIncomeTrend, getProjects } from '../../services/api';
import type { Bill, Project } from '../../types';
import { getAuthUser } from '../../utils/authStorage';
import './Dashboard.css';

type TrendPoint = {
    name: string;
    income: number;
};

type RankingPoint = {
    name: string;
    value: number;
};

type ChargeItemRankingRow = {
    item_name?: string;
    percentage?: number;
};

const Dashboard = () => {
    const [incomeTrend, setIncomeTrend] = useState<TrendPoint[]>([]);
    const [ranking, setRanking] = useState<RankingPoint[]>([]);
    const [recentBills, setRecentBills] = useState<Bill[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    const currentUser = useMemo(() => {
        return getAuthUser<{ role?: string; api_keys?: unknown[] }>();
    }, []);

    const canReadProjects = useMemo(() => {
        if (currentUser?.role === 'admin') return true;
        const apiKeys = Array.isArray(currentUser?.api_keys)
            ? currentUser.api_keys.filter((item: unknown): item is string => typeof item === 'string')
            : [];
        return apiKeys.includes('project.manage');
    }, [currentUser]);

    useEffect(() => {
        const fetchData = async () => {
            setIsLoading(true);
            try {
                const [trendResult, rankingResult, billsResult, projectsResult] = await Promise.allSettled([
                    getIncomeTrend(),
                    getChargeItemsRanking(5),
                    getBills({ limit: 5 }),
                    canReadProjects ? getProjects() : Promise.resolve({ items: [] }),
                ]);

                if (trendResult.status === 'fulfilled') {
                    const trendData = trendResult.value;
                    const formattedTrend = (trendData.labels || []).map((label: number, index: number) => ({
                        name: `${label}月`,
                        income: trendData.data?.[index] || 0,
                    }));
                    setIncomeTrend(formattedTrend);
                } else {
                    console.error('Failed to fetch income trend:', trendResult.reason);
                    setIncomeTrend([]);
                }

                if (rankingResult.status === 'fulfilled') {
                    const formattedRanking = (rankingResult.value || []).map((item: ChargeItemRankingRow) => ({
                        name: item.item_name,
                        value: item.percentage,
                    }));
                    setRanking(formattedRanking);
                } else {
                    console.error('Failed to fetch charge items ranking:', rankingResult.reason);
                    setRanking([]);
                }

                if (billsResult.status === 'fulfilled') {
                    setRecentBills(billsResult.value || []);
                } else {
                    console.error('Failed to fetch recent bills:', billsResult.reason);
                    setRecentBills([]);
                }

                if (projectsResult.status === 'fulfilled') {
                    const payload = projectsResult.value;
                    const items = Array.isArray(payload) ? payload : payload.items || [];
                    setProjects(items.slice(0, 3));
                } else {
                    console.error('Failed to fetch projects overview:', projectsResult.reason);
                    setProjects([]);
                }
            } finally {
                setIsLoading(false);
            }
        };

        void fetchData();
    }, [canReadProjects]);

    return (
        <div className="dashboard-container">
            <div className="dashboard-main-grid">
                <div className="card glass chart-section">
                    <div className="card-header">
                        <h3>收入趋势</h3>
                        <div className="action-buttons">
                            <button className="btn-sm active">今年</button>
                        </div>
                    </div>
                    <div className="card-body">
                        <RevenueChart data={incomeTrend} loading={isLoading} />
                    </div>
                </div>

                <div className="card glass chart-section">
                    <div className="card-header">
                        <h3>收入构成 (Top 5)</h3>
                        <button className="btn-icon">
                            <TrendingUp size={16} />
                        </button>
                    </div>
                    <div className="card-body">
                        <ExpenseChart data={ranking} loading={isLoading} />
                    </div>
                </div>
            </div>

            <div className="dashboard-bottom-grid">
                <div className="card glass recent-bills">
                    <div className="card-header">
                        <h3>最近账单</h3>
                        <a href="/bills" className="btn-link">
                            查看全部 <ArrowRight size={16} />
                        </a>
                    </div>
                    <div className="card-body">
                        {isLoading ? (
                            <div className="loading-placeholder">加载中...</div>
                        ) : recentBills.length > 0 ? (
                            <table className="simple-table">
                                <thead>
                                    <tr>
                                        <th>序号</th>
                                        <th>账单ID</th>
                                        <th>费项</th>
                                        <th>金额</th>
                                        <th>状态</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {recentBills.map((bill, index) => (
                                        <tr key={bill.id}>
                                            <td>{index + 1}</td>
                                            <td>{bill.id}</td>
                                            <td>{bill.charge_item_name}</td>
                                            <td>{bill.amount?.toLocaleString()}</td>
                                            <td>
                                                <span className={`badge ${bill.pay_status_str === '已缴' ? 'success' : 'warning'}`}>
                                                    {bill.pay_status_str}
                                                </span>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        ) : (
                            <div className="empty-placeholder">暂无账单数据</div>
                        )}
                    </div>
                </div>

                <div className="card glass project-status">
                    <div className="card-header">
                        <h3>项目概览</h3>
                        {canReadProjects ? (
                            <a href="/projects" className="btn-link">查看详情</a>
                        ) : (
                            <span className="btn-link" style={{ cursor: 'default', opacity: 0.6 }}>只读概览</span>
                        )}
                    </div>
                    <div className="card-body">
                        {isLoading ? (
                            <div className="loading-placeholder">加载中...</div>
                        ) : projects.length > 0 ? (
                            <div className="status-list">
                                {projects.map((project) => (
                                    <div className="status-item" key={project.proj_id}>
                                        <div className="status-info">
                                            <p className="status-name">{project.proj_name}</p>
                                            <p className="status-desc">项目代码: {project.proj_id}</p>
                                        </div>
                                        <div className="status-badge success">正常运营</div>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="empty-placeholder">
                                {canReadProjects ? '暂无项目数据' : '当前账号无项目管理权限'}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Dashboard;
