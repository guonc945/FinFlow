import { useState, useEffect } from 'react';
import { TrendingUp, ArrowRight } from 'lucide-react';
import RevenueChart from '../../components/charts/RevenueChart';
import ExpenseChart from '../../components/charts/ExpenseChart';
import { getIncomeTrend, getChargeItemsRanking, getBills, getProjects } from '../../services/api';
import type { Bill, Project } from '../../types';
import './Dashboard.css';

const Dashboard = () => {
    const [incomeTrend, setIncomeTrend] = useState<any[]>([]);
    const [ranking, setRanking] = useState<any[]>([]);
    const [recentBills, setRecentBills] = useState<Bill[]>([]);
    const [projects, setProjects] = useState<Project[]>([]);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            setIsLoading(true);
            try {
                const [trendData, rankingData, billsData, projectsData] = await Promise.all([
                    getIncomeTrend(),
                    getChargeItemsRanking(5),
                    getBills({ limit: 5 }),
                    getProjects()
                ]);

                // Format trend data for RevenueChart
                const formattedTrend = trendData.labels.map((label: number, index: number) => ({
                    name: `${label}月`,
                    income: trendData.data[index]
                }));
                setIncomeTrend(formattedTrend);

                // Format ranking data for ExpenseChart
                const formattedRanking = rankingData.map((item: any) => ({
                    name: item.item_name,
                    value: item.percentage
                }));
                setRanking(formattedRanking);

                setRecentBills(billsData || []);
                setProjects((projectsData.items || projectsData).slice(0, 3)); // Only show top 3
            } catch (error) {
                console.error('Failed to fetch dashboard data:', error);
            } finally {
                setIsLoading(false);
            }
        };

        fetchData();
    }, []);

    return (
        <div className="dashboard-container">
            {/* Headers row instead of stats grid */}


            {/* Main Content Grid */}
            <div className="dashboard-main-grid">
                {/* Revenue Trend Chart */}
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

                {/* Expense Distribution */}
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

            {/* Recent Bills & Project Status */}
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
                                            <td>¥{bill.amount?.toLocaleString()}</td>
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
                        <a href="/projects" className="btn-link">查看详情</a>
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
                            <div className="empty-placeholder">暂无项目数据</div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Dashboard;
