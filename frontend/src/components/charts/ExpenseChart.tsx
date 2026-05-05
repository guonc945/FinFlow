import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';

interface ExpenseChartProps {
    data?: { name: string; value: number }[];
    loading?: boolean;
}

type TooltipValue = number | string | undefined;

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

const ExpenseChart = ({ data = [], loading = false }: ExpenseChartProps) => {
    if (loading) {
        return (
            <div className="chart-loading">
                <div className="loading-spinner"></div>
                <p>加载中...</p>
            </div>
        );
    }

    if (data.length === 0) {
        return (
            <div className="chart-empty">
                <p>暂无数据</p>
            </div>
        );
    }

    return (
        <div style={{ width: '100%', height: 300 }}>
            <ResponsiveContainer>
                <PieChart>
                    <Pie
                        data={data}
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={80}
                        fill="#8884d8"
                        paddingAngle={5}
                        dataKey="value"
                    >
                        {data.map((_, index) => (
                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} stroke="none" />
                        ))}
                    </Pie>
                    <Tooltip
                        contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                        formatter={(value: TooltipValue) => [`${Number(value).toFixed(1)}%`, '占比']}
                    />
                    <Legend
                        verticalAlign="bottom"
                        height={36}
                        iconType="circle"
                        wrapperStyle={{ fontSize: '12px', color: '#64748b' }}
                    />
                </PieChart>
            </ResponsiveContainer>
        </div>
    );
};

export default ExpenseChart;
