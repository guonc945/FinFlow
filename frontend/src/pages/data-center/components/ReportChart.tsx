import { useMemo } from 'react';
import {
    Bar,
    BarChart,
    CartesianGrid,
    Cell,
    Legend,
    Line,
    LineChart,
    Pie,
    PieChart,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from 'recharts';
import type { ReportChartConfig } from '../types';
import { buildChartData } from '../utils';

const COLORS = ['#0f766e', '#0ea5e9', '#f59e0b', '#ef4444', '#8b5cf6', '#14b8a6', '#ec4899'];

type ReportChartProps = {
    rows: Record<string, unknown>[];
    chart?: ReportChartConfig;
};

export default function ReportChart({ rows, chart }: ReportChartProps) {
    const data = useMemo(() => buildChartData(rows, chart), [rows, chart]);

    if (!chart?.enabled) {
        return <div className="empty-box">为数据应用配置图表后，这里会显示可视化结果。</div>;
    }

    if (!data.length) {
        return <div className="empty-box">当前筛选结果没有可用于绘图的数据。</div>;
    }

    return (
        <div className="chart-stage">
            <ResponsiveContainer>
                {chart.chart_type === 'pie' ? (
                    <PieChart>
                        <Pie data={data} dataKey="value" nameKey="name" outerRadius={96}>
                            {data.map((_, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip />
                        <Legend />
                    </PieChart>
                ) : chart.chart_type === 'line' ? (
                    <LineChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="name" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Line type="monotone" dataKey="value" stroke="#0ea5e9" strokeWidth={3} />
                    </LineChart>
                ) : (
                    <BarChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="name" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Bar dataKey="value" fill="#0f766e" radius={[8, 8, 0, 0]} />
                    </BarChart>
                )}
            </ResponsiveContainer>
        </div>
    );
}
