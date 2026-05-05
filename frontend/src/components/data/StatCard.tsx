import classNames from 'classnames';
import type { LucideIcon } from 'lucide-react';
import './StatCard.css';

interface StatCardProps {
    title: string;
    value: string | number;
    change?: string;
    trend?: 'up' | 'down' | 'neutral';
    icon: LucideIcon;
    color: 'blue' | 'green' | 'orange' | 'purple' | 'red';
    loading?: boolean;
}

const StatCard = ({
    title,
    value,
    change,
    trend = 'neutral',
    icon: Icon,
    color,
    loading = false
}: StatCardProps) => {
    if (loading) {
        return (
            <div className="stat-card glass loading">
                <div className="stat-content">
                    <div className="skeleton title-skeleton"></div>
                    <div className="skeleton value-skeleton"></div>
                    <div className="skeleton change-skeleton"></div>
                </div>
                <div className="stat-icon-wrapper skeleton-icon"></div>
            </div>
        );
    }

    return (
        <div className="stat-card glass">
            <div className="stat-content">
                <p className="stat-title">{title}</p>
                <h3 className="stat-value">{value}</h3>
                {change && (
                    <div className={classNames('stat-change', trend)}>
                        <span>{change}</span>
                        <span className="trend-label">vs last month</span>
                    </div>
                )}
            </div>
            <div className={classNames('stat-icon-wrapper', color)}>
                <Icon size={24} />
            </div>
        </div>
    );
};

export default StatCard;
