import type { DataCenterTabKey } from '../types';
import { TAB_META } from '../config';

type DataCenterHeroProps = {
    activeTab: DataCenterTabKey;
    canManageReporting: boolean;
    onSelectTab: (tab: DataCenterTabKey) => void;
};

export default function DataCenterHero({
    activeTab,
    canManageReporting,
    onSelectTab,
}: DataCenterHeroProps) {
    return (
        <section className="card glass reporting-hero">
            <div>
                <div className="reporting-eyebrow">Data Center</div>
                <h2>数据中心</h2>
                <p className="reporting-copy">
                    数据中心负责统一管理外部数据库连接、SQL 数据集模型和面向业务的数据应用。
                    这里不再只是“报表设计”，而是完整的数据接入、建模、复用与交付工作台。
                </p>
            </div>
            <div className="reporting-overview-grid">
                {TAB_META.filter((item) => canManageReporting || item.key === 'reports').map((item) => (
                    <button
                        key={item.key}
                        type="button"
                        className={`overview-card ${activeTab === item.key ? 'active' : ''}`}
                        onClick={() => onSelectTab(item.key)}
                    >
                        <span className="overview-card-icon">
                            <item.icon size={18} />
                        </span>
                        <span className="overview-card-content">
                            <span className="overview-card-label">{item.title}</span>
                            <span className="overview-card-title">{item.label}</span>
                            <span className="overview-card-detail">{item.detail}</span>
                        </span>
                    </button>
                ))}
            </div>
        </section>
    );
}
