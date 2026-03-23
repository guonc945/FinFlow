import { BarChart3, FileSpreadsheet, LayoutPanelTop, PieChart } from 'lucide-react';
import '../oa-center/PlaceholderCenter.css';

const ReportCenterPage = () => {
    return (
        <div className="placeholder-center">
            <section className="placeholder-hero">
                <div className="placeholder-badge">
                    <BarChart3 size={14} />
                    报表中心规划中
                </div>
                <h1 className="placeholder-title">报表中心</h1>
                <p className="placeholder-description">
                    报表中心将作为最终报表消费与分析展示入口，与当前位于集成中心内的“报表设计”保持区分。
                    设计页负责配置，报表中心负责查看、汇总、分析和分发，二者语义独立。
                </p>
                <div className="placeholder-points">
                    <span className="placeholder-point"><BarChart3 size={14} /> 面向报表查看与分析</span>
                    <span className="placeholder-point"><LayoutPanelTop size={14} /> 与报表设计职责分离</span>
                    <span className="placeholder-point"><FileSpreadsheet size={14} /> 可承接经营分析与专题报表</span>
                </div>
            </section>

            <section className="placeholder-grid">
                <article className="placeholder-card">
                    <div className="placeholder-card-icon">
                        <PieChart size={20} />
                    </div>
                    <h3>经营分析</h3>
                    <p>适合承接收费、收款、预存款、押金、凭证等主题分析看板。</p>
                </article>
                <article className="placeholder-card">
                    <div className="placeholder-card-icon">
                        <FileSpreadsheet size={20} />
                    </div>
                    <h3>专题报表</h3>
                    <p>支持后续接入业务专题报表、财务专题报表和跨系统汇总报表。</p>
                </article>
                <article className="placeholder-card">
                    <div className="placeholder-card-icon">
                        <LayoutPanelTop size={20} />
                    </div>
                    <h3>统一展示入口</h3>
                    <p>后续所有正式报表页面建议从这里进入，避免与配置类页面混杂在一起。</p>
                </article>
            </section>

            <section className="placeholder-tip">
                当前这里先作为独立占位页保留。后续若上线正式报表，可继续沿用本一级菜单，避免影响现有集成配置路径。
            </section>
        </div>
    );
};

export default ReportCenterPage;
