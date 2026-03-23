import { CheckCircle2, GitBranch, Network, Workflow } from 'lucide-react';
import './PlaceholderCenter.css';

const OACenterPage = () => {
    return (
        <div className="placeholder-center">
            <section className="placeholder-hero">
                <div className="placeholder-badge">
                    <Workflow size={14} />
                    泛微协同建设中
                </div>
                <h1 className="placeholder-title">泛微协同中心</h1>
                <p className="placeholder-description">
                    这里将作为泛微相关能力的统一入口，后续会逐步承接组织协同、流程审批、业务单据联动以及主数据同步等内容。
                    当前先保留独立导航位，方便后续功能持续扩展，而不打乱现有系统结构。
                </p>
                <div className="placeholder-points">
                    <span className="placeholder-point"><CheckCircle2 size={14} /> 已预留独立系统域</span>
                    <span className="placeholder-point"><CheckCircle2 size={14} /> 可承接审批与协同流程</span>
                    <span className="placeholder-point"><CheckCircle2 size={14} /> 可扩展跨系统联动能力</span>
                </div>
            </section>

            <section className="placeholder-grid">
                <article className="placeholder-card">
                    <div className="placeholder-card-icon">
                        <Workflow size={20} />
                    </div>
                    <h3>流程协同</h3>
                    <p>后续可接入审批流、待办事项、流程状态回写等泛微协同能力。</p>
                </article>
                <article className="placeholder-card">
                    <div className="placeholder-card-icon">
                        <GitBranch size={20} />
                    </div>
                    <h3>单据联动</h3>
                    <p>支持业务单据与泛微流程之间建立触发、回写、审批结果同步等关系。</p>
                </article>
                <article className="placeholder-card">
                    <div className="placeholder-card-icon">
                        <Network size={20} />
                    </div>
                    <h3>组织与主数据</h3>
                    <p>可逐步扩展组织架构、人员、部门、基础资料等异构系统数据同步入口。</p>
                </article>
            </section>

            <section className="placeholder-tip">
                当前该模块为预留阶段。后续新增泛微相关页面时，建议继续归入本模块下，保持“主系统分域”的导航结构稳定。
            </section>
        </div>
    );
};

export default OACenterPage;
