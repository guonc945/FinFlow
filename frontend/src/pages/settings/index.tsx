
import { BookKey } from 'lucide-react';
import Variables from './Variables';
import '../../styles/ResourceConsole.css';

const Settings = () => {
    return (
        <div className="variables-page fade-in">
            <header className="page-header-pro">
                <div>
                    <h2 className="text-2xl font-bold text-slate-900 tracking-tight">系统配置中心</h2>
                    <p className="text-sm text-slate-500 mt-1">
                        统一维护系统级变量与运行参数，给公式、接口和业务规则提供底层配置能力。
                    </p>
                </div>
            </header>

            <div className="settings-resource-tabs mt-6">
                <button type="button" className="resource-tab-btn active">
                    <BookKey size={16} />
                    全局变量
                </button>
            </div>

            <VariableContent />
        </div>
    );
};

const VariableContent = () => (
    <section className="settings-tab-panel">
        <div className="settings-panel-intro">
            <strong>全局变量</strong>
            <p>适合维护系统运行时参数、默认值和业务上下文变量。</p>
        </div>
        <Variables />
    </section>
);

export default Settings;
