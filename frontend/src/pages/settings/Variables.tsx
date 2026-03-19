import VariableManager from './VariableManager';
import './Variables.css';

const GlobalVariables = () => {
    return (
        <div className="variables-page">
            <header className="page-header-pro">
                <div>
                    <h2 className="text-2xl font-bold text-slate-900 tracking-tight">全局变量管理</h2>
                    <p className="text-sm text-slate-500 mt-1">
                        这里仅维护全局变量；公式编辑改为在各业务字段内通过大编辑器直接完成。
                    </p>
                </div>
            </header>

            <VariableManager />
        </div>
    );
};

export default GlobalVariables;
