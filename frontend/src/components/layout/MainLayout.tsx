import Sidebar from './Sidebar';
import Header from './Header';
import RouteTabs from './RouteTabs';

const MainLayout = () => {
    const getPageTitle = (pathname: string) => {
        const titles: Record<string, { title: string; subtitle?: string }> = {
            '/': { title: '首页仪表盘', subtitle: '系统运行状态与核心业务概览' },
            '/bills': { title: '运营账单', subtitle: '查看全部应收、已收与账单明细信息' },
            '/deposit-records': { title: '押金管理', subtitle: '集中查看押金收取、退还与关联情况' },
            '/receipt-bills': { title: '收款账单', subtitle: '统一查看来自业务系统的收款明细' },
            '/charge-items': { title: '收费项目', subtitle: '维护收费项目与相关规则配置' },
            '/houses': { title: '房屋管理', subtitle: '查看业务系统中的房屋基础资料' },
            '/projects': { title: '项目管理', subtitle: '维护园区与项目基础信息' },
            '/residents': { title: '住户管理', subtitle: '维护业主、租户及住户信息' },
            '/parks': { title: '车位管理', subtitle: '维护车位与车辆相关信息' },
            '/reports': { title: '报表分析', subtitle: '查看核心业务统计与分析数据' },
            '/integrations/credentials': { title: '凭证配置', subtitle: '管理外部系统认证凭据与授权信息' },
            '/integrations/apis': { title: '接口管理', subtitle: '维护外部系统 API 定义与调试配置' },
            '/vouchers/templates': { title: '凭证模板中心', subtitle: '可视化设计与维护凭证生成模板' },
            '/vouchers/categories': { title: '模板分类', subtitle: '管理凭证模板的分类树结构与所属关系' },
            '/accounting-subjects': { title: '会计科目管理', subtitle: '同步与管理会计科目基础资料' },
            '/account-books': { title: '账簿管理', subtitle: '管理与同步金蝶账簿档案' },
            '/auxiliary-data': { title: '辅助资料', subtitle: '管理财务辅助核算资料' },
            '/auxiliary-data-categories': { title: '辅助资料分类', subtitle: '维护辅助核算分类与维度' },
            '/customers': { title: '客户管理', subtitle: '管理客户基础档案信息' },
            '/suppliers': { title: '供应商管理', subtitle: '管理供应商基础档案信息' },
            '/kd-houses': { title: '金蝶房号管理', subtitle: '同步与查看金蝶房号档案' },
            '/bank-accounts': { title: '银行账户管理', subtitle: '同步与管理金蝶银行账户档案' },
            '/settings': { title: '系统设置', subtitle: '维护全局系统参数与偏好' },
            '/account': { title: '个人设置', subtitle: '维护个人资料、密码与偏好设置' },
            '/users': { title: '用户管理', subtitle: '管理用户、角色与访问权限' },
            '/organizations': { title: '组织管理', subtitle: '维护组织架构与层级关系' },
        };

        return titles[pathname] || { title: '控制台' };
    };

    return (
        <div className="app-wrapper">
            <Sidebar />
            <div className="main-content">
                <Header />
                <RouteTabs getPageTitle={getPageTitle} />
            </div>
        </div>
    );
};

export default MainLayout;
