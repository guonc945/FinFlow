import Sidebar from './Sidebar';
import Header from './Header';
import RouteTabs from './RouteTabs';

const MainLayout = () => {

    const getPageTitle = (pathname: string) => {
        const titles: Record<string, { title: string, subtitle?: string }> = {
            '/': { title: '首页仪表盘', subtitle: '系统运行正常，您可以在下方查看近期的收入趋势及账单动态' },
            '/bills': { title: '账单管理', subtitle: '所有应收/已收账单信息的全景视图' },
            '/charge-items': { title: '收费项目与规则管理', subtitle: '集中配置和管理各类收费项目及其计费规则' },
            '/houses': { title: '房屋管理 (马克)', subtitle: '马克系统内的房屋基础资产数据' },
            '/projects': { title: '项目与园区管理', subtitle: '定义并管理所有物业服务项目及关联区域' },
            '/residents': { title: '住户管理', subtitle: '维护业主、租户及家庭成员等核心人员信息' },
            '/parks': { title: '车位与车辆管理', subtitle: '统筹管理车位资源及授权车辆信息' },
            '/reports': { title: '报表分析', subtitle: '核心运营数据的深度洞察与可视化' },
            '/integrations/credentials': { title: '集成凭证配置', subtitle: '管理各外部系统认证参数与授权有效期' },
            '/integrations/apis': { title: '接口管理', subtitle: '集中管理外部系统 API 定义、调试与批量导入' },
            '/vouchers/templates': { title: '凭证模板中心', subtitle: '可视化设计与管理标准财务凭证生成模板' },
            '/accounting-subjects': { title: '会计科目管理', subtitle: '管理和同步来自外部系统的会计科目数据' },
            '/account-books': { title: '账簿管理 (金蝶)', subtitle: '管理和同步金蝶系统账簿档案' },
            '/auxiliary-data': { title: '辅助资料', subtitle: '管理基于金蝶配置辅助核算分类的具体资料档案' },
            '/auxiliary-data-categories': { title: '辅助资料分类', subtitle: '定义和管理财务常用的各类辅助核算维度' },
            '/customers': { title: '客户管理 (金蝶)', subtitle: '统一管理金蝶应收体系下的客户基础档案' },
            '/suppliers': { title: '供应商体系', subtitle: '统一管理金蝶应付体系下的供应商基础档案' },
            '/kd-houses': { title: '房号管理 (金蝶)', subtitle: '单向同步和检查金蝶房号明细数据' },
            '/bank-accounts': { title: '银行账户管理 (金蝶)', subtitle: '同步和管理金蝶系统中的银行账户基础档案' },
            '/settings': { title: '系统设置', subtitle: '全局系统参数与偏好配置' },
            '/users': { title: '用户与权限管理', subtitle: '内部员工账号、角色与系统访问控制' },
            '/organizations': { title: '组织架构管理', subtitle: '维护公司的多层级组织与部门结构' }
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
