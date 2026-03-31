import { useCallback } from 'react';
import Sidebar from './Sidebar';
import Header from './Header';
import RouteTabs from './RouteTabs';

const MainLayout = () => {
    const getPageTitle = useCallback((pathname: string) => {
        const titles: Record<string, { title: string; subtitle?: string }> = {
            '/': { title: '首页仪表盘', subtitle: '查看系统运行状态与核心业务概览' },
            '/bills': { title: '运营账单', subtitle: '马克业务中运营账单的统一查询与处理入口' },
            '/deposit-records': { title: '押金管理', subtitle: '马克业务中押金收取、退还与关联情况总览' },
            '/prepayment-records': { title: '预存款管理', subtitle: '马克业务中预存款充值、退款与结转情况总览' },
            '/receipt-bills': { title: '收款单据', subtitle: '马克业务中收款明细与到账情况的统一入口' },
            '/charge-items': { title: '收费项目', subtitle: '维护马克业务基础资料中的收费项目与计费规则' },
            '/houses': { title: '房屋管理', subtitle: '维护马克业务基础资料中的房屋档案信息' },
            '/projects': { title: '园区管理', subtitle: '维护马克业务基础资料中的园区与项目档案' },
            '/residents': { title: '住户管理', subtitle: '维护马克业务基础资料中的业主、租户与住户信息' },
            '/parks': { title: '车位管理', subtitle: '维护马克业务基础资料中的车位与车辆信息' },
            '/reports': { title: '报表设计', subtitle: '在集成中心配置数据连接、数据集与报表定义' },
            '/oa-center': { title: '泛微协同', subtitle: '作为泛微相关能力的独立入口，承接后续协同与流程集成页面' },
            '/report-center': { title: '报表中心', subtitle: '作为最终报表查看、分析与汇总展示的统一入口' },
            '/integrations/reporting': { title: '报表设计', subtitle: '在集成中心配置数据连接、数据集与报表定义' },
            '/integrations/sync-schedules': { title: '同步计划', subtitle: '独立管理马克与金蝶数据同步的定时计划、执行频率与运行记录' },
            '/integrations/credentials': { title: '接口认证', subtitle: '管理跨系统接口认证、授权信息与连接配置' },
            '/integrations/apis': { title: '接口管理', subtitle: '维护外部系统接口定义、调试参数与接入能力' },
            '/vouchers/templates': { title: '财务凭证模板', subtitle: '维护金蝶财务凭证模板规则与生成逻辑' },
            '/vouchers/categories': { title: '财务凭证分类', subtitle: '维护金蝶财务凭证模板的分类结构与归属关系' },
            '/accounting-subjects': { title: '会计科目', subtitle: '管理金蝶财务档案中的会计科目资料' },
            '/account-books': { title: '账簿管理', subtitle: '管理与同步金蝶财务档案中的账簿信息' },
            '/auxiliary-data': { title: '辅助资料', subtitle: '管理金蝶财务档案中的辅助核算资料' },
            '/auxiliary-data-categories': { title: '辅助资料分类', subtitle: '维护金蝶财务档案中的辅助核算分类体系' },
            '/customers': { title: '客户管理', subtitle: '管理金蝶财务档案中的客户主数据' },
            '/suppliers': { title: '供应商管理', subtitle: '管理金蝶财务档案中的供应商主数据' },
            '/kd-houses': { title: '金蝶房号', subtitle: '管理与同步金蝶财务档案中的房号资料' },
            '/bank-accounts': { title: '银行账户', subtitle: '管理与同步金蝶财务档案中的银行账户资料' },
            '/settings': { title: '系统设置', subtitle: '维护系统级参数、默认规则与运行配置' },
            '/account': { title: '个人设置', subtitle: '维护个人资料、密码与个人偏好设置' },
            '/users': { title: '用户管理', subtitle: '管理系统用户、角色与访问权限' },
            '/menu-permissions': { title: '菜单权限', subtitle: '按角色配置系统菜单可见范围，统一管理前台导航访问入口' },
            '/organizations': { title: '组织管理', subtitle: '维护组织架构、层级关系与归属信息' },
        };

        return titles[pathname] || { title: '控制台' };
    }, []);

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
