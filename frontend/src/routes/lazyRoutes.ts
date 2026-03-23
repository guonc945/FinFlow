import { lazy } from 'react';
import type { ComponentType, LazyExoticComponent } from 'react';

type Loader<T extends ComponentType<any>> = () => Promise<{ default: T }>;
type LazyWithPreload<T extends ComponentType<any>> = LazyExoticComponent<T> & {
    preload: Loader<T>;
};

const lazyWithPreload = <T extends ComponentType<any>>(loader: Loader<T>): LazyWithPreload<T> => {
    const Component = lazy(loader) as LazyWithPreload<T>;
    Component.preload = loader;
    return Component;
};

export const Login = lazyWithPreload(() => import('../pages/auth/Login'));
export const Dashboard = lazyWithPreload(() => import('../pages/dashboard'));
export const Bills = lazyWithPreload(() => import('../pages/bills'));
export const DepositRecords = lazyWithPreload(() => import('../pages/deposit-records'));
export const PrepaymentRecords = lazyWithPreload(() => import('../pages/prepayment-records'));
export const ReceiptBills = lazyWithPreload(() => import('../pages/receipt-bills'));
export const Projects = lazyWithPreload(() => import('../pages/projects'));
export const ChargeItems = lazyWithPreload(() => import('../pages/charge-items'));
export const Reports = lazyWithPreload(() => import('../pages/reports'));
export const OACenterPage = lazyWithPreload(() => import('../pages/oa-center'));
export const ReportCenterPage = lazyWithPreload(() => import('../pages/report-center'));
export const Settings = lazyWithPreload(() => import('../pages/settings'));
export const Users = lazyWithPreload(() => import('../pages/users'));
export const Organizations = lazyWithPreload(() => import('../pages/organizations'));
export const Account = lazyWithPreload(() => import('../pages/account'));
export const CredentialsManager = lazyWithPreload(() => import('../pages/integrations/credentials'));
export const APIManager = lazyWithPreload(() => import('../pages/integrations/apis'));
export const SyncSchedulesPage = lazyWithPreload(() => import('../pages/integrations/sync-schedules'));
export const VoucherTemplates = lazyWithPreload(() => import('../pages/vouchers/VoucherTemplates'));
export const TemplateCategories = lazyWithPreload(() => import('../pages/vouchers/TemplateCategories'));
export const AccountingSubjects = lazyWithPreload(() => import('../pages/finance/AccountingSubjects'));
export const Houses = lazyWithPreload(() => import('../pages/houses'));
export const Residents = lazyWithPreload(() => import('../pages/residents'));
export const Parks = lazyWithPreload(() => import('../pages/parks'));
export const Customers = lazyWithPreload(() => import('../pages/finance/Customers'));
export const Suppliers = lazyWithPreload(() => import('../pages/finance/Suppliers'));
export const KingdeeHouses = lazyWithPreload(() => import('../pages/finance/KingdeeHouses'));
export const AuxiliaryDataPage = lazyWithPreload(() => import('../pages/finance/AuxiliaryData'));
export const AuxiliaryDataCategoriesPage = lazyWithPreload(() => import('../pages/finance/AuxiliaryDataCategories'));
export const AccountBookPage = lazyWithPreload(() => import('../pages/finance/AccountBook'));
export const BankAccountsPage = lazyWithPreload(() => import('../pages/finance/BankAccounts'));

const routePreloaders: Record<string, () => Promise<unknown>> = {
    '/login': Login.preload,
    '/': Dashboard.preload,
    '/bills': Bills.preload,
    '/deposit-records': DepositRecords.preload,
    '/prepayment-records': PrepaymentRecords.preload,
    '/receipt-bills': ReceiptBills.preload,
    '/houses': Houses.preload,
    '/residents': Residents.preload,
    '/parks': Parks.preload,
    '/reports': Reports.preload,
    '/oa-center': OACenterPage.preload,
    '/report-center': ReportCenterPage.preload,
    '/integrations/reports': Reports.preload,
    '/integrations/reporting': Reports.preload,
    '/accounting-subjects': AccountingSubjects.preload,
    '/customers': Customers.preload,
    '/suppliers': Suppliers.preload,
    '/kd-houses': KingdeeHouses.preload,
    '/auxiliary-data': AuxiliaryDataPage.preload,
    '/auxiliary-data-categories': AuxiliaryDataCategoriesPage.preload,
    '/account-books': AccountBookPage.preload,
    '/bank-accounts': BankAccountsPage.preload,
    '/projects': Projects.preload,
    '/charge-items': ChargeItems.preload,
    '/users': Users.preload,
    '/organizations': Organizations.preload,
    '/account': Account.preload,
    '/integrations': Reports.preload,
    '/integrations/credentials': CredentialsManager.preload,
    '/integrations/apis': APIManager.preload,
    '/integrations/sync-schedules': SyncSchedulesPage.preload,
    '/vouchers/templates': VoucherTemplates.preload,
    '/vouchers/categories': TemplateCategories.preload,
    '/settings': Settings.preload,
};

const normalizeRoutePath = (path: string) => {
    if (!path || path === '/') return '/';
    return path.replace(/\/+$/, '') || '/';
};

export const preloadRoute = async (path: string) => {
    const normalizedPath = normalizeRoutePath(path);
    const preload = routePreloaders[normalizedPath];
    if (!preload) return;

    try {
        await preload();
    } catch {
        return;
    }
};

export const preloadAllRoutes = async () => {
    await Promise.allSettled(Object.values(routePreloaders).map(preload => preload()));
};
