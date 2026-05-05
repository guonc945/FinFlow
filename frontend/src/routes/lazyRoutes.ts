import { lazy } from 'react';
import type { ComponentType, LazyExoticComponent } from 'react';

type Loader<T extends ComponentType<object>> = () => Promise<{ default: T }>;
type LazyWithPreload<T extends ComponentType<object>> = LazyExoticComponent<T> & {
    preload: Loader<T>;
};

const lazyWithPreload = <T extends ComponentType<object>>(loader: Loader<T>): LazyWithPreload<T> => {
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
export const DataCenterIndex = lazyWithPreload(() => import('../pages/data-center'));
export const DataCenterConnectionsPage = lazyWithPreload(() => import('../pages/data-center/connections'));
export const DataCenterDatasetsPage = lazyWithPreload(() => import('../pages/data-center/datasets'));
export const DataCenterApplicationsPage = lazyWithPreload(() => import('../pages/data-center/applications'));
export const DataCenterDictionariesPage = lazyWithPreload(() => import('../pages/data-center/dictionaries'));
export const DataCenterCategoriesPage = lazyWithPreload(() => import('../pages/data-center/categories'));
export const OACenterPage = lazyWithPreload(() => import('../pages/oa-center'));
export const ReportCenterPage = lazyWithPreload(() => import('../pages/report-center'));
export const Settings = lazyWithPreload(() => import('../pages/settings'));
export const Users = lazyWithPreload(() => import('../pages/users'));
export const MenuPermissions = lazyWithPreload(() => import('../pages/menu-permissions'));
export const Organizations = lazyWithPreload(() => import('../pages/organizations'));
export const Account = lazyWithPreload(() => import('../pages/account'));
export const CredentialsManager = lazyWithPreload(() => import('../pages/integrations/credentials'));
export const APIManager = lazyWithPreload(() => import('../pages/integrations/apis'));
export const DataSyncSchedulesPage = lazyWithPreload(() => import('../pages/integrations/sync-schedules'));
export const VoucherPushSchedulesPage = lazyWithPreload(() => import('../pages/integrations/voucher-push-schedules'));
export const VoucherTemplates = lazyWithPreload(() => import('../pages/vouchers/VoucherTemplates'));
export const TemplateCategories = lazyWithPreload(() => import('../pages/vouchers/TemplateCategories'));
export const AccountingSubjects = lazyWithPreload(() => import('../pages/finance/AccountingSubjects'));
export const Houses = lazyWithPreload(() => import('../pages/houses'));
export const Residents = lazyWithPreload(() => import('../pages/residents'));
export const Parks = lazyWithPreload(() => import('../pages/parks'));
export const Customers = lazyWithPreload(() => import('../pages/finance/Customers'));
export const Suppliers = lazyWithPreload(() => import('../pages/finance/Suppliers'));
export const TaxRates = lazyWithPreload(() => import('../pages/finance/TaxRates'));
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
    '/integrations/data-center': DataCenterIndex.preload,
    '/integrations/data-center/connections': DataCenterConnectionsPage.preload,
    '/integrations/data-center/datasets': DataCenterDatasetsPage.preload,
    '/integrations/data-center/applications': DataCenterApplicationsPage.preload,
    '/integrations/data-center/dictionaries': DataCenterDictionariesPage.preload,
    '/integrations/data-center/categories': DataCenterCategoriesPage.preload,
    '/oa-center': OACenterPage.preload,
    '/report-center': ReportCenterPage.preload,
    '/integrations/reports': Reports.preload,
    '/integrations/reporting': Reports.preload,
    '/accounting-subjects': AccountingSubjects.preload,
    '/customers': Customers.preload,
    '/suppliers': Suppliers.preload,
    '/tax-rates': TaxRates.preload,
    '/kd-houses': KingdeeHouses.preload,
    '/auxiliary-data': AuxiliaryDataPage.preload,
    '/auxiliary-data-categories': AuxiliaryDataCategoriesPage.preload,
    '/account-books': AccountBookPage.preload,
    '/bank-accounts': BankAccountsPage.preload,
    '/projects': Projects.preload,
    '/charge-items': ChargeItems.preload,
    '/users': Users.preload,
    '/menu-permissions': MenuPermissions.preload,
    '/organizations': Organizations.preload,
    '/account': Account.preload,
    '/integrations': DataCenterIndex.preload,
    '/integrations/credentials': CredentialsManager.preload,
    '/integrations/apis': APIManager.preload,
    '/integrations/data-sync-schedules': DataSyncSchedulesPage.preload,
    '/integrations/voucher-push-schedules': VoucherPushSchedulesPage.preload,
    '/integrations/sync-schedules': DataSyncSchedulesPage.preload,
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
