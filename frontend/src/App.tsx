import { Suspense, useEffect } from 'react';
import type { ReactNode } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import MainLayout from './components/layout/MainLayout';
import ProtectedRoute from './components/auth/ProtectedRoute';
import AdminRoute from './components/auth/AdminRoute';
import MenuRoute from './components/auth/MenuRoute';
import RouteTransitionFallback from './components/layout/RouteTransitionFallback';
import {
  Login,
  Dashboard,
  Bills,
  DepositRecords,
  PrepaymentRecords,
  ReceiptBills,
  Projects,
  ChargeItems,
  Reports,
  OACenterPage,
  ReportCenterPage,
  Settings,
  Users,
  MenuPermissions,
  Organizations,
  Account,
  CredentialsManager,
  APIManager,
  SyncSchedulesPage,
  VoucherTemplates,
  TemplateCategories,
  AccountingSubjects,
  Houses,
  Residents,
  Parks,
  Customers,
  Suppliers,
  TaxRates,
  KingdeeHouses,
  AuxiliaryDataPage,
  AuxiliaryDataCategoriesPage,
  AccountBookPage,
  BankAccountsPage,
  preloadAllRoutes,
} from './routes/lazyRoutes';

type RouteFallbackVariant = 'dashboard' | 'table' | 'detail' | 'settings' | 'login';

const RouteElement = ({
  children,
  fullscreen = false,
  variant = 'detail',
}: {
  children: ReactNode;
  fullscreen?: boolean;
  variant?: RouteFallbackVariant;
}) => (
  <Suspense fallback={<RouteTransitionFallback fullscreen={fullscreen} variant={variant} />}>
    {children}
  </Suspense>
);

function App() {
  useEffect(() => {
    const idleWindow = window as Window & {
      requestIdleCallback?: (callback: () => void, options?: { timeout: number }) => number;
      cancelIdleCallback?: (handle: number) => void;
    };

    let timeoutId: number | null = null;
    let idleId: number | null = null;

    const warmRoutes = () => {
      void preloadAllRoutes();
    };

    if (typeof idleWindow.requestIdleCallback === 'function') {
      idleId = idleWindow.requestIdleCallback(warmRoutes, { timeout: 1200 });
    } else {
      timeoutId = window.setTimeout(warmRoutes, 800);
    }

    return () => {
      if (idleId !== null && typeof idleWindow.cancelIdleCallback === 'function') {
        idleWindow.cancelIdleCallback(idleId);
      }
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, []);

  return (
    <Router>
      <Routes>
        <Route path="/login" element={<RouteElement fullscreen variant="login"><Login /></RouteElement>} />

        <Route element={<ProtectedRoute />}>
          <Route path="/" element={<MainLayout />}>
            <Route index element={<RouteElement variant="dashboard"><Dashboard /></RouteElement>} />
            <Route path="bills" element={<MenuRoute menuKey="/bills"><RouteElement variant="table"><Bills /></RouteElement></MenuRoute>} />
            <Route path="deposit-records" element={<MenuRoute menuKey="/deposit-records"><RouteElement variant="table"><DepositRecords /></RouteElement></MenuRoute>} />
            <Route path="prepayment-records" element={<MenuRoute menuKey="/prepayment-records"><RouteElement variant="table"><PrepaymentRecords /></RouteElement></MenuRoute>} />
            <Route path="receipt-bills" element={<MenuRoute menuKey="/receipt-bills"><RouteElement variant="table"><ReceiptBills /></RouteElement></MenuRoute>} />
            <Route path="houses" element={<MenuRoute menuKey="/houses"><RouteElement variant="table"><Houses /></RouteElement></MenuRoute>} />
            <Route path="residents" element={<MenuRoute menuKey="/residents"><RouteElement variant="table"><Residents /></RouteElement></MenuRoute>} />
            <Route path="parks" element={<MenuRoute menuKey="/parks"><RouteElement variant="table"><Parks /></RouteElement></MenuRoute>} />
            <Route path="oa-center" element={<MenuRoute menuKey="/oa-center"><RouteElement variant="dashboard"><OACenterPage /></RouteElement></MenuRoute>} />
            <Route path="report-center" element={<MenuRoute menuKey="/report-center"><RouteElement variant="dashboard"><ReportCenterPage /></RouteElement></MenuRoute>} />
            <Route path="reports" element={<Navigate to="/integrations/reporting" replace />} />
            <Route path="integrations" element={<Navigate to="/integrations/reporting" replace />} />
            <Route path="integrations/reports" element={<Navigate to="/integrations/reporting" replace />} />
            <Route path="integrations/reporting" element={<MenuRoute menuKey="/integrations/reporting" apiKey="reporting.manage"><RouteElement variant="dashboard"><Reports /></RouteElement></MenuRoute>} />
            <Route path="accounting-subjects" element={<MenuRoute menuKey="/accounting-subjects"><RouteElement variant="table"><AccountingSubjects /></RouteElement></MenuRoute>} />
            <Route path="customers" element={<MenuRoute menuKey="/customers"><RouteElement variant="table"><Customers /></RouteElement></MenuRoute>} />
            <Route path="suppliers" element={<MenuRoute menuKey="/suppliers"><RouteElement variant="table"><Suppliers /></RouteElement></MenuRoute>} />
            <Route path="tax-rates" element={<MenuRoute menuKey="/tax-rates"><RouteElement variant="table"><TaxRates /></RouteElement></MenuRoute>} />
            <Route path="kd-houses" element={<MenuRoute menuKey="/kd-houses"><RouteElement variant="table"><KingdeeHouses /></RouteElement></MenuRoute>} />
            <Route path="auxiliary-data" element={<MenuRoute menuKey="/auxiliary-data"><RouteElement variant="table"><AuxiliaryDataPage /></RouteElement></MenuRoute>} />
            <Route path="auxiliary-data-categories" element={<MenuRoute menuKey="/auxiliary-data-categories"><RouteElement variant="table"><AuxiliaryDataCategoriesPage /></RouteElement></MenuRoute>} />
            <Route path="account-books" element={<MenuRoute menuKey="/account-books"><RouteElement variant="table"><AccountBookPage /></RouteElement></MenuRoute>} />
            <Route path="bank-accounts" element={<MenuRoute menuKey="/bank-accounts"><RouteElement variant="table"><BankAccountsPage /></RouteElement></MenuRoute>} />
            <Route path="account" element={<RouteElement variant="settings"><Account /></RouteElement>} />
            <Route path="projects" element={<MenuRoute menuKey="/projects" apiKey="project.manage"><RouteElement variant="table"><Projects /></RouteElement></MenuRoute>} />
            <Route path="charge-items" element={<MenuRoute menuKey="/charge-items" apiKey="charge_item.manage"><RouteElement variant="table"><ChargeItems /></RouteElement></MenuRoute>} />
            <Route path="users" element={<MenuRoute menuKey="/users" apiKey="user.manage"><RouteElement variant="table"><Users /></RouteElement></MenuRoute>} />
            <Route path="organizations" element={<MenuRoute menuKey="/organizations" apiKey="organization.manage"><RouteElement variant="table"><Organizations /></RouteElement></MenuRoute>} />
            <Route path="integrations/credentials" element={<MenuRoute menuKey="/integrations/credentials" apiKey="credential.manage"><RouteElement variant="settings"><CredentialsManager /></RouteElement></MenuRoute>} />
            <Route path="integrations/apis" element={<MenuRoute menuKey="/integrations/apis" apiKey="api_registry.manage"><RouteElement variant="table"><APIManager /></RouteElement></MenuRoute>} />
            <Route path="integrations/sync-schedules" element={<MenuRoute menuKey="/integrations/sync-schedules" apiKey="sync_schedule.manage"><RouteElement variant="table"><SyncSchedulesPage /></RouteElement></MenuRoute>} />
            <Route path="vouchers/templates" element={<MenuRoute menuKey="/vouchers/templates" apiKey="voucher_template.manage"><RouteElement variant="settings"><VoucherTemplates /></RouteElement></MenuRoute>} />
            <Route path="vouchers/categories" element={<MenuRoute menuKey="/vouchers/categories" apiKey="voucher_template.manage"><RouteElement variant="settings"><TemplateCategories /></RouteElement></MenuRoute>} />
            <Route path="settings" element={<MenuRoute menuKey="/settings" apiKey="setting.manage"><RouteElement variant="settings"><Settings /></RouteElement></MenuRoute>} />

            <Route element={<AdminRoute />}>
              <Route path="menu-permissions" element={<RouteElement variant="settings"><MenuPermissions /></RouteElement>} />
            </Route>
          </Route>
        </Route>
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Router>
  );
}

export default App;
