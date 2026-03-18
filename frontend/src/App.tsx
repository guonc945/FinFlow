import { Suspense, useEffect } from 'react';
import type { ReactNode } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import MainLayout from './components/layout/MainLayout';
import ProtectedRoute from './components/auth/ProtectedRoute';
import AdminRoute from './components/auth/AdminRoute';
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
  Settings,
  Users,
  Organizations,
  Account,
  CredentialsManager,
  APIManager,
  VoucherTemplates,
  TemplateCategories,
  AccountingSubjects,
  Houses,
  Residents,
  Parks,
  Customers,
  Suppliers,
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
            <Route path="bills" element={<RouteElement variant="table"><Bills /></RouteElement>} />
            <Route path="deposit-records" element={<RouteElement variant="table"><DepositRecords /></RouteElement>} />
            <Route path="prepayment-records" element={<RouteElement variant="table"><PrepaymentRecords /></RouteElement>} />
            <Route path="receipt-bills" element={<RouteElement variant="table"><ReceiptBills /></RouteElement>} />
            <Route path="houses" element={<RouteElement variant="table"><Houses /></RouteElement>} />
            <Route path="residents" element={<RouteElement variant="table"><Residents /></RouteElement>} />
            <Route path="parks" element={<RouteElement variant="table"><Parks /></RouteElement>} />
            <Route path="reports" element={<RouteElement variant="dashboard"><Reports /></RouteElement>} />
            <Route path="accounting-subjects" element={<RouteElement variant="table"><AccountingSubjects /></RouteElement>} />
            <Route path="customers" element={<RouteElement variant="table"><Customers /></RouteElement>} />
            <Route path="suppliers" element={<RouteElement variant="table"><Suppliers /></RouteElement>} />
            <Route path="kd-houses" element={<RouteElement variant="table"><KingdeeHouses /></RouteElement>} />
            <Route path="auxiliary-data" element={<RouteElement variant="table"><AuxiliaryDataPage /></RouteElement>} />
            <Route path="auxiliary-data-categories" element={<RouteElement variant="table"><AuxiliaryDataCategoriesPage /></RouteElement>} />
            <Route path="account-books" element={<RouteElement variant="table"><AccountBookPage /></RouteElement>} />
            <Route path="bank-accounts" element={<RouteElement variant="table"><BankAccountsPage /></RouteElement>} />
            <Route path="account" element={<RouteElement variant="settings"><Account /></RouteElement>} />

            <Route element={<AdminRoute />}>
              <Route path="projects" element={<RouteElement variant="table"><Projects /></RouteElement>} />
              <Route path="charge-items" element={<RouteElement variant="table"><ChargeItems /></RouteElement>} />
              <Route path="users" element={<RouteElement variant="table"><Users /></RouteElement>} />
              <Route path="organizations" element={<RouteElement variant="table"><Organizations /></RouteElement>} />
              <Route path="integrations" element={<Navigate to="/integrations/credentials" replace />} />
              <Route path="integrations/credentials" element={<RouteElement variant="settings"><CredentialsManager /></RouteElement>} />
              <Route path="integrations/apis" element={<RouteElement variant="table"><APIManager /></RouteElement>} />
              <Route path="vouchers/templates" element={<RouteElement variant="settings"><VoucherTemplates /></RouteElement>} />
              <Route path="vouchers/categories" element={<RouteElement variant="settings"><TemplateCategories /></RouteElement>} />
              <Route path="settings" element={<RouteElement variant="settings"><Settings /></RouteElement>} />
            </Route>
          </Route>
        </Route>
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Router>
  );
}

export default App;
