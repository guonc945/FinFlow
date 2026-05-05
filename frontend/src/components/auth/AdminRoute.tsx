import { Navigate, Outlet } from 'react-router-dom';
import { getAuthToken, getAuthUser } from '../../utils/authStorage';

const AdminRoute = () => {
    const token = getAuthToken();
    const user = getAuthUser<{ role?: string }>();
    const isAdmin = user?.role === 'admin';

    if (!token) {
        return <Navigate to="/login" replace />;
    }

    if (!isAdmin) {
        return <Navigate to="/" replace />;
    }

    return <Outlet />;
};

export default AdminRoute;
