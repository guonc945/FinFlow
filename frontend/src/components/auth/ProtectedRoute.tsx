import { Navigate, Outlet } from 'react-router-dom';
import { getAuthToken } from '../../utils/authStorage';

const ProtectedRoute = () => {
    const token = getAuthToken();

    // 如果没有token，则重定向到登录页
    if (!token) {
        return <Navigate to="/login" replace />;
    }

    return <Outlet />;
};

export default ProtectedRoute;
