import './RouteTransitionFallback.css';

type RouteTransitionVariant = 'dashboard' | 'table' | 'detail' | 'settings' | 'login';

const RouteTransitionFallback = ({
    fullscreen = false,
    variant = 'detail',
}: {
    fullscreen?: boolean;
    variant?: RouteTransitionVariant;
}) => {
    const renderBody = () => {
        switch (variant) {
            case 'dashboard':
                return (
                    <>
                        <div className="route-transition-header shimmer"></div>
                        <div className="route-transition-grid stats">
                            <div className="route-transition-card compact shimmer"></div>
                            <div className="route-transition-card compact shimmer"></div>
                            <div className="route-transition-card compact shimmer"></div>
                            <div className="route-transition-card compact shimmer"></div>
                        </div>
                        <div className="route-transition-grid charts">
                            <div className="route-transition-panel chart shimmer"></div>
                            <div className="route-transition-panel chart shimmer"></div>
                        </div>
                        <div className="route-transition-panel shimmer"></div>
                    </>
                );
            case 'table':
                return (
                    <>
                        <div className="route-transition-header shimmer"></div>
                        <div className="route-transition-toolbar shimmer"></div>
                        <div className="route-transition-table shimmer">
                            <div className="route-transition-table-head"></div>
                            <div className="route-transition-table-row"></div>
                            <div className="route-transition-table-row"></div>
                            <div className="route-transition-table-row"></div>
                            <div className="route-transition-table-row short"></div>
                        </div>
                    </>
                );
            case 'settings':
                return (
                    <>
                        <div className="route-transition-header shimmer"></div>
                        <div className="route-transition-grid split">
                            <div className="route-transition-sidebar shimmer"></div>
                            <div className="route-transition-stack">
                                <div className="route-transition-panel medium shimmer"></div>
                                <div className="route-transition-panel medium shimmer"></div>
                            </div>
                        </div>
                    </>
                );
            case 'login':
                return (
                    <div className="route-transition-login-card shimmer">
                        <div className="route-transition-login-logo"></div>
                        <div className="route-transition-login-title"></div>
                        <div className="route-transition-login-input"></div>
                        <div className="route-transition-login-input"></div>
                        <div className="route-transition-login-button"></div>
                    </div>
                );
            case 'detail':
            default:
                return (
                    <>
                        <div className="route-transition-header shimmer"></div>
                        <div className="route-transition-grid">
                            <div className="route-transition-card shimmer"></div>
                            <div className="route-transition-card shimmer"></div>
                            <div className="route-transition-card shimmer"></div>
                        </div>
                        <div className="route-transition-panel shimmer"></div>
                    </>
                );
        }
    };

    return (
        <div className={`route-transition-fallback ${fullscreen ? 'fullscreen' : ''}`}>
            <div className="route-transition-skeleton">
                {renderBody()}
            </div>
        </div>
    );
};

export default RouteTransitionFallback;
