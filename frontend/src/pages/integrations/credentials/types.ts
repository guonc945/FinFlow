export interface ExternalApi {
    id: number;
    name: string;
    method: string;
    url_path: string;
    description: string;
    is_active: boolean;
}

export interface ExternalService {
    id: number;
    service_name: string;
    display_name: string;
    app_id: string;
    app_secret?: string;
    has_app_secret?: boolean;
    auth_url: string;
    base_url: string;
    auth_type: string;
    auth_method: string;
    auth_headers?: string;
    auth_body?: string;
    refresh_token?: string;
    is_active: boolean;



    expires_at: string;
    updated_at: string;
    apis: ExternalApi[];
}
