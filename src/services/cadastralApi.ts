import { NKZClient } from '@nekazari/sdk';

export interface CadastralData {
  cadastralReference: string;
  municipality: string;
  province: string;
  address: string;
  coordinates: { lon: number; lat: number };
  region: 'spain' | 'navarra' | 'euskadi';
  geometry?: {
    type: 'Polygon';
    coordinates: number[][][];
  };
}

// Helper to get auth token from Keycloak or localStorage
const getAuthToken = (): string | null => {
  if (typeof window === 'undefined') return null;

  // Try Keycloak instance first
  const keycloakInstance = (window as any).keycloak;
  if (keycloakInstance && keycloakInstance.token) {
    return keycloakInstance.token;
  }

  // Fallback to localStorage
  return localStorage.getItem('auth_token');
};

// Helper to get tenant ID from token or default
const getTenantId = (): string | null => {
  const token = getAuthToken();
  if (!token) return null;

  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const jsonPayload = decodeURIComponent(
      window.atob(base64)
        .split('')
        .map((c) => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2))
        .join('')
    );
    const decoded = JSON.parse(jsonPayload);
    return decoded['tenant-id'] || decoded.tenant_id || decoded.tenantId || decoded.tenant || null;
  } catch (e) {
    console.warn('[CadastralAPI] Failed to decode token for tenant extraction', e);
    return null;
  }
};

// Get API URL from runtime config
const getApiUrl = (): string => {
  if (typeof window !== 'undefined') {
    // 1. Use host's runtime config if available
    if ((window as any).__ENV__?.API_URL) {
      return (window as any).__ENV__.API_URL;
    }
    // 2. Derive from current origin: nekazari.{domain} → nkz.{domain}
    const origin = window.location.origin;
    if (origin.includes('nekazari.')) {
      return origin.replace('nekazari.', 'nkz.');
    }
    // 3. Localhost fallback for dev
    return origin;
  }
  return '';
};

class CadastralApiService {
  private client: NKZClient;

  constructor() {
    this.client = new NKZClient({
      baseUrl: `${getApiUrl()}/api/cadastral-api`,
      getToken: getAuthToken,
      getTenantId: getTenantId,
    });
  }

  async queryByCoordinates(
    longitude: number,
    latitude: number,
    srs: string = '4326'
  ): Promise<CadastralData> {
    const response = await this.client.post<CadastralData>('/parcels/query-by-coordinates', {
      longitude,
      latitude,
      srs,
    });
    return response;
  }
}

export const cadastralApi = new CadastralApiService();

