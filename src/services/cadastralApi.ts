import axios, { AxiosInstance } from 'axios';

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

class CadastralApiService {
  private client: AxiosInstance;

  constructor() {
    // Use relative URL - the host will proxy to the correct service
    this.client = axios.create({
      baseURL: '/api',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Add request interceptor to include auth token
    this.client.interceptors.request.use((config) => {
      // Token will be added by the host's axios instance
      return config;
    });
  }

  async queryByCoordinates(
    longitude: number,
    latitude: number,
    srs: string = '4326'
  ): Promise<CadastralData> {
    const response = await this.client.post('/cadastral-api/parcels/query-by-coordinates', {
      longitude,
      latitude,
      srs,
    });
    return response.data;
  }
}

export const cadastralApi = new CadastralApiService();

