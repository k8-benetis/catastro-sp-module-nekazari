import axios, { AxiosInstance } from 'axios';

export interface Parcel {
  id?: string;
  name?: string;
  geometry?: {
    type: 'Polygon';
    coordinates: number[][][];
  };
  municipality?: string;
  province?: string;
  cadastralReference?: string;
  cropType?: string;
  area?: number;
  notes?: string;
  category?: string;
  ndviEnabled?: boolean;
}

class ParcelApiService {
  private client: AxiosInstance;

  constructor() {
    // Use relative URL - the host will proxy to the correct service
    this.client = axios.create({
      baseURL: '/api',
      headers: {
        'Content-Type': 'application/ld+json',
      },
    });
  }

  async createParcel(parcel: Partial<Parcel>): Promise<Parcel> {
    // Generate entity ID
    const entityId = parcel.id || `urn:ngsi-ld:AgriParcel:${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    // Build NGSI-LD entity (same format as host platform)
    const entity: any = {
      id: entityId,
      type: 'AgriParcel',
      category: {
        type: 'Property',
        value: parcel.category || 'cadastral',
      },
    };

    // Add location (GeoProperty)
    if (parcel.geometry) {
      entity.location = {
        type: 'GeoProperty',
        value: {
          type: parcel.geometry.type || 'Polygon',
          coordinates: parcel.geometry.coordinates,
        },
      };
    }

    // Add name
    if (parcel.name) {
      entity.name = {
        type: 'Property',
        value: parcel.name,
      };
    }

    // Add municipality (only if not empty)
    if (parcel.municipality && parcel.municipality.trim() !== '') {
      entity.municipality = {
        type: 'Property',
        value: parcel.municipality,
      };
    }

    // Add province (only if not empty)
    if (parcel.province && parcel.province.trim() !== '') {
      entity.province = {
        type: 'Property',
        value: parcel.province,
      };
    }

    // Add cadastral reference
    if (parcel.cadastralReference) {
      entity.cadastralReference = {
        type: 'Property',
        value: parcel.cadastralReference,
      };
    }

    // Add crop type
    if (parcel.cropType) {
      entity.cropType = {
        type: 'Property',
        value: parcel.cropType,
      };
    }

    // Add area
    if (parcel.area !== undefined && parcel.area !== null) {
      entity.area = {
        type: 'Property',
        value: parcel.area,
      };
    }

    // Add NDVI enabled flag
    entity.ndviEnabled = {
      type: 'Property',
      value: parcel.ndviEnabled !== undefined ? parcel.ndviEnabled : true,
    };

    // Add notes
    if (parcel.notes) {
      entity.notes = {
        type: 'Property',
        value: parcel.notes,
      };
    }

    // Use the parcel API from the host platform (same as parcelApi.createParcel in host)
    // Context URL is typically https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld
    const response = await this.client.post('/ngsi-ld/v1/entities', entity, {
      headers: {
        'Content-Type': 'application/ld+json',
        'Link': `<https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"`,
      },
    });

    return response.data;
  }
}

export const parcelApi = new ParcelApiService();

