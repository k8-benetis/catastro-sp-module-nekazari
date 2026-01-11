# Catastro Spain Module for Nekazari

Spanish Cadastre integration module for the Nekazari agricultural platform.

## Features

- **Multi-region support**: Spain (State), Navarra, Euskadi
- **Reverse geocoding**: Query parcels by coordinates
- **Orion-LD sync**: Webhook receiver for AgriParcel entity synchronization
- **PostGIS cache**: Spatial queries for agricultural analytics

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/parcels` | GET | List tenant parcels |
| `/parcels` | POST | Create parcel |
| `/parcels/<id>` | GET | Get parcel |
| `/parcels/<id>` | PUT | Update parcel |
| `/parcels/<id>` | DELETE | Delete parcel |
| `/parcels/summary` | GET | Tenant statistics |
| `/parcels/query-by-coordinates` | GET | Reverse geocode from coordinates |
| `/orion/notify` | POST | Webhook for Orion-LD notifications |

## Requirements

- PostgreSQL with PostGIS extension
- Access to Orion-LD (for sync)
- Keycloak (for authentication)

## Environment Variables

```bash
POSTGRES_HOST=postgresql-service
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secret
POSTGRES_DB=nekazari
ORION_URL=http://orion-ld:1026
KEYCLOAK_URL=http://keycloak:8080
```

## Deployment

```bash
# Build image
docker build -t catastro-module-backend ./backend

# Deploy to Kubernetes
kubectl apply -f k8s/deployment.yaml
```

## License

AGPL-3.0
