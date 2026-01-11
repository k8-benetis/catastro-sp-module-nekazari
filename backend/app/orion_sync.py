# =============================================================================
# Orion-LD Synchronization Functions
# =============================================================================
# Functions to sync AgriParcel entities from Orion-LD to PostGIS

import logging
import json
import psycopg2
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def extract_ngsi_ld_value(attribute: Any) -> Any:
    """
    Extract value from NGSI-LD attribute format
    
    NGSI-LD format: {"type": "Property", "value": "actual_value"}
    or {"type": "Relationship", "object": "urn:ngsi-ld:..."}
    """
    if isinstance(attribute, dict):
        if 'value' in attribute:
            return attribute['value']
        elif 'object' in attribute:
            return attribute['object']
    return attribute

def extract_tenant_from_entity(entity: Dict[str, Any]) -> Optional[str]:
    """
    Extract tenant_id from NGSI-LD entity
    
    Looks for tenant in multiple possible locations:
    - entity['tenant']['value']
    - entity['tenantId']['value']
    - entity['@context'] metadata
    """
    # Try direct tenant attribute
    if 'tenant' in entity:
        return extract_ngsi_ld_value(entity['tenant'])
    
    if 'tenantId' in entity:
        return extract_ngsi_ld_value(entity['tenantId'])
    
    # Fallback: extract from entity ID if it contains tenant info
    # Example: urn:ngsi-ld:AgriParcel:tenant-abc:parcel-123
    entity_id = entity.get('id', '')
    parts = entity_id.split(':')
    if len(parts) >= 4:
        # Assume format: urn:ngsi-ld:Type:tenant:id
        return parts[3]
    
    logger.warning(f"Could not extract tenant from entity {entity_id}")
    return None

def sync_parcel_to_postgres(
    entity_id: str,
    tenant_id: str,
    location: Dict[str, Any],
    category: str,
    ref_parent: Optional[str],
    full_entity: Dict[str, Any],
    postgres_url: str
) -> bool:
    """
    Synchronize an AgriParcel entity from Orion-LD to PostgreSQL
    
    Args:
        entity_id: NGSI-LD entity ID (e.g., urn:ngsi-ld:AgriParcel:001)
        tenant_id: Tenant identifier
        location: GeoProperty value with geometry
        category: 'cadastral' or 'managementZone'
        ref_parent: Parent parcel ID (for management zones)
        full_entity: Complete NGSI-LD entity for extracting other attributes
        postgres_url: PostgreSQL connection string
    
    Returns:
        True if sync successful, False otherwise
    """
    conn = None
    cur = None
    
    try:
        # Extract geometry
        geometry_type = location.get('type')
        coordinates = location.get('coordinates')
        
        if geometry_type != 'Polygon':
            logger.warning(f"Skipping non-Polygon geometry for {entity_id}: {geometry_type}")
            return False
        
        if not coordinates:
            logger.error(f"Missing coordinates for {entity_id}")
            return False
        
        geometry_json = json.dumps({
            'type': 'Polygon',
            'coordinates': coordinates
        })
        
        # Extract other attributes
        cadastral_ref = extract_ngsi_ld_value(full_entity.get('cadastralReference'))
        municipality = extract_ngsi_ld_value(full_entity.get('municipality', {}))
        province = extract_ngsi_ld_value(full_entity.get('province', {}))
        crop_type = extract_ngsi_ld_value(full_entity.get('cropType', {})) or 'unknown'
        ndvi_enabled = extract_ngsi_ld_value(full_entity.get('ndviEnabled', {}))
        if ndvi_enabled is None:
            ndvi_enabled = True
        
        # Connect to database
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        
        # UPSERT (insert or update on conflict)
        cur.execute("""
            INSERT INTO cadastral_parcels (
                orion_entity_id,
                tenant_id,
                category,
                ref_parent,
                cadastral_reference,
                municipality,
                province,
                crop_type,
                geometry,
                ndvi_enabled,
                is_active
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                ST_GeomFromGeoJSON(%s),
                %s, true
            )
            ON CONFLICT (orion_entity_id) DO UPDATE SET
                category = EXCLUDED.category,
                ref_parent = EXCLUDED.ref_parent,
                cadastral_reference = EXCLUDED.cadastral_reference,
                municipality = EXCLUDED.municipality,
                province = EXCLUDED.province,
                crop_type = EXCLUDED.crop_type,
                geometry = EXCLUDED.geometry,
                ndvi_enabled = EXCLUDED.ndvi_enabled,
                updated_at = NOW()
            RETURNING id
        """, (
            entity_id,
            tenant_id,
            category,
            ref_parent,
            cadastral_ref,
            municipality,
            province,
            crop_type,
            geometry_json,
            ndvi_enabled
        ))
        
        result = cur.fetchone()
        conn.commit()
        
        parcel_id = result[0] if result else None
        logger.info(f"✅ Synced parcel {entity_id} to PostgreSQL (ID: {parcel_id})")
        return True
        
    except psycopg2.Error as e:
        logger.error(f"❌ PostgreSQL error syncing {entity_id}: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"❌ Error syncing {entity_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def delete_parcel_from_postgres(entity_id: str, postgres_url: str) -> bool:
    """
    Soft delete a parcel from PostgreSQL (set is_active = false)
    
    Args:
        entity_id: NGSI-LD entity ID
        postgres_url: PostgreSQL connection string
    
    Returns:
        True if deletion successful, False otherwise
    """
    conn = None
    cur = None
    
    try:
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE cadastral_parcels
            SET is_active = false, updated_at = NOW()
            WHERE orion_entity_id = %s
            RETURNING id
        """, (entity_id,))
        
        result = cur.fetchone()
        conn.commit()
        
        if result:
            logger.info(f"✅ Soft deleted parcel {entity_id} from PostgreSQL")
            return True
        else:
            logger.warning(f"⚠️ Parcel {entity_id} not found for deletion")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error deleting {entity_id}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
