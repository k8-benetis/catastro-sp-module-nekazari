#!/usr/bin/env python3
# =============================================================================
# Keycloak Authentication Middleware - Catastro Spain Module
# =============================================================================
# Copied from nekazari-public/services/common/keycloak_auth.py
# Self-contained authentication module for the catastro-spain module

import os
import json
import logging
import time
import hashlib
import hmac
import base64
from functools import wraps
from typing import Optional, Dict, Any

import jwt
from jwt import PyJWKClient
import requests
from flask import request, jsonify, g

logger = logging.getLogger(__name__)

# Configuration from environment
KEYCLOAK_URL = os.getenv('KEYCLOAK_URL', 'http://keycloak-service:8080')
KEYCLOAK_PUBLIC_URL = os.getenv('KEYCLOAK_PUBLIC_URL')
KEYCLOAK_HOSTNAME = os.getenv('KEYCLOAK_HOSTNAME')
KEYCLOAK_REALM = os.getenv('KEYCLOAK_REALM', 'nekazari')
HMAC_SECRET = os.getenv('HMAC_SECRET', os.getenv('JWT_SECRET', ''))

# JWKs URL
_keycloak_base_url = KEYCLOAK_URL.rstrip('/')
if _keycloak_base_url.endswith('/auth'):
    JWKS_URL = f"{_keycloak_base_url}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
else:
    JWKS_URL = f"{_keycloak_base_url}/auth/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"

# Cache for JWKs client
_jwks_client = None


class KeycloakAuthError(Exception):
    """Base exception for Keycloak authentication errors"""
    pass


class TokenValidationError(KeycloakAuthError):
    """Token validation failed"""
    pass


def get_jwks_client():
    """Get or create PyJWKClient with basic caching."""
    global _jwks_client
    if _jwks_client is None:
        try:
            logger.debug(f"Creating PyJWKClient with JWKS_URL: {JWKS_URL}")
            _jwks_client = PyJWKClient(JWKS_URL)
            logger.debug("PyJWKClient created successfully")
        except Exception as e:
            logger.error(f"Failed to create PyJWKClient: {e}")
            raise
    return _jwks_client


def validate_keycloak_token(token: str) -> Optional[Dict[str, Any]]:
    """Validate Keycloak JWT token using JWKs"""
    if not token:
        raise TokenValidationError("Token is empty")
    
    try:
        jwks_client = get_jwks_client()
        signing_key = jwks_client.get_signing_key_from_jwt(token)

        _keycloak_url_for_issuer = KEYCLOAK_URL.rstrip('/')
        if not _keycloak_url_for_issuer.endswith('/auth'):
            _keycloak_url_for_issuer = f"{_keycloak_url_for_issuer}/auth"
        expected_issuer = f"{_keycloak_url_for_issuer}/realms/{KEYCLOAK_REALM}"
        allowed_issuers = {expected_issuer}
        
        if KEYCLOAK_PUBLIC_URL:
            _public_url = KEYCLOAK_PUBLIC_URL.rstrip('/')
            if not _public_url.endswith('/auth'):
                _public_url = f"{_public_url}/auth"
            allowed_issuers.add(f"{_public_url}/realms/{KEYCLOAK_REALM}")
        
        if KEYCLOAK_HOSTNAME:
            allowed_issuers.add(f"https://{KEYCLOAK_HOSTNAME}/auth/realms/{KEYCLOAK_REALM}")
            allowed_issuers.add(f"http://{KEYCLOAK_HOSTNAME}/auth/realms/{KEYCLOAK_REALM}")

        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=['RS256', 'RS512'],
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_aud": False,
                "verify_iss": False,
            }
        )

        issuer = payload.get('iss')
        expected_issuer_suffix = f"/realms/{KEYCLOAK_REALM}"
        issuer_valid = (
            issuer in allowed_issuers or 
            (issuer and issuer.endswith(expected_issuer_suffix))
        )
        
        if not issuer_valid:
            logger.warning(f"Token issuer {issuer} not in allowed issuers")
            raise TokenValidationError("Invalid token issuer")

        logger.debug(f"Successfully validated token for user: {payload.get('preferred_username')}")
        return payload
        
    except jwt.ExpiredSignatureError:
        raise TokenValidationError("Token has expired")
    except jwt.InvalidSignatureError:
        raise TokenValidationError("Invalid token signature")
    except Exception as e:
        logger.error(f"Unexpected error validating token: {e}")
        raise TokenValidationError(f"Unexpected error: {e}")


def extract_tenant_id(payload: Dict[str, Any]) -> Optional[str]:
    """Extract tenant_id from JWT payload"""
    tenant_id = payload.get('tenant-id') or payload.get('tenant_id') or payload.get('tenant')
    
    if not tenant_id and 'groups' in payload:
        groups = payload.get('groups', [])
        if groups and isinstance(groups, list) and len(groups) > 0:
            tenant_groups = []
            for g in groups:
                if not g:
                    continue
                group_name = g[1:] if g.startswith('/') else g
                group_lower = group_name.lower()
                
                if group_lower == 'platform':
                    tenant_groups.append('platform')
                elif group_lower in ('default', 'offline_access', 'uma_authorization'):
                    continue
                elif group_lower.endswith(('administrators', 'admins')):
                    continue
                else:
                    tenant_groups.append(group_name)
            
            if tenant_groups:
                tenant_id = tenant_groups[0]

    if not tenant_id:
        logger.debug(f"No tenant_id found in payload claims: {list(payload.keys())}")
        return None
    
    # Basic normalization
    import re
    normalized = tenant_id.lower().replace('-', '_').replace(' ', '_')
    normalized = re.sub(r'[^a-z0-9_]', '', normalized)
    normalized = normalized.strip('_')
    return normalized if normalized else tenant_id


def verify_hmac_signature(signature_header: str, token: str, tenant_id: str) -> bool:
    """Verify HMAC signature for internal header propagation"""
    if not signature_header or not HMAC_SECRET:
        return True  # Don't block if not configured
    
    try:
        parts = signature_header.split(':')
        if len(parts) != 2:
            return False
        
        provided_signature, timestamp = parts
        current_timestamp = int(time.time())
        if abs(current_timestamp - int(timestamp)) > 300:
            return False
        
        message = f"{token}|{tenant_id}|{timestamp}"
        expected_signature = hmac.new(
            HMAC_SECRET.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(provided_signature, expected_signature)
        
    except Exception as e:
        logger.error(f"Error verifying HMAC signature: {e}")
        return False


def require_keycloak_auth(f):
    """Decorator to require Keycloak authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Missing or invalid authorization header'}), 401
        
        token = auth_header.split(' ')[1]
        trusted_tenant = request.headers.get('X-Tenant-ID')
        trust_api_gateway = os.getenv('TRUST_API_GATEWAY', 'true').lower() == 'true'
        
        try:
            if trust_api_gateway and trusted_tenant:
                logger.debug("Trusting API Gateway validation")
                try:
                    payload = jwt.decode(token, options={"verify_signature": False, "verify_exp": True})
                    tenant_id = trusted_tenant
                except jwt.ExpiredSignatureError:
                    return jsonify({'error': 'Token has expired'}), 401
                except Exception as e:
                    logger.warning(f"Token decode failed: {e}")
                    return jsonify({'error': 'Invalid token'}), 401
            else:
                payload = validate_keycloak_token(token)
                if not payload:
                    return jsonify({'error': 'Token validation failed'}), 401
                
                tenant_id = extract_tenant_id(payload)
                if not tenant_id:
                    logger.warning("No tenant_id in token")
                    return jsonify({'error': 'Tenant ID not found in token'}), 401
            
            hmac_signature = request.headers.get('X-Auth-Signature')
            if hmac_signature:
                if not verify_hmac_signature(hmac_signature, token, tenant_id):
                    return jsonify({'error': 'Invalid HMAC signature'}), 401
            
            g.current_user = payload
            g.tenant = tenant_id
            g.tenant_id = tenant_id
            g.user_id = payload.get('sub')
            g.username = payload.get('preferred_username')
            g.email = payload.get('email')
            g.roles = payload.get('realm_access', {}).get('roles', [])
            
            return f(*args, **kwargs)
            
        except TokenValidationError as e:
            logger.warning(f"Token validation error: {e}")
            return jsonify({'error': str(e)}), 401
        except Exception as e:
            logger.error(f"Unexpected error in auth decorator: {e}")
            return jsonify({'error': 'Internal server error'}), 500
    
    return decorated_function


def get_current_user() -> Optional[Dict[str, Any]]:
    """Get current user from Flask request context"""
    return getattr(g, 'current_user', None)


def get_current_tenant() -> Optional[str]:
    """Get current tenant from Flask request context"""
    return getattr(g, 'tenant', None) or getattr(g, 'tenant_id', None)


__all__ = [
    'KeycloakAuthError',
    'TokenValidationError',
    'validate_keycloak_token',
    'extract_tenant_id',
    'require_keycloak_auth',
    'get_current_user',
    'get_current_tenant',
]

