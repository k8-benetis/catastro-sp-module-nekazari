#!/usr/bin/env python3
# =============================================================================
# Task Queue Module - Redis Streams Integration
# =============================================================================
# Copied from nekazari-public/services/task-queue/task_queue.py
# Self-contained task queue module for the catastro-spain module

import os
import json
import logging
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, asdict
from urllib.parse import quote

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("Redis not available, task queue will be disabled")

logger = logging.getLogger(__name__)

# Redis configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis-service:6379')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', os.getenv('REDIS_PASS'))

# Redis connection pool
_redis_pool = None


@dataclass
class Task:
    """Task structure for queue"""
    id: str
    tenant_id: str
    task_type: str
    payload: Dict[str, Any]
    status: str = 'pending'
    retry_count: int = 0
    max_retries: int = 3
    created_at: str = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()


class TaskQueue:
    """Redis Streams based task queue"""
    
    def __init__(self, stream_name: str = 'task_queue'):
        self.stream_name = stream_name
        self.redis_client = None
        
        if REDIS_AVAILABLE:
            try:
                self.redis_client = self._get_redis_client()
                if self.redis_client:
                    logger.info(f"Task queue initialized: {stream_name}")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self.redis_client = None
    
    def _clean_none_values(self, data):
        """Recursively remove None values from dict/list for Redis compatibility"""
        if isinstance(data, dict):
            return {k: self._clean_none_values(v) for k, v in data.items() if v is not None}
        elif isinstance(data, list):
            return [self._clean_none_values(item) for item in data if item is not None]
        else:
            return data
    
    def _get_redis_client(self):
        """Get or create Redis client"""
        global _redis_pool
        if _redis_pool is None and REDIS_AVAILABLE:
            try:
                redis_url = REDIS_URL
                if REDIS_PASSWORD and 'redis://' in redis_url and '@' not in redis_url:
                    encoded_password = quote(REDIS_PASSWORD, safe='')
                    redis_url = redis_url.replace('redis://', f'redis://:{encoded_password}@')
                
                _redis_pool = redis.ConnectionPool.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_keepalive=True
                )
                
                test_client = redis.Redis(connection_pool=_redis_pool)
                test_client.ping()
                logger.info("Redis connection pool created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create Redis pool: {e}")
                _redis_pool = None
                return None
        
        if _redis_pool:
            return redis.Redis(connection_pool=_redis_pool)
        return None
    
    def enqueue_task(
        self,
        tenant_id: str,
        task_type: str,
        payload: Dict[str, Any],
        max_retries: int = 3
    ) -> Optional[str]:
        """Enqueue a new task"""
        if not self.redis_client:
            self.redis_client = self._get_redis_client()
            if not self.redis_client:
                logger.error("Redis not available, cannot enqueue task")
                return None
        
        try:
            task_id = str(uuid.uuid4())
            task = Task(
                id=task_id,
                tenant_id=tenant_id,
                task_type=task_type,
                payload=payload,
                max_retries=max_retries
            )
            
            message = asdict(task)
            try:
                cleaned_payload = self._clean_none_values(payload)
                payload_json = json.dumps(cleaned_payload, default=str)
                message['payload'] = payload_json
            except Exception as e:
                logger.error(f"Failed to serialize payload: {e}")
                raise
            
            cleaned_message = self._clean_none_values(message)
            
            self.redis_client.xadd(
                self.stream_name,
                cleaned_message,
                id='*'
            )
            
            logger.info(f"Task enqueued: {task_id} ({task_type}) for tenant {tenant_id}")
            return task_id
            
        except Exception as e:
            logger.error(f"Failed to enqueue task: {e}")
            return None


class TaskType:
    """Predefined task types"""
    NDVI_PROCESSING = 'ndvi_processing'
    DATA_EXPORT = 'data_export'
    REPORT_GENERATION = 'report_generation'
    NOTIFICATION = 'notification'
    ALERT_EVALUATION = 'alert_evaluation'
    CUSTOM = 'custom'

