import asyncio
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Dict, Any, Callable, Awaitable

logger = logging.getLogger(__name__)

# Track active operations
class OperationInfo:
    def __init__(self, workspace_id: str, op_type: str, request_id: str):
        self.workspace_id = workspace_id
        self.op_type = op_type
        self.request_id = request_id
        self.start_time = time.time()
        self.active = True

# Global state
_workspace_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_active_operations: Dict[str, OperationInfo] = {}

# In-flight request deduplication
_inflight_exports: Dict[str, asyncio.Task] = {}

# Cache exported definitions with TTL
_export_cache: Dict[str, Dict[str, Any]] = {}

def get_cache_key(workspace_id: str, pipeline_ids: list[str]) -> str:
    """Generate a consistent cache key for export operations."""
    return f"{workspace_id}::" + ",".join(sorted(pipeline_ids))

@asynccontextmanager
async def workspace_lock(op_type: str, request_id: str, *workspace_ids: str):
    """
    Acquire locks for one or more workspace IDs in a consistent order to prevent deadlocks.
    Tracks the active operation and logs metrics.
    """
    unique_ids = sorted(list(set(wid for wid in workspace_ids if wid)))
    
    if not unique_ids:
        yield
        return

    locks = [_workspace_locks[wid] for wid in unique_ids]
    
    # Track the operation per workspace
    ops = []
    
    # Acquire locks in sorted order
    for wid, lock in zip(unique_ids, locks):
        logger.debug(f"Waiting for lock on workspace {wid} (op: {op_type}, req: {request_id})")
        await lock.acquire()
        logger.info(f"LOCK ACQUIRED workspace {wid} | req={request_id} | op={op_type}")
        op = OperationInfo(wid, op_type, request_id)
        _active_operations[wid] = op
        ops.append(op)
        
    try:
        logger.info(f"REQUEST START | req={request_id} | op={op_type} | workspaces={unique_ids}")
        yield
        logger.info(f"REQUEST END | req={request_id} | op={op_type} | status=Success")
    except Exception as e:
        logger.error(f"REQUEST END | req={request_id} | op={op_type} | status=Failed | error={e}")
        raise
    finally:
        # Release locks in reverse order
        for wid, lock, op in reversed(list(zip(unique_ids, locks, ops))):
            op.active = False
            if wid in _active_operations:
                del _active_operations[wid]
            logger.info(f"LOCK RELEASED workspace {wid} | req={request_id} | op={op_type}")
            lock.release()

def invalidate_export_cache(workspace_id: str):
    """Invalidate cache entries for a given workspace_id."""
    keys_to_delete = [k for k in _export_cache.keys() if k.startswith(f"{workspace_id}::")]
    for k in keys_to_delete:
        del _export_cache[k]

async def deduplicate_export(workspace_id: str, pipeline_ids: list[str], export_func: Callable[[], Awaitable[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Ensure only one export runs for a given workspace_id and pipeline_ids combination.
    Uses an in-memory TTL cache (60s) to avoid unnecessary requests.
    """
    cache_key = get_cache_key(workspace_id, pipeline_ids)
    
    # Check cache
    if cache_key in _export_cache:
        cache_entry = _export_cache[cache_key]
        # Check TTL (60 seconds)
        if time.time() - cache_entry["timestamp"] < 60:
            logger.info(f"Returning cached export for {cache_key}")
            return cache_entry["data"]
        else:
            del _export_cache[cache_key]

    # Check in-flight
    if cache_key in _inflight_exports:
        logger.info(f"Waiting for in-flight export task for {cache_key}")
        return await _inflight_exports[cache_key]
        
    # Create new task
    task = asyncio.create_task(export_func())
    _inflight_exports[cache_key] = task
    
    try:
        result = await task
        # Populate cache
        _export_cache[cache_key] = {
            "timestamp": time.time(),
            "data": result
        }
        return result
    finally:
        if cache_key in _inflight_exports:
            del _inflight_exports[cache_key]
