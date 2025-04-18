import time
import asyncio
from typing import Dict, Any, Optional
from collections import OrderedDict
import logging
from FileStream.config import Server

class LRUCache:
    """
    Least Recently Used (LRU) cache implementation
    """
    def __init__(self, max_size: int = 100, ttl: int = 3600):
        """
        Initialize the LRU cache
        
        :param max_size: Maximum number of items to keep in cache
        :param ttl: Time-to-live in seconds for cached items
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache: OrderedDict = OrderedDict()
        self.expiry: Dict[str, float] = {}
        asyncio.create_task(self._cleanup_task())
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in cache and is not expired"""
        if key not in self.cache:
            return False
        
        # Check if item is expired
        if key in self.expiry and time.time() > self.expiry[key]:
            self._remove_item(key)
            return False
            
        return True
    
    def __getitem__(self, key: str) -> Any:
        """Get item from cache, update its position in LRU, and return it"""
        if key not in self:
            raise KeyError(key)
        
        # Move to end of OrderedDict (most recently used)
        value = self.cache.pop(key)
        self.cache[key] = value
        return value
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Add item to cache, evicting least recently used items if necessary"""
        if key in self.cache:
            self.cache.pop(key)
        
        # Set expiry time
        self.expiry[key] = time.time() + self.ttl
        
        # Add to cache
        self.cache[key] = value
        
        # Evict oldest items if we exceed max size
        if len(self.cache) > self.max_size:
            self._evict_oldest()
    
    def _evict_oldest(self) -> None:
        """Evict the least recently used item from the cache"""
        if not self.cache:
            return
        
        # Get the first item from the ordered dict (oldest)
        oldest_key, _ = next(iter(self.cache.items()))
        self._remove_item(oldest_key)
        
    def _remove_item(self, key: str) -> None:
        """Remove an item from the cache and expiry tracking"""
        if key in self.cache:
            self.cache.pop(key)
        if key in self.expiry:
            self.expiry.pop(key)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get item from cache or return default if not found"""
        try:
            return self[key]
        except KeyError:
            return default
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set item in cache with optional custom TTL"""
        self[key] = value
        if ttl is not None:
            self.expiry[key] = time.time() + ttl
    
    def clear(self) -> None:
        """Clear all items from cache"""
        self.cache.clear()
        self.expiry.clear()
    
    async def _cleanup_task(self) -> None:
        """
        Periodic task to clean up expired cache entries
        """
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                current_time = time.time()
                
                # Create a list of keys to remove (can't modify while iterating)
                expired_keys = [
                    key for key, expiry_time in self.expiry.items() 
                    if current_time > expiry_time
                ]
                
                # Remove expired keys
                for key in expired_keys:
                    self._remove_item(key)
                
                if expired_keys:
                    logging.debug(f"Cache cleanup: removed {len(expired_keys)} expired items")
            except Exception as e:
                logging.error(f"Error in cache cleanup: {str(e)}")

# Use configuration values for cache sizes and TTLs
RESPONSE_CACHE_SIZE = getattr(Server, 'CACHE_SIZE', 1000)
RESPONSE_CACHE_TTL = getattr(Server, 'CACHE_TTL', 3600)
INFO_CACHE_SIZE = RESPONSE_CACHE_SIZE * 5  # Store 5x more file info entries than responses
INFO_CACHE_TTL = RESPONSE_CACHE_TTL * 2   # Keep file info 2x longer than responses

# Create caches with configured sizes
file_response_cache = LRUCache(max_size=RESPONSE_CACHE_SIZE, ttl=RESPONSE_CACHE_TTL)
file_info_cache = LRUCache(max_size=INFO_CACHE_SIZE, ttl=INFO_CACHE_TTL) 
