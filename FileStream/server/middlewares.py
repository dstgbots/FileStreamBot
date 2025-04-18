import time
import logging
import asyncio
from typing import Dict, Callable, Awaitable, Optional
from collections import defaultdict
from aiohttp import web
from FileStream.config import Server

# Simple in-memory rate limiter
class RateLimiter:
    def __init__(self, rate_limit: int = 30, time_period: int = 60, burst_limit: int = 5):
        """
        Initialize rate limiter
        
        :param rate_limit: Maximum number of requests per time period
        :param time_period: Time period in seconds
        :param burst_limit: Maximum number of requests allowed in burst
        """
        self.rate_limit = rate_limit
        self.time_period = time_period
        self.burst_limit = burst_limit
        self.request_timestamps: Dict[str, list] = defaultdict(list)
        self.cleanup_counter = 0
    
    async def is_rate_limited(self, key: str) -> bool:
        """
        Check if a key is rate limited
        
        :param key: The key to check (usually IP address)
        :return: True if rate limited, False otherwise
        """
        # Periodically cleanup old timestamps
        self.cleanup_counter += 1
        if self.cleanup_counter > 1000:
            self._cleanup_old_timestamps()
            self.cleanup_counter = 0
            
        current_time = time.time()
        
        # Add current timestamp
        self.request_timestamps[key].append(current_time)
        
        # Calculate number of requests in the time period
        min_time = current_time - self.time_period
        recent_requests = [t for t in self.request_timestamps[key] if t > min_time]
        
        # Update timestamps list with only recent ones
        self.request_timestamps[key] = recent_requests
        
        # Check if exceeds rate limit
        if len(recent_requests) > self.rate_limit:
            # Check burst - allow short bursts over the limit
            burst_min_time = current_time - 5  # 5 second window for burst
            burst_requests = [t for t in recent_requests if t > burst_min_time]
            
            if len(burst_requests) > self.burst_limit:
                return True
        
        return False
    
    def _cleanup_old_timestamps(self):
        """Remove timestamps older than the time period to save memory"""
        current_time = time.time()
        min_time = current_time - self.time_period
        
        for key in list(self.request_timestamps.keys()):
            # Clean old timestamps
            self.request_timestamps[key] = [
                t for t in self.request_timestamps[key] if t > min_time
            ]
            
            # Remove empty lists
            if not self.request_timestamps[key]:
                del self.request_timestamps[key]

# Create a global rate limiter instance using config values
rate_limiter = RateLimiter(
    rate_limit=getattr(Server, 'RATE_LIMIT', 30),
    time_period=60,
    burst_limit=getattr(Server, 'BURST_LIMIT', 10)
)
whitelist = set()  # IPs that bypass rate limiting

@web.middleware
async def rate_limit_middleware(request: web.Request, handler: Callable[[web.Request], Awaitable[web.Response]]) -> web.Response:
    """
    Middleware to apply rate limiting
    
    :param request: The web request
    :param handler: The request handler
    :return: The response
    """
    # Get client IP
    ip = request.headers.get("X-Forwarded-For", request.remote)
    
    # Skip rate limiting for status endpoints and whitelisted IPs
    if request.path.startswith("/status") or ip in whitelist:
        return await handler(request)
    
    # Check if rate limited
    if await rate_limiter.is_rate_limited(ip):
        logging.warning(f"Rate limited request from {ip}")
        return web.HTTPTooManyRequests(
            text="Too many requests. Please try again later.",
            headers={"Retry-After": "60"}
        )
    
    return await handler(request)

@web.middleware
async def timeout_middleware(request: web.Request, handler: Callable[[web.Request], Awaitable[web.Response]]) -> web.Response:
    """
    Middleware to apply timeouts to all requests
    
    :param request: The web request
    :param handler: The request handler
    :return: The response
    """
    # Different timeouts for different endpoints
    if request.path.startswith(("/dl/", "/watch/")):
        timeout_duration = getattr(Server, 'REQUEST_TIMEOUT', 300)  # Use configured timeout
    else:
        timeout_duration = 60   # 1 minute for other requests
    
    try:
        # Apply timeout
        return await asyncio.wait_for(handler(request), timeout=timeout_duration)
    except asyncio.TimeoutError:
        logging.error(f"Request timeout for {request.path}")
        return web.HTTPGatewayTimeout(text="Request timed out. Please try again.")

@web.middleware
async def error_handling_middleware(request: web.Request, handler: Callable[[web.Request], Awaitable[web.Response]]) -> web.Response:
    """
    Middleware to handle errors and exceptions
    
    :param request: The web request
    :param handler: The request handler
    :return: The response
    """
    try:
        return await handler(request)
    except web.HTTPException:
        # Let aiohttp's HTTP exceptions pass through
        raise
    except asyncio.CancelledError:
        # Client disconnected or request was cancelled
        raise
    except Exception as e:
        # Log the exception
        logging.exception(f"Unhandled exception in request handler: {str(e)}")
        return web.HTTPInternalServerError(text="An unexpected error occurred. Please try again later.")

@web.middleware
async def performance_middleware(request: web.Request, handler: Callable[[web.Request], Awaitable[web.Response]]) -> web.Response:
    """
    Middleware to track request performance
    
    :param request: The web request
    :param handler: The request handler
    :return: The response
    """
    start_time = time.time()
    
    response = await handler(request)
    
    # Calculate and log request duration
    duration = time.time() - start_time
    
    # Log slow requests
    if duration > 5:
        logging.warning(f"Slow request: {request.method} {request.path} took {duration:.2f}s")
    
    # Add performance headers to response
    response.headers["X-Response-Time"] = f"{duration:.3f}s"
    
    return response 
