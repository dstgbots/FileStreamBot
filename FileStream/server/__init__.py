import logging
from aiohttp import web
from .stream_routes import routes
from .middlewares import (
    rate_limit_middleware,
    timeout_middleware, 
    error_handling_middleware,
    performance_middleware
)

def web_server():
    """
    Initialize and configure the web server with all middlewares and routes
    """
    # Configure middlewares (order matters - executed in reverse order)
    middlewares = [
        error_handling_middleware,  # Should be first (last to execute)
        rate_limit_middleware,      # Rate limiting
        timeout_middleware,         # Request timeouts
        performance_middleware,     # Performance tracking (first to execute)
    ]
    
    # Create application with middlewares and increased client max size
    web_app = web.Application(
        middlewares=middlewares,
        client_max_size=100 * 1024 * 1024  # 100MB max request size
    )
    
    # Add routes
    web_app.add_routes(routes)
    
    # Configure for high load
    web_app['connection_timeout'] = 600  # 10 minutes connection timeout
    
    logging.info("Web server initialized with optimized settings")
    return web_app
