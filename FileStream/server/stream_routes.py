import time
import math
import logging
import mimetypes
import traceback
import random
import asyncio
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from FileStream.bot import multi_clients, work_loads, FileStream
from FileStream.config import Telegram, Server
from FileStream.server.exceptions import FIleNotFound, InvalidHash
from FileStream import utils, StartTime, __version__
from FileStream.utils.render_template import render_page
from FileStream.utils.file_properties import get_file_thumbnail
from FileStream.utils.cache import file_response_cache, file_info_cache
from FileStream.utils.load_balancer import LoadBalancer

routes = web.RouteTableDef()

# Initialize the load balancer
load_balancer = LoadBalancer(multi_clients, work_loads)

@routes.get("/status", allow_head=True)
async def root_route_handler(_):
    # Add load balancer status to response
    load_balancer_status = load_balancer.get_status()
    
    return web.json_response(
        {
            "server_status": "running",
            "uptime": utils.get_readable_time(time.time() - StartTime),
            "telegram_bot": "@" + FileStream.username,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "load_balancer": load_balancer_status,
            "version": __version__,
            "thumbnails_enabled": getattr(Telegram, 'ENABLE_THUMBNAILS', False)
        }
    )

@routes.get("/watch/{path}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        
        # Check cache first
        cache_key = f"watch_{path}"
        cached_response = file_response_cache.get(cache_key)
        if cached_response:
            logging.debug(f"Cache hit for /watch/{path}")
            return cached_response
        
        # Generate response
        response = web.Response(text=await render_page(path), content_type='text/html')
        
        # Cache the response
        file_response_cache[cache_key] = response
        
        return response
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError, ConnectionError, asyncio.CancelledError) as e:
        logging.error(f"Connection error in /watch route: {str(e)}")
        return web.HTTPServiceUnavailable(text="Service temporarily unavailable. Please try again.")

@routes.get("/thumb/{path}")
async def get_thumbnail(request: web.Request):
    path = request.match_info["path"]
    
    # Check if thumbnails are enabled
    if not getattr(Telegram, 'ENABLE_THUMBNAILS', False):
        return web.json_response({
            "message": "Thumbnails are disabled on this server for performance reasons"
        })
    
    # Check cache first
    cache_key = f"thumb_{path}"
    cached_response = file_response_cache.get(cache_key)
    if cached_response:
        logging.debug(f"Cache hit for /thumb/{path}")
        return cached_response
    
    # Get thumbnail
    response = await get_file_thumbnail(FileStream, path, request)
    
    # Cache the response for thumbnails
    if response.status == 200:
        file_response_cache[cache_key] = response
    
    return response


@routes.get("/dl/{path}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        
        # For small range requests, check cache
        range_header = request.headers.get("Range", "")
        if range_header and "bytes=0-" in range_header:
            cache_key = f"dl_{path}_init"
            cached_response = file_response_cache.get(cache_key)
            if cached_response:
                logging.debug(f"Cache hit for initial range of /dl/{path}")
                return cached_response
        
        return await media_streamer(request, path)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError, ConnectionError, asyncio.CancelledError) as e:
        logging.error(f"Connection error in /dl route: {str(e)}")
        return web.HTTPServiceUnavailable(text="Service temporarily unavailable. Please try again.")
    except Exception as e:
        traceback.print_exc()
        logging.critical(e.with_traceback(None))
        logging.debug(traceback.format_exc())
        raise web.HTTPInternalServerError(text=str(e))

# Class cache for ByteStreamer instances
class_cache = {}

# Custom StreamResponse class with socket error handling
class SafeStreamResponse(web.StreamResponse):
    async def write(self, data):
        try:
            return await super().write(data)
        except (ConnectionResetError, ConnectionError, asyncio.CancelledError) as e:
            logging.warning(f"Connection error during stream write: {str(e)}")
            return False

async def media_streamer(request: web.Request, db_id: str):
    start_time = time.time()
    range_header = request.headers.get("Range", 0)
    
    # Use load balancer to get the best client for this request
    client_id, faster_client = load_balancer.get_client()
    
    if Telegram.MULTI_CLIENT:
        logging.info(f"Client {client_id} is now serving {request.headers.get('X-FORWARDED-FOR',request.remote)}")

    # Get or create ByteStreamer instance
    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        logging.debug(f"Using cached ByteStreamer object for client {client_id}")
    else:
        logging.debug(f"Creating new ByteStreamer object for client {client_id}")
        tg_connect = utils.ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect
    
    # Check file info cache
    file_info = file_info_cache.get(db_id)
    if file_info:
        file_id = file_info
        logging.debug(f"Using cached file properties for {db_id}")
    else:
        logging.debug("Before calling get_file_properties")
        try:
            file_id = await tg_connect.get_file_properties(db_id, multi_clients)
            # Cache the file info for future requests
            file_info_cache[db_id] = file_id
            logging.debug(f"Cached file properties for {db_id}")
        except Exception as e:
            logging.error(f"Error getting file properties: {str(e)}")
            # If one client fails, try another
            load_balancer.mark_unhealthy(client_id)
            # Try one more time with a different client
            alt_client_id, alt_client = load_balancer.get_client()
            if alt_client_id != client_id:
                if alt_client in class_cache:
                    alt_tg_connect = class_cache[alt_client]
                else:
                    alt_tg_connect = utils.ByteStreamer(alt_client)
                    class_cache[alt_client] = alt_tg_connect
                
                file_id = await alt_tg_connect.get_file_properties(db_id, multi_clients)
                file_info_cache[db_id] = file_id
                
                # Use the new client for the rest of the request
                client_id = alt_client_id
                faster_client = alt_client
                tg_connect = alt_tg_connect
                
                logging.info(f"Switched to backup client {client_id} after failure")
    
    logging.debug("After calling get_file_properties")
    
    file_size = file_id.file_size

    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    # Use a smaller chunk size for better streaming performance
    chunk_size = Telegram.CHUNK_SIZE  # Use the configured chunk size
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    
    # Create a safe stream response with error handling
    mime_type = file_id.mime_type
    file_name = utils.get_name(file_id)
    
    # For videos and audio, use inline disposition for better player compatibility
    disposition = "attachment"
    if mime_type and ("video/" in mime_type or "audio/" in mime_type):
        disposition = "inline"
    
    if not mime_type:
        mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"

    # Create custom response with socket error handling
    response = SafeStreamResponse(
        status=206 if range_header else 200,
        headers={
            "Content-Type": f"{mime_type}",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=3600",  # Allow client caching for 1 hour
        },
    )
    
    try:
        await response.prepare(request)
        
        # Use timeout to prevent hanging connections
        try:
            # Create generator for file streaming
            generator = tg_connect.yield_file(
                file_id, client_id, offset, first_part_cut, last_part_cut, part_count, chunk_size
            )
            
            # Use asyncio.wait_for for compatibility with older Python versions
            async def stream_with_timeout():
                async for chunk in generator:
                    if not await response.write(chunk):
                        # If write returns False, the connection was closed
                        break
            
            await asyncio.wait_for(stream_with_timeout(), timeout=Server.REQUEST_TIMEOUT)
        except asyncio.TimeoutError:
            logging.warning(f"Timeout while streaming file {db_id}")
        
        # Ensure we finalize the response
        try:
            await response.write_eof()
        except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
            pass
            
    except Exception as e:
        logging.error(f"Error in media streaming: {str(e)}")
        # If response wasn't started yet, return an error
        if not response.prepared:
            return web.HTTPInternalServerError(text="Error streaming media")
    
    # Cache initial chunk responses for faster subsequent loads
    if range_header and "bytes=0-" in range_header and req_length < 1024*1024:
        cache_key = f"dl_{db_id}_init"
        # We can't cache the actual response, so create a new one for caching
        cached_headers = dict(response.headers)
        cached_response = web.Response(
            status=response.status,
            headers=cached_headers,
            body=b''  # Empty body as we can't get the actual content anymore
        )
        file_response_cache[cache_key] = cached_response
        logging.debug(f"Cached initial response headers for {db_id}")
    
    # Record response time for load balancing
    response_time = time.time() - start_time
    load_balancer.record_response_time(client_id, response_time)
    
    # Mark client as healthy again if it was previously marked unhealthy
    load_balancer.mark_healthy(client_id)
    
    return response
