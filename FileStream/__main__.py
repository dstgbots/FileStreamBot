import sys
import asyncio
import logging
import traceback
import logging.handlers as handlers
from FileStream.config import Telegram, Server
from aiohttp import web
from pyrogram import idle

# Import optimized event loop and JSON libraries
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Using uvloop for enhanced performance")
except ImportError:
    print("uvloop not available, using standard event loop")

# Import optimized DNS resolver
try:
    import aiodns
    from aiohttp.resolver import AsyncResolver
    dns_resolver = AsyncResolver(nameservers=["1.1.1.1", "8.8.8.8"])
    print("Using aiodns for enhanced DNS resolution")
except ImportError:
    dns_resolver = None
    print("aiodns not available, using default resolver")

# Import optimized JSON library
try:
    import orjson
    print("Using orjson for enhanced JSON performance")
except ImportError:
    print("orjson not available, using standard json")

from FileStream.bot import FileStream
from FileStream.server import web_server
from FileStream.bot.clients import initialize_clients

# Get debug mode from environment
DEBUG = getattr(Telegram, 'DEBUG', False)

# Configure logging with proper formatting
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    datefmt="%d/%m/%Y %H:%M:%S",
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(stream=sys.stdout),
              handlers.RotatingFileHandler("streambot.log", mode="a", maxBytes=104857600, backupCount=5, encoding="utf-8")],
)

# Set up a special filter to reduce socket error noise
class SocketErrorFilter(logging.Filter):
    def filter(self, record):
        # Filter out common socket.send() warnings unless in debug mode
        if not DEBUG and "socket.send() raised exception" in record.getMessage():
            return False
        return True

# Apply the filter to relevant loggers
logging.getLogger("asyncio").addFilter(SocketErrorFilter())
logging.getLogger("aiohttp").addFilter(SocketErrorFilter())

# Silence verbose loggers
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.WARNING)

# Create server with optimized settings
web_app = web_server()
if dns_resolver:
    web_app._resolver = dns_resolver

server = web.AppRunner(web_app)

# Get or create optimized event loop
loop = asyncio.get_event_loop()

# Increase max connections for event loop
try:
    # Set higher ulimit for more concurrent connections
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(hard, 10000)  # Target 10K open files
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        print(f"Increased file limit from {soft} to {target}")
except (ImportError, ValueError, OSError):
    pass

async def start_services():
    print()
    if Telegram.SECONDARY:
        print("------------------ Starting as Secondary Server ------------------")
    else:
        print("------------------- Starting as Primary Server -------------------")
    print()
    print("-------------------- Initializing Telegram Bot --------------------")

    await FileStream.start()
    bot_info = await FileStream.get_me()
    FileStream.id = bot_info.id
    FileStream.username = bot_info.username
    FileStream.fname=bot_info.first_name
    print("------------------------------ DONE ------------------------------")
    print()
    print("---------------------- Initializing Clients ----------------------")
    await initialize_clients()
    print("------------------------------ DONE ------------------------------")
    print()
    print("--------------------- Initializing Web Server ---------------------")
    await server.setup()
    site = web.TCPSite(
        server, 
        Server.BIND_ADDRESS, 
        Server.PORT,
        backlog=1024,        # Increase connection backlog
        reuse_address=True,  # Allow address reuse
        reuse_port=True      # Allow port reuse for better load distribution
    )
    await site.start()
    print("------------------------------ DONE ------------------------------")
    print()
    print("------------------------- Service Started -------------------------")
    print("                        bot =>> {}".format(bot_info.first_name))
    if bot_info.dc_id:
        print("                        DC ID =>> {}".format(str(bot_info.dc_id)))
    print(" URL =>> {}".format(Server.URL))
    print(" Thumbnails =>> {}".format("Enabled" if getattr(Telegram, 'ENABLE_THUMBNAILS', False) else "Disabled"))
    print("------------------------------------------------------------------")
    await idle()

async def cleanup():
    await server.cleanup()
    await FileStream.stop()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        pass
    except Exception as err:
        logging.error(traceback.format_exc())
    finally:
        loop.run_until_complete(cleanup())
        loop.stop()
        print("------------------------ Stopped Services ------------------------")
