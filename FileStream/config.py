from os import environ as env
from dotenv import load_dotenv

load_dotenv()

class Telegram:
    API_ID = int(env.get("API_ID"))
    API_HASH = str(env.get("API_HASH"))
    BOT_TOKEN = str(env.get("BOT_TOKEN"))
    OWNER_ID = int(env.get('OWNER_ID', '7978482443'))
    WORKERS = int(env.get("WORKERS", "12"))  # Increased from 6 to 12 workers
    DATABASE_URL = str(env.get('DATABASE_URL'))
    UPDATES_CHANNEL = str(env.get('UPDATES_CHANNEL', "Telegram"))
    SESSION_NAME = str(env.get('SESSION_NAME', 'FileStream'))
    FORCE_SUB_ID = env.get('FORCE_SUB_ID', None)
    FORCE_SUB = env.get('FORCE_UPDATES_CHANNEL', False)
    FORCE_SUB = True if str(FORCE_SUB).lower() == "true" else False
    SLEEP_THRESHOLD = int(env.get("SLEEP_THRESHOLD", "60"))
    FILE_PIC = env.get('FILE_PIC', "https://graph.org/file/5bb9935be0229adf98b73.jpg")
    START_PIC = env.get('START_PIC', "https://graph.org/file/290af25276fa34fa8f0aa.jpg")
    VERIFY_PIC = env.get('VERIFY_PIC', "https://graph.org/file/736e21cc0efa4d8c2a0e4.jpg")
    MULTI_CLIENT = env.get('MULTI_CLIENT', "true").lower() in ("true", "t", "1", "yes", "y")  # Enable multi-client by default
    FLOG_CHANNEL = int(env.get("FLOG_CHANNEL", 0)) if env.get("FLOG_CHANNEL") else None  # Logs channel for file logs
    ULOG_CHANNEL = int(env.get("ULOG_CHANNEL", 0)) if env.get("ULOG_CHANNEL") else None  # Logs channel for user logs
    MODE = env.get("MODE", "primary")
    SECONDARY = True if MODE.lower() == "secondary" else False
    AUTH_USERS = list(set(int(x) for x in str(env.get("AUTH_USERS", "")).split()))
    EMBED_BASE_LINK = env.get("EMBED_BASE_LINK", "https://siwut.com/articles/go")
    
    # Thumbnail settings
    ENABLE_THUMBNAILS = env.get('ENABLE_THUMBNAILS', "false").lower() in ("true", "t", "1", "yes", "y")  # Disabled by default to save resources
    
    # Performance tuning
    CONNECTION_RETRIES = int(env.get("CONNECTION_RETRIES", "3"))
    MAX_CONCURRENT_DOWNLOADS = int(env.get("MAX_CONCURRENT_DOWNLOADS", "20"))
    CHUNK_SIZE = int(env.get("CHUNK_SIZE", "524288"))  # 512 KB chunks for streaming
    
    # Debug mode
    DEBUG = env.get('DEBUG', "false").lower() in ("true", "t", "1", "yes", "y")  # Disabled by default

class Server:
    PORT = int(env.get("PORT", 8080))
    BIND_ADDRESS = str(env.get("BIND_ADDRESS", "0.0.0.0"))
    PING_INTERVAL = int(env.get("PING_INTERVAL", "1200"))
    HAS_SSL = str(env.get("HAS_SSL", "0").lower()) in ("1", "true", "t", "yes", "y")
    NO_PORT = str(env.get("NO_PORT", "0").lower()) in ("1", "true", "t", "yes", "y")
    FQDN = str(env.get("FQDN", BIND_ADDRESS))
    URL = "http{}://{}{}/".format(
        "s" if HAS_SSL else "", FQDN, "" if NO_PORT else ":" + str(PORT)
    )
    
    # Performance settings
    REQUEST_TIMEOUT = int(env.get("REQUEST_TIMEOUT", "300"))  # 5 minutes timeout
    RATE_LIMIT = int(env.get("RATE_LIMIT", "30"))  # Requests per minute per IP
    BURST_LIMIT = int(env.get("BURST_LIMIT", "10"))  # Burst request limit
    MAX_CLIENTS = int(env.get("MAX_CLIENTS", "10000"))  # Maximum concurrent clients
    CACHE_SIZE = int(env.get("CACHE_SIZE", "1000"))  # Response cache size
    CACHE_TTL = int(env.get("CACHE_TTL", "3600"))  # Cache TTL in seconds


