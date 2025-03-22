import base64
import secrets
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from FileStream.config import Telegram

def string_to_bytes(s):
    """Convert a string to bytes using UTF-8 encoding."""
    return s.encode('utf-8')

def bytes_to_base64url(b):
    """Convert bytes to a Base64 URL-safe encoded string."""
    return base64.urlsafe_b64encode(b).decode('utf-8').rstrip("=")

def encrypt(payload):
    """Encrypt a payload using AES-CBC and return Base64 URL-encoded output."""
    
    key = string_to_bytes('yHG57AHA6Biv8i9zUmjhkMr3xtDs92zp')  # Ensure 32 bytes (AES-256 key)
    iv = secrets.token_bytes(16)  # Generate a random 16-byte IV

    cipher = AES.new(key, AES.MODE_CBC, iv)  # Create AES-CBC cipher
    ciphertext = cipher.encrypt(pad(string_to_bytes(payload), AES.block_size))  # Encrypt with padding

    combined = iv + ciphertext  # Concatenate IV and ciphertext
    return bytes_to_base64url(combined)

def gen_final_embed_link(link):
    encrypted_text = encrypt(link)
    return f"{Telegram.EMBED_BASE_LINK}/{encrypted_text}"
