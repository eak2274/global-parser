import requests
import brotli
import gzip
import zlib

import requests
import brotli
import gzip
import zlib

from pathlib import Path
from typing import Any

from datetime import date, timedelta

def get_decoded_text(response: requests.Response) -> str:
    """
    Decodes response content using headers for encoding and charset.
    
    Args:
        response: The requests.Response object.

    Returns:
        A decoded string.
    """
    content = response.content
    
    # 1. Try to decompress if needed (only if content looks compressed)
    content_encoding = response.headers.get('Content-Encoding', '').lower()
    
    if content_encoding in ['br', 'gzip', 'deflate']:
        try:
            if content_encoding == 'br':
                content = brotli.decompress(content)
            elif content_encoding == 'gzip':
                content = gzip.decompress(content)
            elif content_encoding == 'deflate':
                content = zlib.decompress(content)
        except Exception:
            # Content is already decompressed by requests, use as-is
            pass
    
    # 2. Extract charset from Content-Type header
    content_type = response.headers.get('Content-Type', '')
    charset = 'utf-8'  # default
    
    if 'charset=' in content_type:
        charset = content_type.split('charset=')[-1].split(';')[0].strip()
    
    # 3. Decode using the specified charset
    try:
        return content.decode(charset)
    except (UnicodeDecodeError, LookupError):
        # Fallback to UTF-8 with error replacement
        return content.decode('utf-8', errors='replace')
    
def parse_flashscore_response_file(file_path: Path) -> dict[str, Any]:
    pass

def get_date_by_offset(offset: int) -> date:
    """
    Returns date according to offset.
    offset = 0 -> current date
    offset = -1 -> yesterday
    offset = -2 -> day before yesterday
    etc.
    """
    return date.today() + timedelta(days=offset)

def get_offset_by_date(target_date: date) -> int:
    """
    Returns offset from today's date.
    Today -> 0
    Yesterday -> -1
    Day before yesterday -> -2
    Tomorrow -> 1
    etc.
    """
    today = date.today()
    delta = target_date - today
    return delta.days

def to_int_or_none(value):
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
