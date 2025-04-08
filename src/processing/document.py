# processing/document.py
import datetime
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ProcessedDocument:
    id: str
    name: str
    url: str
    mime_type: str
    modified_time_str: str
    created_time_str: str
    downloaded_mime_type: str
    content: Optional[str] = None
    checksum: Optional[str] = None
    chunks: List[str] = field(default_factory=list)
    error: Optional[str] = None # Store processing errors
    # Add parsed datetime objects if needed
    modified_time: Optional[datetime.datetime] = None
    created_time: Optional[datetime.datetime] = None