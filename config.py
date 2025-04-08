import os


"""
Application constants for file and directory management.

This module defines the constants used throughout the Sync Tool application
for managing file paths, database locations, and output directories.
"""



APP_NAME = "GOOGLE DRIVE SICK SYNC MERGE TOOL"
LOG_FILE = "drive_sync.log"
LAST_SYNC_FILE = 'last_sync.txt'
DEFAULT_START_TIME = '1970-01-01T00:00:00.000Z'


# Data Storage Constants
# ---------------------
DATA_FOLDER = '.data'
"""
Hidden folder for storing application data.
This folder is automatically created by the program and hidden from users
to prevent manual modification of critical data files.
"""

DATA_FOLDER_PATH = os.path.join(os.getenv('APPDATA'), "DriveSyncMerger")
"""
    TO IMPLEMENT.
"""

DOCUMENT_DB_FILE = 'document_database.json'
"""
JSON database file for document metadata and content.
Stored in the DATA_FOLDER to prevent accidental manual modification.
Format: JSON with document IDs as keys and metadata/content as values.

{
    "name": name of the document in the google drive (string),
    "url": url of the document in the google drive (string),
    "mimeType": mime type of the document (string),
    "modifiedTime": last modified time of the document (string),
    "createdTime": creation time of the document (string),
    "lastSynced": last synced time of the document (string),
    "checksum": checksum of the document (string),
    "content": text content of the document (string)
}
"""

MARQO_URL = "http://gk4k0ckgck04g04ow8w08wws.100.71.51.35.sslip.io/"
MARQO_INDEX_NAME = "drive_sync_index"



SYNC_INFO_FILE = 'last_sync.txt'
"""
Text file containing information about the last synchronization operation.
Stored in the DATA_FOLDER and includes timestamp and sync statistics.
"""


# Output Constants
# ---------------
SYNCED_CONTENT_FOLDER = "synced_content"
"""
Folder name where merged and synchronized content files are stored.
This is the main output directory accessible to users.
"""



QUERY_MIME_TYPES = [
    'application/vnd.google-apps.document',        # Google Docs
    'application/pdf',                             # PDF
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document', # DOCX
    'application/vnd.google-apps.spreadsheet',     # Google Sheets
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', # XLSX
    'text/csv'                                     # CSV
]

# config.py
PAGE_SIZE = 100
DRIVE_FIELDS = "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, webViewLink, parents, trashed, lastModifyingUser)"