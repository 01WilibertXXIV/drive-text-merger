import os

"""
Application constants for file and directory management.

This module defines the constants used throughout the Sync Tool application
for managing file paths, database locations, and output directories.
"""


APP_NAME = "GOOGLE DRIVE SICK SYNC MERGE TOOL"


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
"""

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