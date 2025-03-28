import re
from googleapiclient.errors import HttpError
import logging


INVALID_CHARS = {
        '/': '', '\\': '', ':': '', '*': '', '?': '', '"': '', '<': '', '>': '', '|': '', '.': '_'
    }

def get_name_for_id(service, url=None, file_id=None):
    """Retrieve the name of a Google Drive folder or shared drive."""
    if url and "my-drive" in url:
        target_id = "root"
    elif file_id:
        target_id = file_id
    else:
        raise ValueError("Either URL or file_id must be provided")

    try:
        drive = service.drives().get(driveId=target_id).execute()
        name = drive.get('name')
        trans_table = str.maketrans(INVALID_CHARS)
        sanitized_name = name.translate(trans_table)
        return f"Shared Drive - {sanitized_name}"
    except HttpError:
        folder = service.files().get(fileId=target_id, fields='name', supportsAllDrives=True).execute()
        name = folder.get('name')
        trans_table = str.maketrans(INVALID_CHARS)
        sanitized_name = name.translate(trans_table)
        return sanitized_name
    
def parse_drive_url(url):
    """Extract folder ID, drive ID, or file ID from Google Drive URL."""
    logging.info(f"Parsing URL: {url}")
    
    # Handle personal drive root URLs (multiple variants)
    personal_drive_patterns = [
        r'drive/u/\d+/my-drive',  # Standard personal drive format with user number
        r'drive/my-drive',        # Alternative personal drive format
        r'drive/home'             # Home view of personal drive
    ]
    
    for pattern in personal_drive_patterns:
        if re.search(pattern, url):
            logging.info(f"Matched personal drive with pattern: {pattern}")
            return "root", "folder"  # "root" is a special identifier for the user's My Drive
    
    # Handle shared drive URLs
    shared_drive_match = re.search(r'drive/folders/([0-9A-Za-z_-]+)', url)
    if shared_drive_match:
        drive_id = shared_drive_match.group(1)
        logging.info(f"Matched folder ID: {drive_id}")
        return drive_id, "folder"
    
    # Handle direct file URLs
    file_match = re.search(r'drive/d/([0-9A-Za-z_-]+)', url)
    if file_match:
        file_id = file_match.group(1)
        logging.info(f"Matched file ID: {file_id}")
        return file_id, "file"
    
    # Handle shorter URLs
    short_match = re.search(r'folders/([0-9A-Za-z_-]+)', url)
    if short_match:
        folder_id = short_match.group(1)
        logging.info(f"Matched short folder ID: {folder_id}")
        return folder_id, "folder"
    
    # Handle shared drive root URLs
    shared_drive_root_match = re.search(r'drive/([0-9A-Za-z_-]+)', url)
    if shared_drive_root_match and not any(re.search(p, url) for p in personal_drive_patterns):
        drive_id = shared_drive_root_match.group(1)
        logging.info(f"Matched shared drive ID: {drive_id}")
        return drive_id, "drive"
    
    # Final attempt for direct links with file IDs
    id_match = re.search(r'id=([0-9A-Za-z_-]+)', url)
    if id_match:
        item_id = id_match.group(1)
        logging.info(f"Matched ID parameter: {item_id}")
        return item_id, "item"  # Generic "item" type, will need to be determined later
    
    logging.warning(f"No match found for URL: {url}")
    return None, None
