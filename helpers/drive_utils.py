import re
from googleapiclient.errors import HttpError
import logging


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
        return f"Shared Drive - {drive.get('name')}"
    except HttpError:
        folder = service.files().get(fileId=target_id, fields='name', supportsAllDrives=True).execute()
        return folder.get('name')
    
def parse_drive_url(url):
    """Extract folder ID, drive ID, or file ID from Google Drive URL."""
    logging.info(f"Parsing URL: {url}")
    
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
    if shared_drive_root_match:
        drive_id = shared_drive_root_match.group(1)
        logging.info(f"Matched shared drive ID: {drive_id}")
        return drive_id, "drive"
        
    # Handle drive root URLs
    drive_match = re.search(r'drive/u/\d+/my-drive', url)
    if drive_match:
        logging.info("Matched root drive")
        return "root", "folder"  # "root" is a special identifier for the user's My Drive
    
    logging.warning(f"No match found for URL: {url}")
    return None, None
