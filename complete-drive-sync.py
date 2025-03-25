import os
import pickle
import datetime
import json
import hashlib
import sys
import re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import io
import docx
import traceback
import logging
from collections import deque

# Set up logging
logging.basicConfig(filename='drive_sync.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the scopes for API access
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Database file to store document metadata and content
DOCUMENT_DB = 'document_database.json'

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

def get_drive_service():
    """Authenticate and return a Google Drive service object."""
    creds = None
    
    # Load existing credentials if available
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # Refresh or get new credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    # Build the service
    return build('drive', 'v3', credentials=creds)

def get_last_sync_time():
    """Read the last sync time from a file."""
    try:
        with open('last_sync.txt', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        # If it's the first run, use a date far in the past
        return '1970-01-01T00:00:00.000Z'

def save_last_sync_time(time_str):
    """Save the current time as the last sync time."""
    with open('last_sync.txt', 'w') as f:
        f.write(time_str)

def load_document_database():
    """Load the document database from file."""
    if os.path.exists(DOCUMENT_DB):
        with open(DOCUMENT_DB, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"documents": {}, "metadata": {"last_updated": ""}}

def save_document_database(db):
    """Save the document database to file."""
    with open(DOCUMENT_DB, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def extract_text_from_docx(file_data):
    """Extract text from a Word document."""
    doc = docx.Document(io.BytesIO(file_data))
    return "\n".join([paragraph.text for paragraph in doc.paragraphs])

def compute_checksum(text):
    """Compute a checksum for the document text."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def list_files_in_folder(service, folder_id, include_subfolders=True, is_team_drive=False):
    """
    List all files in a folder and optionally its subfolders.
    Returns a tuple of (files, subfolders)
    """
    files = []
    subfolders = []
    
    # Set up parameters for the API call
    query = f"'{folder_id}' in parents and trashed=false"
    
    # Parameters for file listing
    list_params = {
        'q': query,
        'pageSize': 1000,  # Maximum page size
        'fields': "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, parents)",
        'spaces': 'drive'
    }
    
    # Add team drive parameters if needed
    if is_team_drive:
        try:
            # Try newer API approach
            list_params['includeItemsFromAllDrives'] = True
            list_params['corpora'] = 'allDrives'
        except:
            # Fall back to older API approach
            list_params['includeTeamDriveItems'] = True
            list_params['corpora'] = 'teamDrive'
            list_params['teamDriveId'] = folder_id
    
    try:
        # Retry loop for API parameter compatibility
        while True:
            try:
                page_token = None
                while True:
                    if page_token:
                        list_params['pageToken'] = page_token
                    
                    results = service.files().list(**list_params).execute()
                    items = results.get('files', [])
                    
                    for item in items:
                        # Check if it's a folder
                        if item['mimeType'] == 'application/vnd.google-apps.folder':
                            subfolders.append({
                                'id': item['id'], 
                                'name': item['name'],
                                'path': item.get('parents', [])
                            })
                        else:
                            # Check if it's a file type we care about
                            mime_type = item['mimeType']
                            if (mime_type == 'application/vnd.google-apps.document' or
                                mime_type == 'application/pdf' or
                                mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'):
                                files.append(item)
                    
                    page_token = results.get('nextPageToken')
                    if not page_token:
                        break
                
                # If we got here, the API call was successful
                break
                
            except TypeError as e:
                error_msg = str(e)
                logging.warning(f"API parameter error: {error_msg}")
                
                # Remove problematic parameters and try again
                if 'includeItemsFromAllDrives' in list_params and 'includeItemsFromAllDrives' in error_msg:
                    del list_params['includeItemsFromAllDrives']
                    logging.info("Removed includeItemsFromAllDrives parameter")
                    
                if 'corpora' in list_params and 'corpora' in error_msg:
                    del list_params['corpora']
                    logging.info("Removed corpora parameter")
                    
                if 'supportsAllDrives' in list_params and 'supportsAllDrives' in error_msg:
                    del list_params['supportsAllDrives']
                    logging.info("Removed supportsAllDrives parameter")
                    
                # If we've removed all the potentially problematic parameters, just break
                if not any(k in list_params for k in ['includeItemsFromAllDrives', 'supportsAllDrives', 'corpora']):
                    break
    
    except Exception as e:
        logging.error(f"Error listing files in folder {folder_id}: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"Error listing files in folder: {str(e)}")
    
    return files, subfolders

def get_all_files_recursive(service, root_folder_id, is_team_drive=False):
    """
    Get all files in a folder and all its subfolders, recursively.
    Returns a list of files.
    """
    all_files = []
    folders_to_process = deque([{'id': root_folder_id, 'name': 'Root'}])
    processed_folders = set()
    
    print(f"Starting recursive file discovery from folder ID: {root_folder_id}")
    logging.info(f"Starting recursive file discovery from folder ID: {root_folder_id}")
    
    while folders_to_process:
        current_folder = folders_to_process.popleft()
        folder_id = current_folder['id']
        folder_name = current_folder['name']
        
        # Skip if we've already processed this folder
        if folder_id in processed_folders:
            continue
        
        processed_folders.add(folder_id)
        
        print(f"Scanning folder: {folder_name} ({folder_id})")
        logging.info(f"Scanning folder: {folder_name} ({folder_id})")
        
        # Get files and subfolders
        files, subfolders = list_files_in_folder(service, folder_id, True, is_team_drive)
        
        # Add files to the list
        all_files.extend(files)
        print(f"Found {len(files)} files in {folder_name}")
        
        # Add subfolders to the queue
        for subfolder in subfolders:
            folders_to_process.append(subfolder)
        
        print(f"Found {len(subfolders)} subfolders in {folder_name}")
    
    print(f"Total files found: {len(all_files)}")
    logging.info(f"Total files found: {len(all_files)}")
    
    return all_files

def process_documents(service, start_time, doc_db, target_id=None, target_type=None):
    """Process files that have changed since the start_time and update the document database."""
    # Track the current time for the next sync point
    current_time = datetime.datetime.utcnow().isoformat() + 'Z'
    
    # Initialize counters
    changes_processed = 0
    files_updated = 0
    files_deleted = 0
    
    logging.info(f"Starting sync from {start_time}")
    print(f"Starting sync from {start_time}")
    
    if target_id:
        logging.info(f"Target {target_type} ID: {target_id}")
        print(f"Target {target_type} ID: {target_id}")
    
    # Get all docs that have been trashed/deleted since last sync
    # First, let's get a list of file IDs we have in our database
    existing_file_ids = set(doc_db["documents"].keys())
    active_file_ids = set()
    
    try:
        # Different handling based on target type
        if target_type == 'file':
            # Just process a single file
            try:
                file = service.files().get(
                    fileId=target_id,
                    fields='id, name, mimeType, modifiedTime, createdTime'
                ).execute()
                
                items = [file]
                logging.info(f"Found single file: {file['name']}")
                print(f"Found single file: {file['name']}")
                
            except Exception as e:
                logging.error(f"Error getting file {target_id}: {str(e)}")
                print(f"Error getting file: {str(e)}")
                items = []
                
        elif target_type == 'folder':
            # Process a folder and all its subfolders
            is_team_drive = False
            items = get_all_files_recursive(service, target_id, is_team_drive)
            
        elif target_type == 'drive':
            # Process an entire shared drive
            is_team_drive = True
            items = get_all_files_recursive(service, target_id, is_team_drive)
            
        else:
            # No target specified, process all files the user has access to
            # This is the default behavior if no URL is provided
            query = ("(mimeType='application/vnd.google-apps.document' OR "
                    "mimeType='application/pdf' OR "
                    "mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')")
                    
            # Parameters for file listing
            list_params = {
                'q': query,
                'pageSize': 1000,
                'fields': "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime)",
                'spaces': 'drive'
            }
            
            items = []
            page_token = None
            
            while True:
                if page_token:
                    list_params['pageToken'] = page_token
                    
                results = service.files().list(**list_params).execute()
                items.extend(results.get('files', []))
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            logging.info(f"Found {len(items)} files in total")
            print(f"Found {len(items)} files in total")
        
        # Process all the files we found
        for item in items:
            file_id = item['id']
            active_file_ids.add(file_id)
            
            # Check if this file is new or modified since last sync
            if (file_id not in doc_db["documents"] or 
                item['modifiedTime'] > start_time):
                changes_processed += 1
                try:
                    file_name = item['name']
                    mime_type = item['mimeType']
                    
                    logging.info(f"Processing file: {file_name} ({file_id}) - {mime_type}")
                    print(f"Processing file: {file_name}")
                    
                    # For Google Docs, we need to export as DOCX
                    try:
                        if mime_type == 'application/vnd.google-apps.document':
                            request = service.files().export_media(
                                fileId=file_id, 
                                mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                            )
                        else:
                            request = service.files().get_media(fileId=file_id)
                            
                        # Download the file content
                        file_data = io.BytesIO()
                        downloader = MediaIoBaseDownload(file_data, request)
                        done = False
                        while not done:
                            status, done = downloader.next_chunk()
                            print(f"Download progress: {int(status.progress() * 100)}%")
                        
                        # Extract text based on file type
                        if mime_type == 'application/vnd.google-apps.document' or mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                            text = extract_text_from_docx(file_data.getvalue())
                        elif mime_type == 'application/pdf':
                            # For PDFs, you would need a PDF processing library
                            # This is a placeholder
                            text = f"PDF content extraction would go here for {file_name}"
                        else:
                            text = f"Unsupported format: {mime_type} for file {file_name}"
                        
                        # Compute checksum to check if content actually changed
                        checksum = compute_checksum(text)
                        
                        # Check if we have this file already and if the content has changed
                        if (file_id not in doc_db["documents"] or 
                            doc_db["documents"][file_id]["checksum"] != checksum):
                            
                            # Store the document in our database
                            doc_db["documents"][file_id] = {
                                "name": file_name,
                                "mimeType": mime_type,
                                "modifiedTime": item['modifiedTime'],
                                "createdTime": item['createdTime'],
                                "lastSynced": current_time,
                                "checksum": checksum,
                                "content": text
                            }
                            files_updated += 1
                            print(f"Updated file: {file_name}")
                        else:
                            # Just update the lastSynced time
                            doc_db["documents"][file_id]["lastSynced"] = current_time
                            print(f"File unchanged: {file_name}")
                            
                    except Exception as e:
                        logging.error(f"Error downloading file {file_name}: {str(e)}")
                        print(f"Error downloading file {file_name}: {str(e)}")
                    
                except Exception as e:
                    logging.error(f"Error processing file {file_id}: {str(e)}")
                    logging.error(traceback.format_exc())
                    print(f"Error processing file: {str(e)}")
        
        # Identify deleted files (in our DB but not in active files)
        # Only do this for the entire drive if no specific target was provided
        if not target_id:
            deleted_files = existing_file_ids - active_file_ids
            for file_id in deleted_files:
                if file_id in doc_db["documents"]:
                    file_name = doc_db["documents"][file_id]["name"]
                    logging.info(f"File deleted: {file_name} ({file_id})")
                    print(f"File deleted: {file_name}")
                    # Mark as deleted but keep the content for reference
                    doc_db["documents"][file_id]["deleted"] = True
                    doc_db["documents"][file_id]["deletedTime"] = current_time
                    files_deleted += 1
    
    except Exception as e:
        logging.error(f"Error in sync process: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"Error in sync process: {str(e)}")
    
    # Update the database metadata
    doc_db["metadata"]["last_updated"] = current_time
    doc_db["metadata"]["total_documents"] = len(doc_db["documents"])
    doc_db["metadata"]["active_documents"] = len([doc for doc_id, doc in doc_db["documents"].items() if not doc.get("deleted", False)])
    
    # Save the document database
    save_document_database(doc_db)
    
    # Generate the merged file with all content
    generate_merged_file(doc_db, current_time, files_updated, files_deleted)
    
    # Update the last sync time
    save_last_sync_time(current_time)
    
    logging.info(f"Sync completed. Processed {changes_processed} changes, updated {files_updated} files, deleted {files_deleted} files.")
    print(f"Sync completed. Processed {changes_processed} changes, updated {files_updated} files, deleted {files_deleted} files.")
    
def generate_merged_file(doc_db, timestamp, files_updated, files_deleted):
    """
    Generate merged files with all active documents, limiting each file to 200MB OR 500,000 words,
    whichever comes first.
    """
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Maximum file size (200MB in bytes) and word count (500,000 words)
    MAX_FILE_SIZE = 200 * 1024 * 1024
    MAX_WORD_COUNT = 500000
    
    # List to keep track of all generated files
    generated_files = []
    
    # Prepare header content
    header = f"Merged Content - Generated on {timestamp}\n"
    header += f"Total documents: {doc_db['metadata']['total_documents']}\n"
    header += f"Active documents: {doc_db['metadata']['active_documents']}\n"
    header += f"Files updated in this sync: {files_updated}\n"
    header += f"Files deleted in this sync: {files_deleted}\n"
    header += "=" * 50 + "\n\n"
    
    # Initialize variables
    current_file = None
    current_file_path = None
    current_file_size = 0
    current_word_count = 0
    file_index = 1
    
    # Count header words
    header_word_count = len(header.split())
    
    # Create the first file
    current_file_path = f"merged_content_{timestamp_str}_part{file_index}.txt"
    current_file = open(current_file_path, 'w', encoding='utf-8')
    current_file.write(header)
    current_file_size = len(header.encode('utf-8'))
    current_word_count = header_word_count
    generated_files.append(current_file_path)
    
    # Write all active documents
    for file_id, doc_info in doc_db["documents"].items():
        # Skip deleted documents
        if doc_info.get("deleted", False):
            continue
            
        # Prepare document content
        doc_header = f"\n\n===== FILE: {doc_info['name']} ({file_id}) =====\n"
        doc_header += f"Last modified: {doc_info['modifiedTime']}\n"
        doc_header += "=" * 50 + "\n\n"
        doc_content = doc_info["content"] + "\n\n"
        
        # Calculate size of this document
        doc_size = len((doc_header + doc_content).encode('utf-8'))
        doc_word_count = len(doc_content.split())
        doc_header_word_count = len(doc_header.split())
        total_doc_word_count = doc_word_count + doc_header_word_count
        
        # Check if adding this document would exceed either limit
        if (current_file_size + doc_size > MAX_FILE_SIZE or 
            current_word_count + total_doc_word_count > MAX_WORD_COUNT):
            # Close current file
            current_file.close()
            
            # Log which limit was reached
            if current_file_size + doc_size > MAX_FILE_SIZE:
                limit_reason = "file size limit (200MB)"
            else:
                limit_reason = "word count limit (500,000 words)"
            
            logging.info(f"Reached {limit_reason} for {current_file_path}")
            print(f"Reached {limit_reason} for {current_file_path}")
            
            # Create a new file
            file_index += 1
            current_file_path = f"merged_content_{timestamp_str}_part{file_index}.txt"
            current_file = open(current_file_path, 'w', encoding='utf-8')
            
            # Write header to the new file
            current_file.write(header)
            current_file_size = len(header.encode('utf-8'))
            current_word_count = header_word_count
            generated_files.append(current_file_path)
            
            logging.info(f"Created new file: {current_file_path}")
            print(f"Created new file: {current_file_path}")
        
        # Write document to current file
        current_file.write(doc_header)
        current_file.write(doc_content)
        
        # Update current file size and word count
        current_file_size += doc_size
        current_word_count += total_doc_word_count
        
        # Log progress periodically
        if file_id == list(doc_db["documents"].keys())[-1] or file_id == list(doc_db["documents"].keys())[0]:
            logging.info(f"Current file: {current_file_path}, Size: {current_file_size / (1024 * 1024):.2f}MB, Words: {current_word_count}")
            print(f"Current file: {current_file_path}, Size: {current_file_size / (1024 * 1024):.2f}MB, Words: {current_word_count}")
    
    # Close the last file
    current_file.close()
    
    logging.info(f"Generated {len(generated_files)} merged files: {', '.join(generated_files)}")
    print(f"Generated {len(generated_files)} merged files: {', '.join(generated_files)}")
    
    return generated_files

def main():
    """Main function to run the sync process."""
    try:
        # Check for command line arguments
        target_id = None
        target_type = None
        
        if len(sys.argv) > 1:
            url = sys.argv[1]
            logging.info(f"URL provided: {url}")
            print(f"Processing Drive URL: {url}")
            
            # Parse the URL to get the target ID and type
            target_id, target_type = parse_drive_url(url)
            
            if not target_id:
                logging.error(f"Could not parse Drive URL: {url}")
                print(f"Error: Could not parse Drive URL. Please provide a valid Google Drive folder or file URL.")
                print("Examples:")
                print("  - https://drive.google.com/drive/folders/1abc123def456")
                print("  - https://drive.google.com/drive/u/0/my-drive")
                print("  - https://drive.google.com/drive/d/1abc123def456")
                print("  - https://drive.google.com/drive/1abc123def456 (Shared Drive)")
                sys.exit(1)
                
            print(f"Target identified as: {target_type} with ID: {target_id}")
        
        # Get the Drive service
        service = get_drive_service()
        
        # Get the last sync time
        last_sync_time = get_last_sync_time()
        
        # Load the document database
        doc_db = load_document_database()
        
        # Process documents and update the database
        doc_db = process_documents(service, last_sync_time, doc_db, target_id, target_type)
        
    except Exception as e:
        logging.error(f"Sync failed: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"Sync failed: {str(e)}")
        print("See log for details.")


if __name__ == '__main__':
    main()