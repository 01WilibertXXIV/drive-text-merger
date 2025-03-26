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
import time
import datetime
import fitz  # PyMuPDF

from helpers.drive_utils import get_name_for_id, parse_drive_url
from helpers.auth_utils import get_drive_service
from helpers.text_utils import extract_text_from_docx, extract_text_from_pdf


# Set up logging
logging.basicConfig(filename='drive_sync.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Database file to store document metadata and content
DATA_FOLDER = '.data'
DOCUMENT_DB = 'document_database.json'


start_time = datetime.datetime.now()

SYNCED_CONTENT_FOLDER_NAME = "synced_content"

RESET = "\033[0m"       # Reset color
BOLD_CYAN = "\033[1;36m"  # Bold Cyan for filenames
BOLD_YELLOW = "\033[1;33m"  # Bold Yellow for folder names
MAGENTA = "\033[0;35m"  # Magenta for status updates
BRIGHT_BLUE = "\033[1;34m"  # Bright Blue for Drive IDs
RED = "\033[0;31m"  # Red for errors
GREEN = "\033[0;32m"  # Green for success


#region Helper Functions

def get_last_sync_time(output_folder_path):
    """Read the last sync time from a file."""
    try:
        with open(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/last_sync.txt"), 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        # If it's the first run, use a date far in the past
        return '1970-01-01T00:00:00.000Z'

def save_last_sync_time(time_str, output_folder_path):
    """Save the current time as the last sync time."""
    with open(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/last_sync.txt"), 'w') as f:
        f.write(time_str)

def load_document_database(output_folder_path):
    """Load the document database from file."""
    if os.path.exists(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/{DOCUMENT_DB}")):
        with open(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/{DOCUMENT_DB}"), 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"documents": {}, "metadata": {"last_updated": ""}}

def save_document_database(db, output_folder_path):
    """Save the document database to file."""
    with open(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/{DOCUMENT_DB}"), 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def compute_checksum(text):
    """Compute a checksum for the document text."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

#endregion



def get_all_subfolders(service, folder_id, parent_path='', _subfolder_counter=None, _start_time=None):
    """
    Recursively retrieve all subfolders and their full path.
    
    Args:
        service: Google Drive service object
        folder_id: ID of the parent folder
        parent_path: Path of parent folders
        _subfolder_counter: Internal counter to track total subfolders found
        _start_time: Time when the operation started
    
    Returns:
        List of dictionaries containing folder details
    """
    # Initialize the counter and start time if not provided
    if _subfolder_counter is None:
        _subfolder_counter = {'count': 0}
    
    if _start_time is None:
        _start_time = time.time()
    
    subfolders = []
    
    # Query to get all subfolders
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    
    try:
        page_token = None
        while True:
            results = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, parents)',
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            for folder in results.get('files', []):
                # Increment the counter
                _subfolder_counter['count'] += 1
                
                # Construct full path
                full_path = f"{parent_path}/{folder['name']}" if parent_path else folder['name']
                
                # Add current folder to list
                subfolders.append({
                    'id': folder['id'],
                    'name': folder['name'],
                    'path': full_path
                })

                # Calculate elapsed time
                elapsed_time = time.time() - _start_time
                hours, remainder = divmod(int(elapsed_time), 3600)
                minutes, seconds = divmod(remainder, 60)

                # Print current subfolder count with incrementing number and elapsed time
                print(f"\rSubfolders found: {_subfolder_counter['count']} | Elapsed: {hours:02d}:{minutes:02d}:{seconds:02d}", end='', flush=True)
                
                # Recursively get subfolders of this folder
                subfolders.extend(get_all_subfolders(service, folder['id'], full_path, _subfolder_counter, _start_time))
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        
        # Print a newline after completed scanning
        if parent_path == '':
            print()  # Only print newline at the end of the top-level call
    
    except Exception as e:
        logging.error(f"Error retrieving subfolders: {e}")
        print(f"Error retrieving subfolders: {e}")
    
    return subfolders



def process_documents(service, start_time, doc_db, target_id=None, target_type=None, output_folder_path=None, output_folder_name=None):
    """
    Enhanced process_documents to recursively search through all subfolders
    """
    # Get list of all changes since the last sync
    changes_processed = 0
    files_updated = 0
    files_deleted = 0
    
    # Track the current time for the next sync point
    current_time = datetime.datetime.now(datetime.UTC).isoformat() + 'Z'
    
    print()
    if(start_time == "1970-01-01T00:00:00.000Z"):
        print(f"First time running this script, building database from scratch. \nThis may take a while...")
    else:
        logging.info(f"Starting sync from {start_time}")
        print(f"Starting sync from last sync time: {start_time}")
    print()

    if target_id:
        logging.info(f"Target {target_type} ID: {target_id}")
     
    # Get all docs that have been trashed/deleted since last sync
    existing_file_ids = set(doc_db["documents"].keys())
    active_file_ids = set()
    
    # Prepare list of folder IDs to search
    folder_ids_to_search = [target_id]
    
    # If a specific folder is targeted, get all its subfolders
    if target_id and target_type == 'folder':
        subfolders = get_all_subfolders(service, target_id)
        folder_ids_to_search.extend([folder['id'] for folder in subfolders])
        
        logging.info(f"Found {len(subfolders)} subfolders")
        #print(f"Found {len(subfolders)} subfolders")
    
    try:

        subfolders_count = 1
        # Process each folder
        for search_folder_id in folder_ids_to_search:

            if search_folder_id == "my-drive" or search_folder_id == "u/0/my-drive":
                search_folder_id = "root"


            logging.info(f"Searching in folder: {search_folder_id}")
            #print(f"({subfolders_count}/{len(folder_ids_to_search)}) | Searching in folder: {search_folder_id}")
            
            # Construct query for this folder
            query = (
                "(mimeType='application/vnd.google-apps.document' OR "
                "mimeType='application/pdf' OR "
                "mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document') "
                f"and '{search_folder_id}' in parents"
            )


            
            page_token = None
            while True:
                list_params = {
                    'q': query,
                    'pageSize': 100,
                    'fields': "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime)",
                    'spaces': 'drive',
                    'supportsAllDrives': True,
                    'includeItemsFromAllDrives': True
                }
                
                if page_token:
                    list_params['pageToken'] = page_token
                
                results = service.files().list(**list_params).execute()

                folder_name = get_name_for_id(service, file_id=search_folder_id)

                terminal_message = f"({subfolders_count}/{len(folder_ids_to_search)}) - Searching in {BOLD_YELLOW}{folder_name}{RESET}"
                print(terminal_message)
                
                items = results.get('files', [])
                logging.info(f"Found {len(items)} files in {folder_name}")

                if(len(items) == 0):
                    print(f"  No files found in this folder")
                else:
                    print(f"  Found {len(items)} files")

                processed_files_count = 0
                files_to_process = len(items)

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
                            print(f"  ↳ {BOLD_CYAN}{file_name}{RESET}. Processing...  ", end="", flush=True) 
                            
                            # For Google Docs, we need to export as DOCX
                            export_params = {
                                'fileId': file_id,
                            }
                            
                            if mime_type == 'application/vnd.google-apps.document':
                                export_params['mimeType'] = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                                request = service.files().export_media(**export_params)
                            else:
                                request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
                            
                            # Download the file content
                            file_data = io.BytesIO()
                            downloader = MediaIoBaseDownload(file_data, request)
                            done = False
                            while not done:
                                status, done = downloader.next_chunk()
                                print(f"\r  ↳ {BOLD_CYAN}{file_name}{RESET} - {int(status.progress() * 100)}%", end="", flush=True)
                            print(f"\r  ↳ {BOLD_CYAN}{file_name}{RESET} - {GREEN}Updated!{RESET}                 ") 

                            # Extract text based on file type
                            if mime_type == 'application/vnd.google-apps.document' or mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                                text = extract_text_from_docx(file_data.getvalue())
                            elif mime_type == 'application/pdf':
                                text = extract_text_from_pdf(file_data.getvalue())
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
                            else:
                                # Just update the lastSynced time
                                doc_db["documents"][file_id]["lastSynced"] = current_time

                            processed_files_count += 1
                            
                        except Exception as e:
                            logging.error(f"Error processing file {file_id}: {str(e)}")
                            logging.error(traceback.format_exc())
                            # print(f"Error processing file: {str(e)}")
                            # print(f"Error processing file: {file_name} ({file_id}) - {mime_type}")

                    else:
                        file_name = item['name']
                        print(f"  ↳ {BOLD_CYAN}{file_name}{RESET} - No changes detected. Skipping!")

                # if(processed_files_count != files_to_process):
                #     print(f"  {files_to_process - processed_files_count} files did not require an update.")

                print()   
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            subfolders_count += 1
        
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
    save_document_database(doc_db, output_folder_path)
    
    # Generate the merged file with all content
    generate_merged_file(doc_db, current_time, files_updated, files_deleted, output_folder_path, output_folder_name)
    
    # Update the last sync time
    save_last_sync_time(current_time, output_folder_path)

    # duration = datetime.datetime.now() - start_time
    # hours, remainder = divmod(int(duration.total_seconds()), 3600)
    # minutes, seconds = divmod(remainder, 60)
    
    logging.info(f"Sync completed. Processed {changes_processed} changes, updated {files_updated} files, deleted {files_deleted} files.")
    # logging.info(f"Total operation time: {hours:02d}:{minutes:02d}:{seconds:02d}")

    print(f"Sync completed. Processed {changes_processed} changes, updated {files_updated} files, deleted {files_deleted} files.")
    # print(f"Total operation time: {hours:02d}:{minutes:02d}:{seconds:02d}")
    
    return doc_db


def generate_merged_file(doc_db, timestamp, files_updated, files_deleted, output_folder_path=None, output_folder_name=None):
    """
    Generate merged files with all active documents, limiting each file to 200MB OR 450,000 words,
    whichever comes first.
    """
    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # Maximum file size (200MB in bytes) and word count (450,000 words)
    MAX_FILE_SIZE = 200 * 1024 * 1024
    MAX_WORD_COUNT = 450000
    
    # List to keep track of all generated files
    generated_files = []

    duration = datetime.datetime.now() - start_time
    hours, remainder = divmod(int(duration.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Prepare header content
    header = "=" * 50
    header += f"Sync Completed - Generated on {timestamp}\n"
    header += "=" * 50
    header += f"Operation took {hours:02d}:{minutes:02d}:{seconds:02d}\n"
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
    
    # Create the first file in the specified output folder path
    current_file_path = os.path.join(output_folder_path, f"{timestamp_str}_{output_folder_name}_part{file_index}.txt")
    current_file = open(current_file_path, 'w', encoding='utf-8')
    current_file.write(header)
    current_file_size = len(header.encode('utf-8'))
    current_word_count = header_word_count
    generated_files.append(current_file_path)

    print()
    print("\n" + "="*50)
    print(f"{GREEN}✅ Merging Completed!{RESET}")
    print(f"{BOLD_YELLOW}📁 Output Folder:{RESET} {output_folder_path}")
    print(f"📂 {len(generated_files)} merged files created: ")
    
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

        document_part_name = f"{timestamp_str}_documents_part{file_index}.txt"
        
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
            current_file_path = os.path.join(output_folder_path, document_part_name)
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
        if file_id == list(doc_db["documents"].keys())[-1]:
            logging.info(f"Current file: {current_file_path}, Size: {current_file_size / (1024 * 1024):.2f}MB, Words: {current_word_count}")
            print(f" ↳ {document_part_name}")
            print(f"   Size: {current_file_size / (1024 * 1024):.2f}MB")
            print(f"   Words: {current_word_count}")
    
    # Close the last file
    current_file.close()
    
    logging.info(f"Generated {len(generated_files)} merged files: {', '.join(generated_files)}")
    print("="*50 + "\n")

    return generated_files

def main():
    """Main function to run the sync process."""

    print("=================================================")
    print("Welcome to the Google Drive Sync Script")
    print("")
    print("This script will sync your Google Drive with your computer.")
    print("It will create a new folder in the current working directory called 'synced_content'.")
    print("It will then create a new folder inside 'synced_content' with the name of the folder you want to sync from Google Drive.")
    print("Do not rename the folder after it has been created, or the update script will not work. You will have to start the synchronization process from scratch if you do.")
    print("=================================================")

    try:
        # Check for command line arguments
        target_id = None
        target_type = None

        output_folder_name = None
        output_folder_path = None

        # Get the Drive service
        service = get_drive_service()
        
        if len(sys.argv) > 1:
            url = sys.argv[1]
            logging.info(f"URL provided: {url}")
            print(f"\nProcessing Drive URL: {url}")
            
            # Parse the URL to get the target ID and type
            target_id, target_type = parse_drive_url(url)

            # Get the folder name
            output_folder_name = get_name_for_id(service, url=url, file_id=target_id)
            print(f"Drive name: {output_folder_name}")

            # Create the output folder
            output_folder_path = os.path.join(os.getcwd(), SYNCED_CONTENT_FOLDER_NAME, output_folder_name)
            
            logging.info(f"Creating output folder: {output_folder_path}")
            print(f"Folder successfully created on computer\n")
            os.makedirs(output_folder_path, exist_ok=True)
            os.makedirs(f"{output_folder_path}/{DATA_FOLDER}", exist_ok=True)
            
            if not target_id:
                logging.error(f"Could not parse Drive URL: {url}")
                print(f"Error: Could not parse Drive URL. Please provide a valid Google Drive folder or file URL.")
                print("Examples:")
                print("  - https://drive.google.com/drive/folders/1abc123def456")
                print("  - https://drive.google.com/drive/u/0/my-drive")
                print("  - https://drive.google.com/drive/d/1abc123def456")
                sys.exit(1)

            # Get the last sync time
            last_sync_time = get_last_sync_time(output_folder_path)
            
            # Load the document database
            doc_db = load_document_database(output_folder_path)
            
            # Process documents and update the database
            doc_db = process_documents(service, last_sync_time, doc_db, target_id, target_type, output_folder_path=output_folder_path, output_folder_name=output_folder_name)
            
        else :

            output_folder_name = 'personal_drive'
            output_folder_path = os.path.join(os.getcwd(), SYNCED_CONTENT_FOLDER_NAME, output_folder_name)
            os.makedirs(output_folder_path, exist_ok=True)
        
            # Get the last sync time
            last_sync_time = get_last_sync_time(output_folder_path)
            
            # Load the document database
            doc_db = load_document_database(output_folder_path)
            
            # Process documents and update the database
            doc_db = process_documents(service, last_sync_time, doc_db, output_folder_path=output_folder_path, output_folder_name=output_folder_name)
            

        # Here you would add code to feed the output_file into your AI system
        # For example:
        # ai_system.process_document(output_file)
        
    except Exception as e:
        logging.error(f"Sync failed: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"Sync failed: {str(e)}")
        print("See log for details.")

if __name__ == '__main__':
    main()