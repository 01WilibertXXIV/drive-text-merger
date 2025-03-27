import os
import datetime
import json
import hashlib
import sys
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import traceback
import logging
import time
import datetime
import threading
from queue import Queue, Empty
from collections import defaultdict

from helpers.drive_utils import get_name_for_id, parse_drive_url
from helpers.auth_utils import get_drive_service
from helpers.text_utils import extract_text_from_docx, extract_text_from_pdf
from helpers.thread_utils import get_all_subfolders_multithreaded

# Set up logging
logging.basicConfig(filename='drive_sync.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Database file to store document metadata and content
DATA_FOLDER = '.data'
DOCUMENT_DB = 'document_database.json'


start_time_string = datetime.datetime.now()
START_TIME = time.time()

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



def process_documents_multithreaded(service, start_time, doc_db, target_id=None, target_type=None, 
                                  output_folder_path=None, output_folder_name=None, 
                                  max_workers=8, throttle_delay=0.05):
    """
    Multithreaded version of process_documents with sequential console output
    """
    # Track the current time for the next sync point
    current_time = datetime.datetime.now(datetime.UTC).isoformat() + 'Z'
    
    # Initialize counters with thread-safe dictionaries
    counters = {
        'changes_processed': 0,
        'files_updated': 0,
        'files_deleted': 0
    }
    counter_lock = threading.Lock()
    
    # Thread-safe API call limiter
    api_lock = threading.RLock()
    
    # Set up adaptive throttling variables
    adaptive_throttling = {
        'current_delay': throttle_delay,
        'min_delay': 0.01,
        'max_delay': 0.5,
        'last_error_time': 0
    }
    delay_lock = threading.Lock()
    
    # Print initial messages
    print()
    if(start_time == "1970-01-01T00:00:00.000Z"):
        print(f"First time running this script, building database from scratch with multithreading. \nThis may take a while...")
    else:
        logging.info(f"Starting multithreaded sync from {start_time}")
        print(f"Starting multithreaded sync from last sync time: {start_time}")
    print()

    if target_id:
        logging.info(f"Target {target_type} ID: {target_id}")
     
    # Get all docs that have been trashed/deleted since last sync
    existing_file_ids = set(doc_db["documents"].keys())
    active_file_ids = set()
    active_files_lock = threading.Lock()
    
    # Prepare list of folder IDs to search
    folder_ids_to_search = [target_id]
    
    # If a specific folder is targeted, get all its subfolders
    if target_id and target_type == 'folder':
        subfolders = get_all_subfolders_multithreaded(
            service, 
            target_id, 
            max_workers=max_workers, 
            throttle_delay=throttle_delay,
            batch_size=5,
            throttle_strategy="adaptive"
        )
        folder_ids_to_search.extend([folder['id'] for folder in subfolders])
        
        logging.info(f"Found {len(subfolders)} subfolders")
        print(f"Found {len(subfolders)} subfolders")
    
    # Flag to signal threads to exit
    shutdown_flag = threading.Event()
    
    # Master console lock
    console_lock = threading.Lock()
    
    # Data structures for collecting results before display
    folder_data = {}  # Will store folder -> files mapping
    folder_data_lock = threading.Lock()
    folder_order = []  # Will maintain ordered folder list
    folder_order_lock = threading.Lock()
    
    # Helper function for API throttling
    def throttled_api_call(api_func):
        """Throttle API calls to prevent overwhelming the server"""
        nonlocal adaptive_throttling
        
        # Get current delay
        with delay_lock:
            current_delay = adaptive_throttling['current_delay']
            # Gradually reduce delay if no errors recently
            time_since_error = time.time() - adaptive_throttling['last_error_time']
            if time_since_error > 10 and current_delay > adaptive_throttling['min_delay']:
                adaptive_throttling['current_delay'] = max(
                    adaptive_throttling['min_delay'], 
                    current_delay * 0.95  # Reduce by 5%
                )
        
        # Use API lock with delay
        with api_lock:
            # Apply throttling delay
            time.sleep(current_delay)
            
            try:
                result = api_func()
                return result
            except Exception as e:
                # On error, increase delay
                with delay_lock:
                    adaptive_throttling['last_error_time'] = time.time()
                    adaptive_throttling['current_delay'] = min(
                        adaptive_throttling['max_delay'],
                        current_delay * 1.5  # Increase by 50%
                    )
                
                logging.error(f"API error: {str(e)}")
                raise
    
    # Create queues for folders and files
    folder_queue = Queue()
    file_queue = Queue()
    folder_complete_queue = Queue()  # Signals when a folder is fully scanned
    
    # Add initial folders to the queue
    for i, folder_id in enumerate(folder_ids_to_search):
        if folder_id == "my-drive" or folder_id == "u/0/my-drive":
            folder_id = "root"
        folder_queue.put((folder_id, i + 1, len(folder_ids_to_search)))
    
    # Output display thread - handles all console output in a sequential manner
    def output_display_thread():
        """Thread that handles all console output in order"""
        folders_displayed = 0
        
        while not shutdown_flag.is_set() or folders_displayed < len(folder_ids_to_search):
            try:
                # Get a completed folder from the queue
                try:
                    folder_id, folder_name, folder_number, total_folders = folder_complete_queue.get(timeout=0.5)
                except Empty:
                    # If nothing to display, check if we should exit
                    if shutdown_flag.is_set() and folder_queue.empty() and file_queue.empty():
                        break
                    continue
                
                # Get files for this folder
                with folder_data_lock:
                    if folder_id not in folder_data:
                        # Skip if we don't have data for this folder
                        continue
                    
                    # Get folder files
                    folder_info = folder_data[folder_id]
                    files = folder_info.get('files', [])
                
                # Display folder header
                with console_lock:
                    print(f"({folder_number}/{total_folders}) - Searching in {BOLD_YELLOW}{folder_name}{RESET}")
                    
                    # Display file count
                    if len(files) == 0:
                        print(f"  No doc, pdf, or docx files found in this folder")
                    else:
                        print(f"  Found {len(files)} doc, pdf, or docx files")
                    
                    # Display files
                    for file in files:
                        if file.get('status') == 'unchanged':
                            print(f"  ↳ {BOLD_CYAN}{file['name']}{RESET} - No changes detected. Skipping!")
                        elif file.get('status') == 'updated':
                            print(f"  ↳ {BOLD_CYAN}{file['name']}{RESET} - {GREEN}Updated!{RESET}")
                        elif file.get('status') == 'error':
                            print(f"  ↳ {BOLD_CYAN}{file['name']}{RESET} - Error: {file.get('error', 'Unknown error')}")
                
                folders_displayed += 1
                
                # If we've displayed all folders, print completion message
                if folders_displayed >= len(folder_ids_to_search):
                    print("\nAll folders and files processed.")
            
            except Exception as e:
                logging.error(f"Output display thread error: {str(e)}")
                logging.error(traceback.format_exc())
    
    # Folder scanner thread function
    def folder_scanner_thread():
        """Thread for scanning folders and finding files to process"""
        while not shutdown_flag.is_set():
            try:
                # Get a folder from the queue with a timeout
                try:
                    folder_id, folder_number, total_folders = folder_queue.get(timeout=0.5)
                except Empty:
                    # If queue is empty, continue checking for shutdown
                    continue
                
                try:
                    # Get folder name
                    folder_name = throttled_api_call(lambda: get_name_for_id(service, file_id=folder_id))
                    
                    # Initialize folder data structure
                    with folder_data_lock:
                        folder_data[folder_id] = {
                            'name': folder_name,
                            'number': folder_number,
                            'total': total_folders,
                            'files': []
                        }
                    
                    # Construct query for this folder - using your exact query
                    query = (
                        "(mimeType='application/vnd.google-apps.document' OR "
                        "mimeType='application/pdf' OR "
                        "mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document') "
                        "and not name contains '.docm' "
                        f"and '{folder_id}' in parents"
                    )
                    
                    page_token = None
                    folder_files = []  # Collect all files before processing
                    
                    # First, collect all files in this folder across all pages
                    while not shutdown_flag.is_set():
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
                        
                        # Use throttled API call
                        results = throttled_api_call(lambda: service.files().list(**list_params).execute())
                        
                        items = results.get('files', [])
                        folder_files.extend(items)
                        
                        logging.info(f"Found {len(items)} files in {folder_name}")
                        
                        # Get next page of results
                        page_token = results.get('nextPageToken')
                        if not page_token:
                            break
                    
                    # Process each file found in this folder
                    for item in folder_files:
                        file_id = item['id']
                        
                        # Add to active files set
                        with active_files_lock:
                            active_file_ids.add(file_id)
                        
                        # Check if this file is new or modified since last sync
                        if (file_id not in doc_db["documents"] or 
                            item['modifiedTime'] > start_time):
                            with counter_lock:
                                counters['changes_processed'] += 1
                            
                            # Add to file queue for processing
                            file_queue.put((
                                item, 
                                folder_id, 
                                folder_name, 
                                folder_number, 
                                total_folders
                            ))
                        else:
                            # File unchanged - add to folder data structure
                            file_name = item['name']
                            with folder_data_lock:
                                folder_data[folder_id]['files'].append({
                                    'id': file_id,
                                    'name': file_name,
                                    'status': 'unchanged'
                                })
                    
                    # Signal that a folder is ready for display only if it has only unchanged files
                    # (Or if it has no files at all)
                    has_changed_files = False
                    for item in folder_files:
                        if (item['id'] not in doc_db["documents"] or 
                            item['modifiedTime'] > start_time):
                            has_changed_files = True
                            break
                    
                    if not has_changed_files:
                        # All files unchanged or no files, can display immediately
                        folder_complete_queue.put((folder_id, folder_name, folder_number, total_folders))
                
                except Exception as e:
                    logging.error(f"Error scanning folder {folder_id}: {str(e)}")
                    logging.error(traceback.format_exc())
                    
                    # Signal folder is complete even if there was an error
                    try:
                        folder_complete_queue.put((folder_id, folder_name, folder_number, total_folders))
                    except:
                        pass
            
            except Exception as e:
                logging.error(f"Folder scanner thread error: {str(e)}")
                logging.error(traceback.format_exc())
    
    # File processor thread function
    def file_processor_thread():
        """Thread for processing individual files"""
        while not shutdown_flag.is_set():
            try:
                # Get a file from the queue with a timeout
                try:
                    item, folder_id, folder_name, folder_number, total_folders = file_queue.get(timeout=0.5)
                except Empty:
                    # If queue is empty, continue checking for shutdown
                    continue
                
                file_id = item['id']
                file_name = item['name']
                mime_type = item['mimeType']
                file_status = None  # Will be set to 'updated' or 'error'
                error_message = None
                
                try:
                    logging.info(f"Processing file: {file_name} ({file_id}) - {mime_type}")
                    
                    # For Google Docs, we need to export as DOCX
                    export_params = {
                        'fileId': file_id,
                    }
                    
                    # Use throttled API call to get file content
                    def get_file_request():
                        if mime_type == 'application/vnd.google-apps.document':
                            export_params['mimeType'] = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                            return service.files().export_media(**export_params)
                        else:
                            return service.files().get_media(fileId=file_id, supportsAllDrives=True)
                    
                    request = throttled_api_call(get_file_request)
                    
                    # Download the file content
                    file_data = io.BytesIO()
                    downloader = MediaIoBaseDownload(file_data, request)
                    done = False
                    
                    while not done and not shutdown_flag.is_set():
                        status, done = downloader.next_chunk()
                    
                    # Extract text based on file type
                    if mime_type == 'application/vnd.google-apps.document' or mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                        text = extract_text_from_docx(file_data.getvalue())
                    elif mime_type == 'application/pdf':
                        text = extract_text_from_pdf(file_data.getvalue())
                    else:
                        text = f"Unsupported format: {mime_type} for file {file_name}"
                    
                    # Compute checksum to check if content actually changed
                    checksum = compute_checksum(text)
                    
                    # Thread-safe update of the document database
                    doc_updated = False
                    doc_db_lock = threading.Lock()
                    
                    with doc_db_lock:
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
                            doc_updated = True
                        else:
                            # Just update the lastSynced time
                            doc_db["documents"][file_id]["lastSynced"] = current_time
                    
                    if doc_updated:
                        with counter_lock:
                            counters['files_updated'] += 1
                    
                    # Set status as updated
                    file_status = 'updated'
                
                except Exception as e:
                    logging.error(f"Error processing file {file_id}: {str(e)}")
                    logging.error(traceback.format_exc())
                    file_status = 'error'
                    error_message = str(e)
                
                finally:
                    # Update the folder data structure with this file's status
                    with folder_data_lock:
                        if folder_id in folder_data:
                            file_info = {
                                'id': file_id,
                                'name': file_name,
                                'status': file_status
                            }
                            if error_message:
                                file_info['error'] = error_message
                            
                            folder_data[folder_id]['files'].append(file_info)
                            
                            # Check if all files for this folder are processed
                            all_files_processed = True
                            file_ids_in_folder = set()
                            
                            # Get all file IDs that should be in this folder
                            for i in range(file_queue.qsize()):
                                try:
                                    qitem = file_queue.get(timeout=0.1)
                                    if qitem[1] == folder_id:  # If folder_id matches
                                        all_files_processed = False
                                    file_queue.put(qitem)  # Put it back
                                except Empty:
                                    break
                            
                            # If all files for this folder are processed, signal folder completion
                            if all_files_processed:
                                folder_complete_queue.put((
                                    folder_id, 
                                    folder_data[folder_id]['name'], 
                                    folder_data[folder_id]['number'], 
                                    folder_data[folder_id]['total']
                                ))
            
            except Exception as e:
                logging.error(f"File processor thread error: {str(e)}")
                logging.error(traceback.format_exc())
    
    # Start threads
    folder_threads = []
    file_threads = []
    
    try:
        # Divide workers between folder scanning, file processing and output display
        folder_thread_count = max(2, max_workers // 4)  # 25% for folders
        file_thread_count = max_workers - folder_thread_count - 1  # Save 1 for output
        
        print(f"Starting {folder_thread_count} folder scanners and {file_thread_count} file processors")
        
        # Start output display thread first
        output_thread = threading.Thread(target=output_display_thread)
        output_thread.daemon = True
        output_thread.start()
        
        # Start folder scanner threads
        for _ in range(folder_thread_count):
            thread = threading.Thread(target=folder_scanner_thread)
            thread.daemon = True
            thread.start()
            folder_threads.append(thread)
        
        # Start file processor threads
        for _ in range(file_thread_count):
            thread = threading.Thread(target=file_processor_thread)
            thread.daemon = True
            thread.start()
            file_threads.append(thread)
        
        # Monitoring loop - wait for both queues to be empty
        while not shutdown_flag.is_set():
            # Check if both queues are empty
            if folder_queue.empty() and file_queue.empty():
                # Wait a bit to make sure nothing new is added
                time.sleep(2)
                
                # Check again
                if folder_queue.empty() and file_queue.empty():
                    # Wait for the output thread to finish displaying everything
                    while folder_complete_queue.qsize() > 0:
                        time.sleep(0.5)
                    
                    shutdown_flag.set()
                    break
            
            # Print periodic stats to log
            with counter_lock:
                files_processed = counters['files_updated']
                changes_processed = counters['changes_processed']
            
            queue_stats = f"Folder queue: {folder_queue.qsize()}, File queue: {file_queue.qsize()}"
            logging.info(f"Progress: {changes_processed} changes, {files_processed} files updated. {queue_stats}")
            
            # Short sleep
            time.sleep(2)
        
    except KeyboardInterrupt:
        print("\nUser interrupted process")
    except Exception as e:
        logging.error(f"Error in sync process: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"Error in sync process: {str(e)}")
    finally:
        # Signal all threads to exit
        shutdown_flag.set()
        
        # Wait for threads to finish
        output_thread.join(timeout=5)
        for thread in folder_threads + file_threads:
            thread.join(timeout=2)
    
    # Print a divider
    print("-" * 80)
    
    # Identify deleted files - only if no specific target was provided
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
                with counter_lock:
                    counters['files_deleted'] += 1
    
    # Update the database metadata
    doc_db["metadata"]["last_updated"] = current_time
    doc_db["metadata"]["total_documents"] = len(doc_db["documents"])
    doc_db["metadata"]["active_documents"] = len([doc for doc_id, doc in doc_db["documents"].items() if not doc.get("deleted", False)])
    
    # Save the document database
    save_document_database(doc_db, output_folder_path)
    
    # Generate the merged file with all content
    generate_merged_file(doc_db, current_time, counters['files_updated'], counters['files_deleted'], output_folder_path, output_folder_name)
    
    # Update the last sync time
    save_last_sync_time(current_time, output_folder_path)
    
    logging.info(f"Sync completed. Processed {counters['changes_processed']} changes, updated {counters['files_updated']} files, deleted {counters['files_deleted']} files.")
    print(f"Sync completed. Processed {counters['changes_processed']} changes, updated {counters['files_updated']} files, deleted {counters['files_deleted']} files.")
    
    return doc_db

def generate_merged_file(doc_db, timestamp, files_updated, files_deleted, output_folder_path=None, output_folder_name=None):
    """
    Generate merged files with all active documents, limiting each file to 200MB OR 400,000 words,
    whichever comes first.
    """
    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # Maximum file size (200MB in bytes) and word count (400,000 words)
    MAX_FILE_SIZE = 200 * 1024 * 1024
    MAX_WORD_COUNT = 400000
    
    # List to keep track of all generated files
    generated_files = []

    duration = datetime.datetime.now() - start_time_string
    hours, remainder = divmod(int(duration.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Prepare header content
    header = "=" * 50 + "\n"
    header += f"Sync Completed - Generated on {timestamp}\n"
    header += f"Operation took {hours:02d}:{minutes:02d}:{seconds:02d}\n"
    header += "=" * 50 + "\n"
    header += f"\nTotal documents: {doc_db['metadata']['total_documents']}\n"
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

    current_file_name = f"{timestamp_str}_{output_folder_name}_part{file_index}.txt"
    
    # Create the first file in the specified output folder path
    current_file_path = os.path.join(output_folder_path, current_file_name)
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
        
        # Check if adding this document would exceed either limit
        if (current_file_size + doc_size > MAX_FILE_SIZE or 
            current_word_count + total_doc_word_count > MAX_WORD_COUNT):
            # Close current file
            current_file.close()
            
            # Log which limit was reached
            if current_file_size + doc_size > MAX_FILE_SIZE:
                limit_reason = "file size limit (200MB)"
            else:
                limit_reason = f"word count limit ({MAX_WORD_COUNT} words)"
            
            logging.info(f"Reached {limit_reason} for {current_file_path}")
            print(f"Reached {limit_reason} for {current_file_path}")
            
            # Create a new file
            file_index += 1
            document_part_name = f"{timestamp_str}_{output_folder_name}_part{file_index}.txt" 
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
            print(f" ↳ {current_file_name}")
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
    print("Welcome to the Google Drive Merge Script")
    print("")
    print("This script will merge your Google Drive documents into a single file.")
    print("It will create a new folder in the current working directory called 'merged_content'.")
    print("It will then create a new folder inside 'synced_content' with the name of the folder you want to merge from Google Drive.")
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
            doc_db = process_documents_multithreaded(
                service, 
                last_sync_time, 
                doc_db, 
                target_id=target_id, 
                target_type=target_type, 
                output_folder_path=output_folder_path, 
                output_folder_name=output_folder_name,
                max_workers=12,          # Adjust based on your system capabilities
                throttle_delay=0.05      # Starting delay between API calls
            )
                        
        else :

            output_folder_name = 'personal_drive'
            output_folder_path = os.path.join(os.getcwd(), SYNCED_CONTENT_FOLDER_NAME, output_folder_name)
            os.makedirs(output_folder_path, exist_ok=True)
        
            # Get the last sync time
            last_sync_time = get_last_sync_time(output_folder_path)
            
            # Load the document database
            doc_db = load_document_database(output_folder_path)
            
            # Process documents and update the database
            doc_db = process_documents_multithreaded(
                service, 
                last_sync_time, 
                doc_db, 
                target_id=target_id, 
                target_type=target_type, 
                output_folder_path=output_folder_path, 
                output_folder_name=output_folder_name,
                max_workers=12,          # Adjust based on your system capabilities
                throttle_delay=0.05      # Starting delay between API calls
            )
            

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