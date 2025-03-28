import os
import json
import hashlib
import traceback
import time
import datetime
from googleapiclient.http import MediaIoBaseDownload
import io
import logging
import time
import threading
from queue import Queue, Empty
import subprocess

from constants.colors import RESET, BOLD_CYAN, YELLOW, GREEN, DARK_GRAY
from constants.app_data import DATA_FOLDER, DOCUMENT_DB_FILE, APP_NAME
from constants.time_data import START_TIME, START_TIME_STRING

from helpers.drive_utils import get_name_for_id
from helpers.sync_utils import save_last_sync_time, compute_checksum
from helpers.text_utils import extract_text_from_docx, extract_text_from_pdf
from helpers.sheet_utils import extract_complete_sheet_text
from helpers.messages.outro import print_outro

# Set up logging
logging.basicConfig(filename='drive_sync.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def ensure_data_folder(output_folder_path):
    """Ensure the data folder exists and is hidden on Windows."""
    data_folder_path = os.path.join(output_folder_path, DATA_FOLDER)
    
    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)
        
        # Hide the folder on Windows
        if os.name == "nt":
            subprocess.call(["attrib", "+H", data_folder_path])

def load_document_database(output_folder_path):
    """Load the document database from file."""
    ensure_data_folder(output_folder_path)  # Ensure the folder exists and is hidden
    
    db_file_path = os.path.join(output_folder_path, DATA_FOLDER, DOCUMENT_DB_FILE)
    
    if os.path.exists(db_file_path):
        with open(db_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    return {"documents": {}, "metadata": {"last_updated": ""}}

def save_document_database(db, output_folder_path):
    """Save the document database to file."""
    with open(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/{DOCUMENT_DB_FILE}"), 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


#region Process Documents
def process_documents(service, start_time, doc_db, target_id=None, target_type=None, output_folder_path=None, output_folder_name=None):
    """
    Enhanced process_documents to recursively search through all subfolders
    """
    # Get list of all changes since the last sync
    changes_processed = 0
    files_updated = 0
    files_deleted = 0
    total_download_bandwidth = 0
    
    # Track the current time for the next sync point
    current_time = datetime.datetime.now(datetime.UTC).isoformat() + 'Z'
    
    print()
    if(start_time == "1970-01-01T00:00:00.000Z"):
        print(f"First time running this script, building database from scratch. \nThis may take a while...")
    else:
        try:
            # Handle different possible time string formats
            if 'Z' in start_time and '+' in start_time:
                # If we have both Z and +00:00, remove the Z as it's redundant
                start_time = start_time.replace('Z', '')
                # Parse with the +00:00 timezone format
                utc_time = datetime.datetime.fromisoformat(start_time)
            elif 'Z' in start_time:
                # Format like "2023-01-01T12:00:00.000Z"
                utc_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            elif '+' in start_time or '-' in start_time[10:]:  # Check for timezone marker after date
                # Format like "2023-01-01T12:00:00.000+00:00"
                utc_time = datetime.datetime.fromisoformat(start_time)
            else:
                # Assume UTC if no timezone specified
                utc_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%f")
                utc_time = utc_time.replace(tzinfo=datetime.timezone.utc)
            
            # Convert to local timezone
            local_time = utc_time.astimezone(tz=None)
            
            # Format for display (includes date and time with timezone info)
            formatted_local_time = local_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            
            logging.info(f"Starting sync from {start_time} (UTC)")
            logging.info(f"Local time: {formatted_local_time}")
            
            print(f"Starting sync from: {YELLOW}{formatted_local_time}{RESET}")
        except (ValueError, TypeError) as e:
            # Handle case where the string format is different than expected
            logging.warning(f"Could not parse time string '{start_time}': {e}")
            print(f"Starting sync from last sync time: {YELLOW}{start_time}{RESET}")
            print(f"{YELLOW}Note:{RESET} Time format could not be converted to local timezone")
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
        subfolders = get_all_subfolders_multithreaded(
            service, 
            target_id, 
            max_workers=8, 
            throttle_delay=0.1,
            batch_size=5,
            throttle_strategy="adaptive"
            )
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
                "mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document' OR "
                "mimeType='application/vnd.google-apps.spreadsheet' OR "
                "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' OR "
                "mimeType='text/csv') "
                "and not name contains '.docm' "
                f"and '{search_folder_id}' in parents"
            )


            
            page_token = None
            while True:
                list_params = {
                    'q': query,
                    'pageSize': 100,
                    'fields': "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, webViewLink)",
                    'spaces': 'drive',
                    'supportsAllDrives': True,
                    'includeItemsFromAllDrives': True
                }
                
                if page_token:
                    list_params['pageToken'] = page_token
                
                results = service.files().list(**list_params).execute()

                folder_name = get_name_for_id(service, file_id=search_folder_id)

                print("\n")
                print(f"({BOLD_CYAN}{subfolders_count}{RESET}/{len(folder_ids_to_search)}) - Searching in {BOLD_CYAN}{folder_name}{RESET}                    ")
                
                items = results.get('files', [])
                logging.info(f"Found {len(items)} files in {folder_name}")

                if(len(items) == 0):
                    print(f"  {DARK_GRAY}No doc, pdf, or docx files found in this folder{RESET}")
                else:
                    print(f"  Found {YELLOW}{len(items)}{RESET} doc, pdf, or docx files")

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

                            logging.info(f"Processing file: {file_name} ({file_id}) - {mime_type}")
                            print(f"  ↳ {YELLOW}{file_name}{RESET} - Processing...                                      ", end="", flush=True)

                            if file_name == "2x2":
                                logging.info(f"Starting download for 2x2 file: {file_id}")
                                
                                while not done:
                                    try:
                                        status, done = downloader.next_chunk()
                                        logging.info(f"2x2 download progress: {int(status.progress() * 100)}%")
                                    except Exception as e:
                                        logging.error(f"Error during 2x2 download: {str(e)}")
                                        raise  

                            while not done:
                                status, done = downloader.next_chunk()
                                print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {int(status.progress() * 100)}%                            ", end="", flush=True)
                            print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {GREEN}Updated!{RESET}                                       ") 

                            # Add downloaded bytes to total bandwidth
                            file_data_size = len(file_data.getvalue())
                            total_download_bandwidth += file_data_size
                            logging.info(f"Downloaded {file_data_size} bytes for {file_name}")
                            
                            elapsed_time = time.time() - START_TIME
                            progress_percentage = (subfolders_count / len(folder_ids_to_search)) * 100

                            progress_bar_width = 36
                            filled_width = int(progress_percentage / 100 * progress_bar_width)
                            bar = '=' * filled_width + '-' * (progress_bar_width - filled_width)

                            # print(f'\r[{bar}] {progress_percentage:.1f}% | Elapsed: {elapsed_time:.2f}s', end='\r', flush=True)

                            file_url = item.get("webViewLink", "N/A")

                            # Extract text based on file type
                            if mime_type == 'application/vnd.google-apps.document' or mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                                text = extract_text_from_docx(file_data.getvalue(), file_url)
                            elif mime_type == 'application/pdf':
                                text = extract_text_from_pdf(file_data.getvalue(), file_url)
                            elif mime_type in [
                                'application/vnd.google-apps.spreadsheet',
                                'application/vnd.ms-excel',
                                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                'text/csv',
                                'application/csv'
                            ]:
                                text = extract_complete_sheet_text(file_data.getvalue(), file_name, file_url)
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
                                    "url": item.get("webViewLink", "N/A"),
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
                                doc_db["documents"][file_id]["url"] = item.get("webViewLink", "N/A")

                            processed_files_count += 1
                         
                        except Exception as e:
                            logging.error(f"Error processing file {file_id}: {str(e)}")
                            logging.error(traceback.format_exc())
                            # print(f"Error processing file: {str(e)}")
                            # print(f"Error processing file: {file_name} ({file_id}) - {mime_type}")

                    else:
                        file_name = item['name']
                        print(f"  ↳ {YELLOW}{file_name}{RESET} - {DARK_GRAY}No changes detected. Skipping!{RESET}                                           ")

                    elapsed_time = time.time() - START_TIME
                    progress_percentage = (subfolders_count / len(folder_ids_to_search)) * 100

                    progress_bar_width = 36
                    filled_width = int(progress_percentage / 100 * progress_bar_width)
                    bar = '=' * filled_width + '-' * (progress_bar_width - filled_width)

                    # print(f'\r[{bar}] {progress_percentage:.1f}% | Elapsed: {elapsed_time:.2f}s', end='\r', flush=True)

                # if(processed_files_count != files_to_process):
                #     print(f"  {files_to_process - processed_files_count} files did not require an update.")

                # print(" " * 100)   
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break

            elapsed_time = time.time() - START_TIME
            progress_percentage = (subfolders_count / len(folder_ids_to_search)) * 100

            progress_bar_width = 36
            filled_width = int(progress_percentage / 100 * progress_bar_width)
            bar = '=' * filled_width + '-' * (progress_bar_width - filled_width)
            
            # print(f'\r[{bar}] {progress_percentage:.1f}% | Elapsed: {elapsed_time:.2f}s', end='\r', flush=True)
            #sys.stdout.write(f'\r[{bar}] {progress_percentage:.1f}% | Elapsed: {elapsed_time:.2f}s')
            #sys.stdout.flush()

            subfolders_count += 1

        print()
        
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
    generate_merged_file(doc_db, current_time, files_updated, files_deleted, output_folder_path, output_folder_name, total_download_bandwidth)
    
    # Update the last sync time
    save_last_sync_time(current_time, output_folder_path)

    # duration = datetime.datetime.now() - start_time
    # hours, remainder = divmod(int(duration.total_seconds()), 3600)
    # minutes, seconds = divmod(remainder, 60)
    
    logging.info(f"Sync completed. Processed {changes_processed} changes, updated {files_updated} files, deleted {files_deleted} files.")
    # logging.info(f"Total operation time: {hours:02d}:{minutes:02d}:{seconds:02d}")

    # print(f"Total operation time: {hours:02d}:{minutes:02d}:{seconds:02d}")
    
    return doc_db
#endregion


#region Generate Merged File
def generate_merged_file(doc_db, timestamp, files_updated, files_deleted, output_folder_path=None, output_folder_name=None, total_download_bandwidth=0):
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
    # Dictionary to track file sizes
    file_sizes = {}
    # Dictionary to track word counts
    file_word_counts = {}
    # Variables to track totals
    total_size = 0
    total_word_count = 0

    duration = datetime.datetime.now() - START_TIME_STRING
    hours, remainder = divmod(int(duration.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Prepare header content
    header = f"Sync Completed - Generated on {timestamp}\n"
    header += f"Operation took {hours:02d}:{minutes:02d}:{seconds:02d}\n\n"
    header += f"Total documents: {doc_db['metadata']['total_documents']}\n"
    header += f"Active documents: {doc_db['metadata']['active_documents']}\n"
    header += f"Files updated in this sync: {files_updated}\n"
    header += f"Files deleted in this sync: {files_deleted}\n"
    
    # Initialize variables
    current_file = None
    current_file_path = None
    current_file_size = 0
    current_word_count = 0
    file_index = 1
    
    # Count header words
    header_word_count = len(header.split())

    current_file_name = f"{timestamp_str}_{output_folder_name}_part{file_index}.md"
    
    # Create the first file in the specified output folder path
    current_file_path = os.path.join(output_folder_path, current_file_name)
    current_file = open(current_file_path, 'w', encoding='utf-8')
    current_file.write(header)
    current_file_size = len(header.encode('utf-8'))
    current_word_count = header_word_count
    generated_files.append(current_file_path)

    index = 1

    # Write all active documents
    for file_id, doc_info in doc_db["documents"].items():
        # Skip deleted documents
        if doc_info.get("deleted", False):
            continue
            
        # Prepare document content
        doc_header = f"## METADATA ##\n"
        doc_header += f"Title: {doc_info['name']}\n"
        doc_header += f"URL: {doc_info['url']}\n"
        doc_header += f"Last Modified: {doc_info['modifiedTime']}\n"
        doc_content = doc_info["content"]
        
        # Calculate size of this document
        doc_size = len((doc_header + doc_content).encode('utf-8'))
        doc_word_count = len(doc_content.split())
        doc_header_word_count = len(doc_header.split())
        total_doc_word_count = doc_word_count + doc_header_word_count
        
        # Check if adding this document would exceed either limit
        if (current_file_size + doc_size > MAX_FILE_SIZE or 
            current_word_count + total_doc_word_count > MAX_WORD_COUNT):
            # Store final size and word count of current file before closing
            file_sizes[current_file_path] = current_file_size
            file_word_counts[current_file_path] = current_word_count
            total_size += current_file_size
            total_word_count += current_word_count
            
            # Close current file
            current_file.close()
            
            # Log which limit was reached
            if current_file_size + doc_size > MAX_FILE_SIZE:
                limit_reason = "file size limit (200MB)"
            else:
                limit_reason = f"word count limit ({MAX_WORD_COUNT} words)"
            
            logging.info(f"Reached {limit_reason} for {current_file_path}")
            
            # Create a new file
            file_index += 1
            document_part_name = f"{timestamp_str}_{output_folder_name}_part{file_index}.md" 
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
        current_file.write(f"\n```START OF FILE {index} ```\n")
        current_file.write(doc_header)
        current_file.write(doc_content)
        current_file.write(f"\n```END OF FILE {index} ```\n")

        index += 1
        
        # Update current file size and word count
        current_file_size += doc_size
        current_word_count += total_doc_word_count
    
    # Add the last file's size and word count to our tracking
    file_sizes[current_file_path] = current_file_size
    file_word_counts[current_file_path] = current_word_count
    total_size += current_file_size
    total_word_count += current_word_count
    
    # Close the last file
    current_file.close()
    
    # Log details about all generated files and total size
    logging.info(f"Generated {len(generated_files)} merged files: {', '.join(generated_files)}")
    

    print_outro(output_folder_path, file_sizes, file_word_counts, total_size, total_word_count, hours, minutes, seconds, total_download_bandwidth)

    return generated_files
#endregion



#region Multithreaded Subfolder Scanning
def get_all_subfolders_multithreaded(service, root_folder_id, max_workers=8, throttle_delay=0.05, 
                                    batch_size=5, throttle_strategy="adaptive"):
    """
    Get all subfolders using optimized multithreading.
    
    Args:
        service: Google Drive service object
        root_folder_id: ID of the root folder to scan
        max_workers: Maximum number of threads to use (default: 8)
        throttle_delay: Base delay between API calls in seconds (default: 0.05)
        batch_size: Number of folders each thread processes before yielding (default: 5)
        throttle_strategy: Strategy for throttling - "fixed", "adaptive", or "none" (default: "adaptive")
    
    Returns:
        List of dictionaries containing folder details
    """
    # Shared variables across threads
    subfolder_counter = {'count': 0}
    error_counter = {'count': 0}
    counter_lock = threading.Lock()
    error_lock = threading.Lock()
    all_subfolders = []
    all_subfolders_lock = threading.Lock()
    start_time = time.time()
    
    # Adaptive throttling variables
    current_delay = throttle_delay
    min_delay = 0.01
    max_delay = 0.5
    delay_lock = threading.Lock()
    last_error_time = 0
    
    # Thread-safe API call limiter
    api_lock = threading.RLock()  # Reentrant lock
    
    # Use a thread-safe set to track processed folders
    processed_folders = set()
    processed_folders_lock = threading.Lock()
    
    # Add root folder to processed set
    processed_folders.add(root_folder_id)
    
    # Create a thread-safe queue for pending folders
    folder_queue = Queue()
    folder_queue.put((root_folder_id, ''))  # (folder_id, parent_path)
    
    # Flag to signal threads to exit
    shutdown_flag = threading.Event()
    
    # Cap the max workers to a reasonable number
    max_workers = min(max_workers, 15)  # Cap at 15 threads max
    print(f"Starting scan with {max_workers} worker threads and {throttle_strategy} throttling (base delay: {throttle_delay}s)...")
    
    # Progress output thread
    def progress_reporter():
        last_count = 0
        last_update_time = time.time()
        no_progress_timer = 0
        last_stats_time = time.time()
        scan_speed = 0
        
        while not shutdown_flag.is_set():
            elapsed_time = time.time() - start_time
            hours, remainder = divmod(int(elapsed_time), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            with counter_lock:
                count = subfolder_counter['count']
            
            with error_lock:
                errors = error_counter['count']
            
            # Calculate scanning speed (folders per second)
            time_diff = time.time() - last_stats_time
            if time_diff >= 5:  # Update speed stats every 5 seconds
                count_diff = count - last_count
                scan_speed = count_diff / time_diff if time_diff > 0 else 0
                last_count = count
                last_stats_time = time.time()
            
            # Check if we're making progress for stall detection
            if count > last_count:
                last_update_time = time.time()
                no_progress_timer = 0
            else:
                no_progress_timer = time.time() - last_update_time
            
            # Get current throttle delay
            with delay_lock:
                delay = current_delay
                
            # Build status message
            queue_size = folder_queue.qsize()
            status = f"\rFolders: {BOLD_CYAN}{count + 1}{RESET} | Speed: {scan_speed:.1f}/s | Errors: {errors} | "
            status += f"Time: {hours:02d}:{minutes:02d}:{seconds:02d} | Queue: {queue_size} | Delay: {delay:.3f}s"
            
            # Add no-progress indicator if we've been stuck
            if no_progress_timer > 5:  # 5 seconds without progress
                status += f" | No progress: {int(no_progress_timer)}s"
                
                # If no progress for extended period and queue is empty, we might be done
                if no_progress_timer > 30 and queue_size == 0:
                    print(f"\nNo progress for {int(no_progress_timer)} seconds and queue is empty. Process may be complete.")
                    shutdown_flag.set()  # Signal threads to exit
                    break

            status += " " * 20
            
            print(status, end='', flush=True)
            time.sleep(0.5)
    
    # Start progress reporter thread
    progress_thread = threading.Thread(target=progress_reporter)
    progress_thread.daemon = True
    progress_thread.start()
    
    def throttled_api_call(api_func):
        """Throttle API calls based on strategy"""
        nonlocal current_delay, last_error_time
        
        # Determine if we need throttling
        if throttle_strategy == "none":
            return api_func()
        
        if throttle_strategy == "adaptive":
            # Reduce delay gradually over time if no errors
            with delay_lock:
                time_since_error = time.time() - last_error_time
                if time_since_error > 10 and current_delay > min_delay:
                    current_delay = max(min_delay, current_delay * 0.95)  # Reduce by 5%
                delay = current_delay
        else:  # "fixed"
            delay = throttle_delay
        
        # Use API lock with delay
        with api_lock:
            # Apply throttling delay
            time.sleep(delay)
            
            try:
                result = api_func()
                return result
            except Exception as e:
                # On error, increase delay if using adaptive strategy
                if throttle_strategy == "adaptive":
                    with delay_lock:
                        last_error_time = time.time()
                        current_delay = min(max_delay, current_delay * 1.5)  # Increase by 50%
                
                with error_lock:
                    error_counter['count'] += 1
                
                raise
    
    def process_folder():
        """Worker function to process folders from the queue"""
        processed_count = 0
        
        while not shutdown_flag.is_set():
            batch_processed = 0
            
            while batch_processed < batch_size and not shutdown_flag.is_set():
                try:
                    # Get a folder from the queue with a timeout
                    try:
                        folder_id, parent_path = folder_queue.get(timeout=0.5)
                    except Empty:
                        # If nothing in queue, break batch processing
                        break
                    
                    # Query to get all subfolders
                    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
                    
                    try:
                        page_token = None
                        while True and not shutdown_flag.is_set():
                            # Use throttled API call
                            results = throttled_api_call(lambda: service.files().list(
                                q=query,
                                spaces='drive',
                                fields='nextPageToken, files(id, name, parents)',
                                pageToken=page_token,
                                pageSize=100,  # Get more items per request
                                supportsAllDrives=True,
                                includeItemsFromAllDrives=True
                            ).execute())
                            
                            subfolders_batch = []
                            for folder in results.get('files', []):
                                folder_id = folder['id']
                                
                                # Check if we've already processed this folder to avoid cycles
                                with processed_folders_lock:
                                    if folder_id in processed_folders:
                                        continue
                                    processed_folders.add(folder_id)
                                
                                # Construct full path
                                full_path = f"{parent_path}/{folder['name']}" if parent_path else folder['name']
                                
                                # Create folder entry
                                folder_entry = {
                                    'id': folder_id,
                                    'name': folder['name'],
                                    'path': full_path
                                }
                                
                                # Add to batch
                                subfolders_batch.append(folder_entry)
                                
                                # Add this folder to the queue for processing its subfolders
                                folder_queue.put((folder_id, full_path))
                            
                            # Update shared counters and lists
                            if subfolders_batch:
                                with counter_lock:
                                    subfolder_counter['count'] += len(subfolders_batch)
                                
                                with all_subfolders_lock:
                                    all_subfolders.extend(subfolders_batch)
                            
                            page_token = results.get('nextPageToken')
                            if not page_token:
                                break
                    
                    except Exception as e:
                        print(f"\nError retrieving subfolders for {folder_id}: {str(e)}")
                    
                    # Increment batch counter
                    batch_processed += 1
                    processed_count += 1
                    
                except Exception as e:
                    print(f"\nWorker thread error: {str(e)}")
            
            # After processing a batch, give other threads a chance
            if batch_processed > 0:
                time.sleep(0.001)  # Tiny sleep to yield CPU
    
    # List to keep track of our threads
    worker_threads = []
    
    try:
        # Create and start worker threads
        for _ in range(max_workers):
            thread = threading.Thread(target=process_folder)
            thread.daemon = True
            thread.start()
            worker_threads.append(thread)
        
        # Main monitoring loop - check if all work is done
        max_empty_checks = 5
        empty_check_count = 0
        
        while not shutdown_flag.is_set():
            # Check if queue is empty
            if folder_queue.empty():
                empty_check_count += 1
                # Give threads a chance to add more to the queue
                time.sleep(0.5)
                
                # If queue remained empty for several checks, we're probably done
                if empty_check_count >= max_empty_checks:
                    print("\nQueue has been empty for consecutive checks. Process appears complete.")
                    shutdown_flag.set()
                    break
            else:
                # Reset counter if queue is not empty
                empty_check_count = 0
            
            # If no progress for a while, progress thread will set shutdown flag
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\nUser interrupted process")
    finally:
        # Signal all threads to exit
        shutdown_flag.set()
        
        # Give threads time to finish cleanly
        for thread in worker_threads:
            thread.join(timeout=2)
        
        # Stop the progress reporter
        progress_thread.join(timeout=1)
        print()  # Print newline after completion
        
        # Final stats
        elapsed_time = time.time() - start_time
        folders_per_second = subfolder_counter['count'] / elapsed_time if elapsed_time > 0 else 0
        
        print(f"Scan completed in {elapsed_time:.1f} seconds.")
        print(f"Found {BOLD_CYAN}{subfolder_counter['count'] + 1}{RESET} subfolders ({folders_per_second:.1f} folders/sec).")
        print(f"Encountered {error_counter['count']} errors.\n")
    
    return all_subfolders

#endregion