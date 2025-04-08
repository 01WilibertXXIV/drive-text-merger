import logging
import time
import datetime
from typing import Set, Dict, Any, Optional

# Import components from your refactored structure
from config import DEFAULT_START_TIME, QUERY_MIME_TYPES, PAGE_SIZE, DRIVE_FIELDS

from constants.colors import RESET, BOLD_CYAN, YELLOW, GREEN, DARK_GRAY, RED # Keep colors if desired
from src.drive import api_utils as drive_api
from src.drive import scanner as drive_scanner
from src.processing import chunker, checksum, extractor
from src.processing.document import ProcessedDocument # Assuming you use the dataclass
from src.storage.local_db import LocalDBManager
from src.storage.vector_store import MarqoManager
from utils import time_utils
from src.reporting import merged_file, stats # If generating report here

# Assume START_TIME and START_TIME_STRING are defined globally or passed if needed for stats
# Example: START_TIME = time.time()
# START_TIME_STRING = datetime.datetime.now()


def process_drive_documents(
    service: Any,  # Google Drive service object
    local_db_manager: LocalDBManager,
    marqo_manager: MarqoManager,
    output_folder_path: str,
    output_folder_name: Optional[str] = None, # For merged file generation
    target_id: Optional[str] = None,
    target_type: Optional[str] = None # e.g., 'folder', 'drive'
) -> Dict[str, Any]:
    """
    Orchestrates the Google Drive document synchronization process.

    Args:
        service: Authenticated Google Drive API service instance.
        local_db_manager: Instance managing the local JSON database.
        marqo_manager: Instance managing the Marqo vector store connection.
        output_folder_path: Path to the main output/data directory.
        output_folder_name: Base name for the generated merged file(s).
        target_id: Optional ID of a specific folder or drive ('root' for My Drive).
        target_type: Type of the target_id ('folder' or 'drive').

    Returns:
        A dictionary containing synchronization statistics.
    """
    sync_start_time = time.time()
    sync_start_time_str = time_utils.get_current_utc_iso_string()
    last_sync_time_str = local_db_manager.load_last_sync_time()

    # --- Initialization ---
    logging.info(f"Starting Drive sync process. Target: {target_id or 'All'}")
    print("\nStarting Document Synchronization...")

    if last_sync_time_str == DEFAULT_START_TIME:
        print(f"First sync or last sync time missing. Scanning all documents.")
        logging.info("Performing initial full scan.")
    else:
        try:
            # Attempt to parse and display the last sync time nicely
            last_sync_dt = time_utils.parse_iso_time_string(last_sync_time_str)
            formatted_local_time = time_utils.format_time_local(last_sync_dt)
            print(f"Syncing changes since: {YELLOW}{formatted_local_time}{RESET}")
            logging.info(f"Starting sync from {last_sync_time_str} (UTC)")
        except (ValueError, TypeError) as e:
            logging.warning(f"Could not parse last sync time '{last_sync_time_str}': {e}. Syncing from stored value.")
            print(f"Syncing changes since: {YELLOW}{last_sync_time_str}{RESET}")
            print(f"{YELLOW}Note:{RESET} Time format could not be converted to local timezone.")
    print()

    files_processed_count = 0
    files_updated_count = 0
    files_deleted_count = 0
    total_download_bandwidth = 0
    active_file_ids: Set[str] = set()
    errors_encountered = 0

    # --- Determine Folders to Scan ---
    folder_ids_to_search = []
    scan_entire_scope = not target_id # Assume full scan unless target is specified

    if target_id:
        # Handle specific targets like 'my-drive' aliases
        if target_id in ["my-drive", "u/0/my-drive"]:
             target_id = "root"
             target_type = 'drive' # Assume it's the root drive

        logging.info(f"Target specified: ID={target_id}, Type={target_type}")
        if target_type == 'folder':
            print(f"Scanning target folder '{BOLD_CYAN}{output_folder_name}{RESET}' and its subfolders...")
            try:
                # Use the refactored multithreaded scanner
                subfolders = drive_scanner.get_all_subfolders_multithreaded(
                    service=service,
                    root_folder_id=target_id,
                    # Pass config params here: max_workers, throttle_delay, etc.
                )
                folder_ids_to_search = [target_id] + [f['id'] for f in subfolders]
                print(f"Found {len(subfolders)} subfolders.")
                logging.info(f"Scanning folder {target_id} and {len(subfolders)} subfolders.")
                scan_entire_scope = False # Only scanning a subset
            except Exception as e:
                 logging.error(f"Failed to scan subfolders for {target_id}: {e}", exc_info=True)
                 print(f"{RED}Error scanning subfolders for {target_id}. Aborting targeted scan.{RESET}")
                 return {"error": "Subfolder scanning failed"}
        else: # Assume it's a drive ID or 'root'
             folder_ids_to_search = [target_id]
             print(f"Scanning target '{target_id}'...")
             logging.info(f"Scanning root/drive: {target_id}")
             # For a single drive/root target, we might still consider it a "full" scan of that scope
             scan_entire_scope = True # Let's assume targeting a drive implies checking deletions within it
    else:
        # No target specified, scan 'root' (My Drive) and all subfolders
        print("Scanning 'My Drive' and all subfolders...")
        try:
            subfolders = drive_scanner.get_all_subfolders_multithreaded(service=service, root_folder_id='root')
            folder_ids_to_search = ['root'] + [f['id'] for f in subfolders]
            print(f"Scanning 'My Drive' and {len(subfolders)} subfolders.")
            logging.info(f"Scanning 'root' and {len(subfolders)} subfolders found.")
        except Exception as e:
            logging.error(f"Failed to scan subfolders for 'root': {e}", exc_info=True)
            print(f"{RED}Error scanning subfolders in 'My Drive'. Proceeding with 'My Drive' only.{RESET}")
            folder_ids_to_search = ['root'] # Fallback to just root if scanning fails

    num_folders_total = len(folder_ids_to_search)
    processed_folder_count = 0

    # Ensure Marqo index exists before processing files
    if not marqo_manager.ensure_index_exists():
         print(f"{RED}Failed to ensure Marqo index '{marqo_manager.index_name}' exists. Aborting sync.{RESET}")
         logging.critical(f"Marqo index '{marqo_manager.index_name}' setup failed. Aborting.")
         return {"error": "Marqo index setup failed"}


    # --- Main Processing Loop ---
    for folder_id in folder_ids_to_search:
        processed_folder_count += 1
        page_token = None

        try:
            folder_name = drive_api.get_name_for_id(service, folder_id)
            print(f"\n({BOLD_CYAN}{processed_folder_count}{RESET}/{num_folders_total}) - Scanning Folder: {BOLD_CYAN}{folder_name or folder_id}{RESET}")
            logging.info(f"Scanning folder: {folder_name} ({folder_id})")
        except Exception as e:
            folder_name = folder_id # Fallback
            print(f"\n({BOLD_CYAN}{processed_folder_count}{RESET}/{num_folders_total}) - Scanning Folder ID: {BOLD_CYAN}{folder_id}{RESET} (Name lookup failed)")
            logging.warning(f"Failed to get name for folder {folder_id}: {e}")

        folder_file_count = 0
        while True: # Paginate through files in the current folder
            try:
                list_results = drive_api.list_files_in_folder(
                    service=service,
                    folder_id=folder_id,
                    mime_types=QUERY_MIME_TYPES, # Get from config
                    page_token=page_token,
                    page_size=PAGE_SIZE, # Get from config
                    fields=DRIVE_FIELDS # Get from config
                )
                items = list_results.get('files', [])
                page_token = list_results.get('nextPageToken')
                folder_file_count += len(items)

                if not items and page_token is None and folder_file_count == 0: # Only print if truly empty
                     print(f"  {DARK_GRAY}No relevant files found in this folder.{RESET}")
                     break # Exit while loop for this folder

                if not items and page_token is None: # End of files for this folder
                     break

                if items:
                     print(f"  Found {YELLOW}{len(items)}{RESET} file(s) on this page.")

            except Exception as api_error:
                logging.error(f"API error listing files in {folder_name} ({folder_id}): {api_error}", exc_info=True)
                print(f"  {RED}API Error listing files in {folder_name}. Skipping folder.{RESET}")
                errors_encountered += 1
                break # Skip to the next folder

            # --- Process Files in Current Page ---
            for item in items:
                file_id = item['id']
                file_name = item.get('name', 'Untitled')
                mime_type = item.get('mimeType', 'unknown')
                modified_time_str = item.get('modifiedTime')
                created_time_str = item.get('createdTime')
                file_url = item.get('webViewLink', '')

                active_file_ids.add(file_id)
                files_processed_count += 1

                print(f"  ↳ {YELLOW}{file_name}{RESET} ({mime_type}) - ", end="")

                # --- Check if Processing Needed ---
                doc_info = local_db_manager.get_document_info(file_id)
                local_mod_time = doc_info.get("modifiedTime") if doc_info else None

                # Process if new or modified time is more recent. Handle potential missing modifiedTime.
                should_process = (not doc_info or not local_mod_time or
                                 (modified_time_str and modified_time_str > local_mod_time))

                if should_process:
                    print(f"Processing...", end="", flush=True)
                    try:
                        # 1. Download/Export Content
                        content_bytes, downloaded_mime_type, size = drive_api.download_file_content(
                            service, file_id, mime_type
                        )
                        if content_bytes is None: # Handle download failure reported by the utility
                             print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {RED}Download Failed/Skipped.{RESET}    ")
                             logging.warning(f"Download failed or skipped for {file_name} ({file_id}).")
                             errors_encountered += 1
                             continue # Skip to next file
                        total_download_bandwidth += size
                        logging.info(f"Downloaded {size} bytes for {file_name} ({file_id}) as {downloaded_mime_type}")

                        # 2. Extract Text
                        text = extractor.extract_text(
                            content_bytes, downloaded_mime_type, file_name, file_url
                        )
                        if not text:
                            print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {RED}Text Extraction Failed.{RESET}        ")
                            logging.error(f"Text extraction failed for {file_name} ({file_id}) type {downloaded_mime_type}")
                            errors_encountered += 1
                            # Optionally store a marker in local DB? For now, skip.
                            continue

                        # 3. Compute Checksum
                        current_checksum = checksum.compute_checksum(text)
                        local_checksum = doc_info.get("checksum") if doc_info else None

                        # 4. Check if Content *Actually* Changed
                        if not local_checksum or current_checksum != local_checksum:
                            print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {GREEN}Content changed. Updating...{RESET}", end="", flush=True)

                            # 5. Chunk Text
                            chunks = chunker.chunk_document_text(text) # Use your chunking util
                            logging.info(f"Chunked '{file_name}' into {len(chunks)} chunks.")

                            # 6. Prepare ProcessedDocument data
                            processed_doc = ProcessedDocument(
                                id=file_id,
                                name=file_name,
                                url=file_url,
                                mime_type=mime_type, # Original type
                                modified_time_str=modified_time_str,
                                created_time_str=created_time_str,
                                downloaded_mime_type=downloaded_mime_type,
                                content=None, # Decide if needed after chunking
                                checksum=current_checksum,
                                chunks=chunks
                            )

                            # 7. Upsert to Vector Store
                            upsert_success = marqo_manager.upsert_document(processed_doc)

                            # 8. Update Local Document Database
                            local_db_manager.update_document(processed_doc, sync_start_time_str)

                            if upsert_success:
                                print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {GREEN}Updated & Stored in Vector DB!{RESET}            ")
                                files_updated_count += 1
                            else:
                                print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {RED}Updated Locally, Vector DB FAILED!{RESET}    ")
                                errors_encountered += 1
                                # Decide if local update should be reverted or flagged

                        else:
                            # Checksum is the same, content hasn't changed
                            print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {DARK_GRAY}No content change. Updating sync time.{RESET} ")
                            # Update only metadata like lastSynced time and potentially URL
                            local_db_manager.update_document_sync_time(file_id, sync_start_time_str, file_url)

                    except drive_api.DownloadPermissionsError as perm_error: # Catch specific error from api_utils
                         print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {RED}Permission Error: {perm_error}{RESET} ")
                         logging.warning(f"Permission error for {file_name} ({file_id}): {perm_error}")
                         errors_encountered += 1
                    except drive_api.DownloadAbuseError as abuse_error: # Catch specific error from api_utils
                         print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {RED}Download Blocked (Policy): {abuse_error}{RESET} ")
                         logging.warning(f"Download blocked for {file_name} ({file_id}): {abuse_error}")
                         errors_encountered += 1
                    except Exception as process_error:
                         print(f"\r  ↳ {YELLOW}{file_name}{RESET} - {RED}Processing Error: {process_error}{RESET}    ")
                         logging.error(f"Error processing {file_name} ({file_id}): {process_error}", exc_info=True)
                         errors_encountered += 1
                else:
                    # File exists and modifiedTime is not newer
                    print(f"{DARK_GRAY}No changes detected. Updating sync time.{RESET}")
                    # Update lastSynced time in local DB
                    local_db_manager.update_document_sync_time(file_id, sync_start_time_str, file_url)


            # --- End of file loop for the page ---
            if not page_token:
                break # Exit pagination loop for this folder

        # --- End of pagination loop for the folder ---
        time.sleep(0.1) # Small delay between folders

    # --- End of Folder Loop ---
    print("\nFinished scanning folders.")

    # --- Handle Deleted Files ---
    if scan_entire_scope: # Only check for deletions if a full scan was performed (or target was root/drive)
        logging.info("Performing deletion check...")
        print("Checking for deleted or moved files...")
        existing_ids = local_db_manager.get_all_document_ids()
        deleted_file_ids = existing_ids - active_file_ids

        if deleted_file_ids:
            print(f"Found {len(deleted_file_ids)} potentially deleted/moved files.")
            for file_id in deleted_file_ids:
                doc_info = local_db_manager.get_document_info(file_id)
                # Check if it exists and is NOT already marked as deleted
                if doc_info and not doc_info.get("deleted", False):
                    file_name = doc_info.get("name", "Unknown Name")
                    print(f"  Marking deleted: {YELLOW}{file_name}{RESET} ({file_id})")
                    logging.info(f"Marking file as deleted: {file_name} ({file_id})")

                    # Mark deleted in local DB
                    local_db_manager.mark_deleted(file_id, sync_start_time_str)

                    # Delete from Vector Store
                    delete_success = marqo_manager.delete_document(file_id)
                    if not delete_success:
                         print(f"  {RED}Failed to delete {file_name} from vector store.{RESET}")
                         logging.error(f"Failed to delete file {file_id} from vector store.")
                         errors_encountered += 1
                    else:
                         logging.info(f"Deleted chunks for file {file_id} from vector store.")

                    files_deleted_count += 1
        else:
             print("No deleted files detected in the scanned scope.")
    else:
        logging.info(f"Target ID ({target_id}) was specified for a folder. Skipping deletion check.")
        print(f"{DARK_GRAY}Targeted folder scan finished. Skipping deletion check outside this scope.{RESET}")


    # --- Finalize ---
    logging.info(f"Sync process finished. Updating database and saving state.")
    print("Finalizing sync...")

    # Update metadata and save local DB
    local_db_manager.set_last_updated_time(sync_start_time_str)
    local_db_manager.save() # This now also updates counts internally

    # Save the timestamp for the next run
    local_db_manager.save_last_sync_time(sync_start_time_str)

    sync_end_time = time.time()
    duration = sync_end_time - sync_start_time

    # --- Reporting (Optional: Generate merged file and print stats) ---
    if output_folder_path and output_folder_name:
         print(f"Generating merged output file(s) in '{output_folder_path}'...")
         # Pass the necessary data from the db manager
         generate_success = merged_file.generate_merged_file(
             doc_db=local_db_manager.db, # Pass the internal dict
             timestamp=sync_start_time_str, # Use the sync time
             files_updated=files_updated_count,
             files_deleted=files_deleted_count,
             output_folder_path=output_folder_path,
             output_folder_name=output_folder_name,
             total_download_bandwidth=total_download_bandwidth,
             # Pass START_TIME_STRING if that specific one is needed for the report header
             # start_time_string_for_report=START_TIME_STRING
         )
         if not generate_success:
              errors_encountered +=1

    # Prepare statistics for return and potential printing right now its in merged_file.py, which sucks.
    final_stats = {
        "start_time": sync_start_time_str,
        "end_time": time_utils.get_current_utc_iso_string(),
        "duration_seconds": duration,
        "files_processed": files_processed_count,
        "files_updated": files_updated_count,
        "files_deleted": files_deleted_count,
        "total_download_mb": total_download_bandwidth / (1024 * 1024),
        "errors": errors_encountered,
        "active_docs_in_db": local_db_manager.db.get("metadata", {}).get("active_documents", 0),
        "total_docs_in_db": local_db_manager.db.get("metadata", {}).get("total_documents", 0),
    }

    logging.info(f"Sync completed. Stats: {final_stats}")
    print("\nSynchronization Complete.")

    return final_stats