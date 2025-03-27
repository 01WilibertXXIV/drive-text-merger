import time
import logging
import os
import threading
from queue import Queue, Empty

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
            status = f"\rFolders: {count} | Speed: {scan_speed:.1f}/s | Errors: {errors} | "
            status += f"Time: {hours:02d}:{minutes:02d}:{seconds:02d} | Queue: {queue_size} | Delay: {delay:.3f}s"
            
            # Add no-progress indicator if we've been stuck
            if no_progress_timer > 5:  # 5 seconds without progress
                status += f" | No progress: {int(no_progress_timer)}s"
                
                # If no progress for extended period and queue is empty, we might be done
                if no_progress_timer > 30 and queue_size == 0:
                    print(f"\nNo progress for {int(no_progress_timer)} seconds and queue is empty. Process may be complete.")
                    shutdown_flag.set()  # Signal threads to exit
                    break
            
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
        print(f"Found {subfolder_counter['count']} subfolders ({folders_per_second:.1f} folders/sec).")
        print(f"Encountered {error_counter['count']} errors.")
    
    return all_subfolders