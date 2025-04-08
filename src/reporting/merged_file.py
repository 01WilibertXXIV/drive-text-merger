import datetime
import os
import logging

from utils.time_utils import START_TIME_STRING

from src.reporting.stats import print_sync_summary


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
    

    print_sync_summary(output_folder_path, file_sizes, file_word_counts, total_size, total_word_count, hours, minutes, seconds, total_download_bandwidth, files_updated, files_deleted)

    return generated_files
#endregion
