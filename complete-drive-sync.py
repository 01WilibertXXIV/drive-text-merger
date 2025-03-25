import os
print("Current working directory:", os.getcwd())
print("Files in current directory:", os.listdir())
print("Looking for 'credentials.json'...")
if os.path.exists('credentials.json'):
    print("credentials.json found!")
    print("File size:", os.path.getsize('credentials.json'), "bytes")
else:
    print("credentials.json NOT found in", os.getcwd())
    
import pickle
import datetime
import json
import hashlib
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import io
import docx
import traceback
import logging

# Set up logging
logging.basicConfig(filename='drive_sync.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the scopes for API access
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Database file to store document metadata and content
DOCUMENT_DB = 'document_database.json'

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

def extract_text_from_google_doc(file_data):
    """Extract text from a Google Doc (exported as DOCX)."""
    # Google Docs are exported as DOCX, so we use the same function
    return extract_text_from_docx(file_data)

def compute_checksum(text):
    """Compute a checksum for the document text."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def process_documents(service, start_time, doc_db):
    """Process files that have changed since the start_time and update the document database."""
    # Get list of all changes since the last sync
    changes_processed = 0
    files_updated = 0
    files_deleted = 0
    
    # Track the current time for the next sync point
    current_time = datetime.datetime.utcnow().isoformat() + 'Z'
    
    logging.info(f"Starting sync from {start_time}")
    print(f"Starting sync from {start_time}")
    
    # Get all docs that have been trashed/deleted since last sync
    # First, let's get a list of file IDs we have in our database
    existing_file_ids = set(doc_db["documents"].keys())
    active_file_ids = set()
    
    # Get all Google Docs, PDFs, Word docs, and text files that have changed or were added
    query = ("(mimeType='application/vnd.google-apps.document' OR "
            "mimeType='application/pdf' OR "
            "mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')")
    
    try:
        # First, get all active documents
        page_token = None
        while True:
            results = service.files().list(
                q=query,
                pageSize=100,
                spaces='drive',
                fields="nextPageToken, files(id, name, mimeType, modifiedTime, createdTime)",
                pageToken=page_token
            ).execute()
            
            items = results.get('files', [])
            
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
                        if mime_type == 'application/vnd.google-apps.document':
                            request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
                        else:
                            request = service.files().get_media(fileId=file_id)
                        
                        # Download the file content
                        file_data = io.BytesIO()
                        downloader = MediaIoBaseDownload(file_data, request)
                        done = False
                        while not done:
                            status, done = downloader.next_chunk()
                        
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
                        else:
                            # Just update the lastSynced time
                            doc_db["documents"][file_id]["lastSynced"] = current_time
                        
                    except Exception as e:
                        logging.error(f"Error processing file {file_name}: {str(e)}")
                        logging.error(traceback.format_exc())
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        
        # Identify deleted files (in our DB but not in active files)
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
    
    # Update the database metadata
    doc_db["metadata"]["last_updated"] = current_time
    doc_db["metadata"]["total_documents"] = len(doc_db["documents"])
    doc_db["metadata"]["active_documents"] = len(active_file_ids)
    
    # Save the document database
    save_document_database(doc_db)
    
    # Generate the merged file with all content
    generate_merged_file(doc_db, current_time, files_updated, files_deleted)
    
    # Update the last sync time
    save_last_sync_time(current_time)
    
    logging.info(f"Sync completed. Processed {changes_processed} changes, updated {files_updated} files, deleted {files_deleted} files.")
    print(f"Sync completed. Processed {changes_processed} changes, updated {files_updated} files, deleted {files_deleted} files.")
    
    return doc_db

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
        # Get the Drive service
        service = get_drive_service()
        
        # Get the last sync time
        last_sync_time = get_last_sync_time()
        
        # Load the document database
        doc_db = load_document_database()
        
        # Process documents and update the database
        doc_db = process_documents(service, last_sync_time, doc_db)
        
        # Here you would add code to feed the output_file into your AI system
        # For example:
        # ai_system.process_document(output_file)
        
    except Exception as e:
        logging.error(f"Sync failed: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"Sync failed. See log for details.")

if __name__ == '__main__':
    main()
