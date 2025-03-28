import os
import sys
import traceback
import logging

from helpers.drive_utils import get_name_for_id, parse_drive_url
from helpers.auth_utils import get_drive_service
from helpers.sync_utils import get_last_sync_time
from helpers.documents_utils import load_document_database, process_documents

from constants.app_data import DATA_FOLDER, SYNCED_CONTENT_FOLDER, APP_NAME
from constants.colors import BOLD_CYAN, RESET, YELLOW, RED

def main():
    """Main function to run the sync process."""

    print("\n\n")
    print("="*50)
    print(f"{BOLD_CYAN}{APP_NAME}{RESET}")
    print("="*50)
    print("")
    print(f"{YELLOW}📋 PURPOSE:{RESET}")
    print("This tool synchronizes and merges your Google Drive documents into consolidated files")
    print("for easier management, searching, and backup.")
    print("")
    print(f"{YELLOW}🔄 FIRST-TIME SETUP:{RESET}")
    print(f"• A folder named {BOLD_CYAN}{SYNCED_CONTENT_FOLDER}{RESET} will be created in your current directory")
    print(f"• Inside this folder, a subfolder will be created with the name of your selected Google Drive")
    print("  or folder that you're syncing")
    print("")
    print(f"{RED}⚠️ IMPORTANT:{RESET}")
    print("• Do not rename these folders after creation")
    print("• Renaming will break the sync relationship and require starting over")
    print("="*50)

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
            output_folder_path = os.path.join(os.getcwd(), SYNCED_CONTENT_FOLDER, output_folder_name)
            
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
            output_folder_path = os.path.join(os.getcwd(), SYNCED_CONTENT_FOLDER, output_folder_name)
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






