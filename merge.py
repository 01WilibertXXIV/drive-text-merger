import os
import sys
import traceback
import logging

from helpers.drive_utils import get_name_for_id, parse_drive_url
from helpers.auth_utils import get_drive_service
from helpers.sync_utils import get_last_sync_time
from helpers.documents_utils import load_document_database, process_documents
from helpers.messages.intro import print_intro
from constants.colors import RED, RESET, YELLOW, BOLD_CYAN, DARK_GRAY

from constants.app_data import DATA_FOLDER, SYNCED_CONTENT_FOLDER


def main():
    """
    Main function to run the sync process.
    Allows user to provide a Drive URL either as a command line argument
    or through interactive input.
    """

    print_intro()
    # OUTRO will be printed in the process_documents function
    # This is done because the outro is dependent on the output folder path, file sizes, file word counts, 
    # total size, total word count, hours, minutes, seconds which are all determined in the process_documents function

    try:
        # Get the Drive service
        service = get_drive_service()
        
        # Initialize variables
        target_id = None
        target_type = None
        output_folder_name = None
        output_folder_path = None
        url = None
        
        # Check for command line arguments
        if len(sys.argv) > 1:
            url = sys.argv[1]
            logging.info(f"URL provided via command line: {url}")
        else:
            # If no URL provided as argument, prompt the user
            print(f"\n{YELLOW}No Google Drive URL provided as argument.{RESET}")
            print("Please paste a Google Drive URL to sync:")
            user_input = input(f"{BOLD_CYAN}> {RESET}").strip()
            
            if user_input:
                url = user_input
                print(f"\nProcessing Drive URL: {YELLOW}{url}{RESET}")
                logging.info(f"URL provided via user input: {url}")
            else:
                url = "https://drive.google.com/drive/u/0/my-drive"
                print(f"\n{YELLOW}No URL provided. Syncing your entire personal Drive.{RESET}")
                logging.info("No URL provided. Syncing entire personal Drive from default drive url.")

        # Process the URL if one was provided
        if url:
            
            # Parse the URL to get the target ID and type
            target_id, target_type = parse_drive_url(url)

            if not target_id:
                logging.error(f"Could not parse Drive URL: {url}")
                print(f"\n{RED}Error: Could not parse Drive URL.{RESET}")
                print("Please provide a valid Google Drive folder or file URL.")
                print("Examples:")
                print(f"  - {DARK_GRAY}https://drive.google.com/drive/folders/1abc123def456{RESET}")
                print(f"  - {DARK_GRAY}https://drive.google.com/drive/u/0/my-drive{RESET}")
                print(f"  - {DARK_GRAY}https://drive.google.com/drive/d/1abc123def456{RESET}")
                sys.exit(1)

            # Get the folder name
            output_folder_name = get_name_for_id(service, url=url, file_id=target_id)
            print(f"Drive name: {BOLD_CYAN}{output_folder_name}{RESET}")

            # Create the output folder
            output_folder_path = os.path.join(os.getcwd(), SYNCED_CONTENT_FOLDER, output_folder_name)
            
            logging.info(f"Creating output folder: {output_folder_path}")
            os.makedirs(output_folder_path, exist_ok=True)
            os.makedirs(f"{output_folder_path}/{DATA_FOLDER}", exist_ok=True)
            print(f"Folder successfully created on computer\n")

            # Get the last sync time
            last_sync_time = get_last_sync_time(output_folder_path)
            
            # Load the document database
            doc_db = load_document_database(output_folder_path)
            
            # Process documents and update the database
            doc_db = process_documents(service, last_sync_time, doc_db, 
                                     target_id, target_type, 
                                     output_folder_path=output_folder_path, 
                                     output_folder_name=output_folder_name)
            
    except Exception as e:
        logging.error(f"Sync failed: {str(e)}")
        logging.error(traceback.format_exc())
        print(f"\n{RED}Sync failed: {str(e)}{RESET}")
        print("See log for details.")

if __name__ == '__main__':
    main()





