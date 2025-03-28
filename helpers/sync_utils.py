import os
import hashlib

from constants.app_data import DATA_FOLDER, SYNC_INFO_FILE


def get_last_sync_time(output_folder_path):
    """
    Read the last sync time from a file.
    Output folder path is the path to the folder where the synced content is stored.
    That is determined dynamically by which drive/folder is being synced.

    Args:
        output_folder_path (str): The path to the output folder

    Returns:
        str: The last sync time
    """
    try:
        with open(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/{SYNC_INFO_FILE}"), 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        # If it's the first run, use a date far in the past
        return '1970-01-01T00:00:00.000Z'



def save_last_sync_time(time_str, output_folder_path):
    """
    Save the current time as the last sync time.
    Output folder path is the path to the folder where the synced content is stored.
    That is determined dynamically by which drive/folder is being synced.

    Args:
        time_str (str): The current time
        output_folder_path (str): The path to the output folder
    """
    with open(os.path.join(f"{output_folder_path}/{DATA_FOLDER}/{SYNC_INFO_FILE}"), 'w') as f:
        f.write(time_str)



def compute_checksum(text):
    """
    Compute a checksum for the document text.
    The checksum will be returned and used to check if the document has been modified since the last sync.
    Previous checksums are stored in the document database per file.

    Args:
        text (str): The text of the document

    Returns:
        str: The checksum of the document
    """
    return hashlib.md5(text.encode('utf-8')).hexdigest()
