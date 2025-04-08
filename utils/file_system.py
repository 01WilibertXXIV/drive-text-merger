import os
from config import DATA_FOLDER

def ensure_data_folder(output_folder_path):
    """Ensure the data folder exists and is hidden on Windows."""
    data_folder_path = os.path.join(output_folder_path, DATA_FOLDER)
    
    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)




