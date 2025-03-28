import os
import json

def init():

    """
    This function initializes the application by creating the version.json and drive_sync.log files if they don't exist.
    
    version.json contains the commit sha, commit date and commit message. 
    It is used to track the version of the application so the auto-updater can work.

    drive_sync.log contains the log of the drive sync.
    It is used to track the changes in the drive.
    
    """
    if not os.path.exists("version.json"):
        default_version_file = os.path.join(os.getcwd(), "version.json")
        with open(default_version_file, "w") as f:
            json.dump({"commit_sha": "", "commit_date": "", "commit_message": "Initial version"}, f)

    # Create drive_sync.log if it doesn't exist
    if not os.path.exists("drive_sync.log"):
        default_drive_sync_log = os.path.join(os.getcwd(), "drive_sync.log")
        with open(default_drive_sync_log, "w") as f:
            f.write("")


