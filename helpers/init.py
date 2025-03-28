import os
import json

def setup():
    """
    This function initializes the application by creating the version.json and drive_sync.log files if they don't exist.
    Both files are needed for the app to work.
    """
    create_git_version_file()
    create_log_file()

    

def create_git_version_file():
    """
    This function initializes the version.json file if it doesn't exist.
    Contains the commit sha, commit date and commit message.

    It is used to track the version of the application so the auto-updater can work.
    This file is not used if the app is run from a Git repository (it will still be created though).
    """
    if not os.path.exists("version.json"):
        default_version_file = os.path.join(os.getcwd(), "version.json")
        with open(default_version_file, "w") as f:
            json.dump({"commit_sha": "", "commit_date": "", "commit_message": "Initial version"}, f)

def create_log_file():
    """
    This function initializes the drive_sync.log file if it doesn't exist.
    Contains the log of the drive sync.

    Everything that is printed to the console is logged in this file.
    Use to debug the app.
    """
    if not os.path.exists("drive_sync.log"):
        default_drive_sync_log = os.path.join(os.getcwd(), "drive_sync.log")
        with open(default_drive_sync_log, "w") as f:
            f.write("")