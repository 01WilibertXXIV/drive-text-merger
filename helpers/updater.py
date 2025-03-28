import os
import sys
import shutil
import tempfile
import zipfile
import time
from urllib.request import urlopen
from urllib.error import URLError
import json
import hashlib
import platform

# Configuration - adjust these values
GITHUB_REPO = "01WilibertXXIV/drive-text-merger"  # Your GitHub username and repository
BRANCH = "main"  # Branch to download from
VERSION_FILE = "version.json"  # Local file to store version info
APP_ENTRY_POINT = "merge.py"  # Main application file

def get_platform_info():
    """Get information about the current platform."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    return f"{system}-{machine}"

def download_file(url, target_path):
    """Download a file from a URL to a specified path."""
    print(f"Downloading from {url}...")
    
    try:
        with urlopen(url) as response, open(target_path, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        return True
    except URLError as e:
        print(f"Download failed: {e}")
        return False

def get_latest_version_info():
    """Get information about the latest version from GitHub."""
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/{BRANCH}"
    
    try:
        with urlopen(api_url) as response:
            data = json.loads(response.read().decode())
            return {
                "commit_sha": data["sha"],
                "commit_date": data["commit"]["committer"]["date"],
                "commit_message": data["commit"]["message"]
            }
    except URLError as e:
        print(f"Failed to get version info: {e}")
        return None
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Failed to parse version info: {e}")
        return None

def get_current_version_info():
    """Get information about the currently installed version."""
    if not os.path.exists(VERSION_FILE):
        return None
    
    try:
        with open(VERSION_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Failed to read current version info: {e}")
        return None

def save_version_info(version_info):
    """Save version information to the local version file."""
    try:
        with open(VERSION_FILE, 'w') as f:
            json.dump(version_info, f, indent=2)
        return True
    except IOError as e:
        print(f"Failed to save version info: {e}")
        return False

def is_update_available():
    """Check if an update is available."""
    current = get_current_version_info()
    latest = get_latest_version_info()
    
    if not latest:
        return False
    
    if not current or current.get("commit_sha") != latest.get("commit_sha"):
        return latest
    
    return False

def download_and_extract_update():
    """Download and extract the latest version of the application."""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, "update.zip")
    
    # Download the zip file from GitHub
    download_url = f"https://github.com/{GITHUB_REPO}/archive/{BRANCH}.zip"
    if not download_file(download_url, zip_path):
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False
    
    # Extract the zip file
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Get the extracted directory name (usually "{repo}-{branch}")
        extracted_dir = None
        for item in os.listdir(temp_dir):
            item_path = os.path.join(temp_dir, item)
            if os.path.isdir(item_path) and item != "__MACOSX":  # Exclude macOS metadata folder
                extracted_dir = item_path
                break
        
        if not extracted_dir:
            print("Couldn't find extracted directory")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return False
        
        return extracted_dir
    except Exception as e:
        print(f"Extraction failed: {e}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False

def backup_current_app():
    """Create a backup of the current application."""
    backup_dir = "backup_" + str(int(time.time()))
    
    # Skip special files and directories
    skip_items = [".git", ".github", "venv", "env", "__pycache__", 
                 ".venv", ".env", backup_dir, "backup_*"]
    
    try:
        os.makedirs(backup_dir, exist_ok=True)
        
        for item in os.listdir('.'):
            # Skip backup directories and other special items
            if any(item.startswith(skip) or item == skip for skip in skip_items):
                continue
                
            source = os.path.join('.', item)
            dest = os.path.join(backup_dir, item)
            
            if os.path.isdir(source):
                shutil.copytree(source, dest)
            else:
                shutil.copy2(source, dest)
        
        return backup_dir
    except Exception as e:
        print(f"Backup failed: {e}")
        return None

def update_application():
    """Update the application with the latest version."""
    latest_version = is_update_available()
    if not latest_version:
        print("No update available.")
        return False
    
    print(f"Update available: {latest_version.get('commit_message', 'No message')}")
    
    # Step 1: Download the update
    extracted_dir = download_and_extract_update()
    if not extracted_dir:
        return False
    
    # Step 2: Create a backup
    backup_dir = backup_current_app()
    if not backup_dir:
        shutil.rmtree(os.path.dirname(extracted_dir), ignore_errors=True)
        return False
    
    try:
        # Step 3: Update the files
        print("Installing update...")
        
        # Skip special directories during update
        skip_items = [".git", ".github", "venv", "env", ".venv", ".env", "__pycache__"]
        
        # Copy new files
        for item in os.listdir(extracted_dir):
            # Skip special items
            if item in skip_items:
                continue
                
            source = os.path.join(extracted_dir, item)
            dest = os.path.join('.', item)
            
            # Remove existing file/directory
            if os.path.exists(dest):
                if os.path.isdir(dest):
                    shutil.rmtree(dest)
                else:
                    os.remove(dest)
            
            # Copy new file/directory
            if os.path.isdir(source):
                shutil.copytree(source, dest)
            else:
                shutil.copy2(source, dest)
        
        # Step 4: Save the new version info
        save_version_info(latest_version)
        
        # Step 5: Clean up
        shutil.rmtree(os.path.dirname(extracted_dir), ignore_errors=True)
        
        print(f"Update installed successfully! (Backup created in {backup_dir})")
        return True
        
    except Exception as e:
        print(f"Update failed: {e}")
        print(f"Restoring from backup ({backup_dir})...")
        
        # Attempt restoration from backup
        try:
            for item in os.listdir(backup_dir):
                source = os.path.join(backup_dir, item)
                dest = os.path.join('.', item)
                
                if os.path.exists(dest):
                    if os.path.isdir(dest):
                        shutil.rmtree(dest)
                    else:
                        os.remove(dest)
                
                if os.path.isdir(source):
                    shutil.copytree(source, dest)
                else:
                    shutil.copy2(source, dest)
            
            print("Restoration completed.")
        except Exception as restore_error:
            print(f"Restoration failed: {restore_error}")
            print(f"Please restore manually from the backup directory: {backup_dir}")
        
        # Clean up the downloaded update
        shutil.rmtree(os.path.dirname(extracted_dir), ignore_errors=True)
        return False

def restart_application():
    """Restart the current application."""
    print("Restarting application...")
    time.sleep(1)  # Brief pause to allow logs to be written
    
    # Restart the application with the same arguments
    os.execv(sys.executable, [sys.executable] + sys.argv)

if __name__ == "__main__":
    # This allows the updater to be run directly
    if update_application():
        print("Update successful")
    else:
        print("No update was applied")