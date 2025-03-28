from constants.colors import BOLD_CYAN, RESET, YELLOW, RED
from constants.app_data import APP_NAME, SYNCED_CONTENT_FOLDER

def print_intro():

    """
    Print the intro message

    """

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

