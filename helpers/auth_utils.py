import os
import pickle
import sys
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from constants.colors import RED, RESET, YELLOW, GREEN

SCOPES = ['https://www.googleapis.com/auth/drive']

def get_drive_service():
    """Authenticate and return a Google Drive service object."""
    creds = None

    print(f"\nTrying to authenticate...")

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print(f"Refreshing credentials...")
                creds.refresh(Request())
                print(f"Credentials refreshed")
            except Exception as e:
                print(f"{RED}Failed to refresh credentials: {e}{RESET}")
                sys.exit(1)
        else:
            if not os.path.exists('credentials.json'):
                print("\n")
                print(f"{RED}credentials.json not found in the root directory{RESET}")
                print("Please follow the instructions [here](https://developers.google.com/workspace/guides/create-credentials?hl=fr)")
                print("Make sure to select Desktop app as the application type")
                print(f"Download the credentials file and rename it to {YELLOW}credentials.json{RESET}")
                print("Place the file in the root directory")
                print("Run the script again")
                print("If you still encounter this error, please contact the developer")
                print(f"{RED}Exiting the script...{RESET}")
                sys.exit(1)
            else:
                print("\n")
                print("Authenticating using credentials.json...")
                try:
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)
                except Exception as e:
                    print(f"{RED}Failed to authenticate using credentials.json: {e}{RESET}")
                    print(f"{RED}Exiting the script...{RESET}")
                    sys.exit(1)

        # Save the valid credentials
        if creds and creds.valid:
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        else:
            print(f"{RED}Invalid credentials. Exiting...{RESET}")
            sys.exit(1)

    print(f"Authentication successful!")

    return build('drive', 'v3', credentials=creds)