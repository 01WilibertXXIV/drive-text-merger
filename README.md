# drive-text-merger
## Overview
This script merges text files from a Google Drive folder into a single file while tracking changes, updates, and deletions. Files are split into chunks of 200MB or 450,000 words (whichever comes first) to ensure compatibility with NotebookLM.

### Limitations:
- Currently supports Doc, DocX, and some PDF files only
- Support for additional file types will be added in future updates


## Installation
Install the required packages by running the following command:
```bash
pip install -r requirements.txt
```

The app will need a file called `credentials.json` in the root directory. This file can be obtained by following the instructions [here](https://developers.google.com/workspace/guides/create-credentials?hl=fr).

Follow the steps to create an OAuth 2.0 client ID for a Desktop app and download the credentials file.
Rename the file to `credentials.json` and place it in the root directory.w
*This file should not be shared with anyone.*

## Usage
You can run the script in two ways:
### 1. Provide the Google Drive URL as a command-line argument
```bash
python merge.py "https://drive.google.com/drive/folders/1234567890"
```
### 2. Run the script and enter the URL when prompted
```bash
python merge.py
```
When prompted, enter the complete Google Drive folder URL.

## Example
```bash
python merge.py "https://drive.google.com/drive/folders/1234567890"
```

## Output
The script generates the following outputs in the `synced_content` folder:

1. A folder named after the Google Drive folder you provided
2. A JSON database that tracks changes, updates, and deleted files
3. A merged file containing all active text documents from the folder / subfolders

Files exceeding the size limits will be automatically split into multiple chunks.













