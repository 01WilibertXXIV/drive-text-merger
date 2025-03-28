# drive-text-merger
## Overview
This script merges text files from a Google Drive folder into a single file while tracking changes, updates, and deletions. Files are split into chunks of 200MB or 450,000 words (whichever comes first) to ensure compatibility with NotebookLM.

This app will be updated regularly to add new features and fix bugs.

### Limitations:
- Currently supports Doc, DocX, and some PDF files only
- Support for additional file types will be added in future updates


## Installation
First, you need to make sure you have Python installed. You can download it [here](https://www.python.org/downloads/).

Then, you need to install the required packages by running the following command *in the root directory*:
```bash
pip install -r requirements.txt
```

The app will need a file called `credentials.json` in the root directory. This file can be obtained by following the instructions [here](https://developers.google.com/workspace/guides/create-credentials?hl=fr).

Follow the steps to create an OAuth 2.0 client ID for a Desktop app and download the credentials file.
Rename the file to `credentials.json` and place it in the root directory.w
*This file should not be shared with anyone.*

## Usage
In the root directory, run the script with the following command:
```bash
python merge.py
```
When prompted, enter the complete Google Drive folder URL.


Url can also be provided as a command-line argument. If that's the case, the app will not prompt for the URL.
```bash
python merge.py "https://drive.google.com/drive/folders/1234567890"
```

## Update
**This is only available if the app is not run from a Git repository.**
**App runs from a Git repository will not check for updates.**

The app will check for updates when it starts. You can skip the update check by running the script with the `--no-update` flag.
```bash
python merge.py --no-update
```
Updates are downloaded from the `main` branch of the [GitHub repository](https://github.com/01WilibertXXIV/drive-text-merger).


 


## Output
The script generates the following outputs in the `synced_content` folder:

1. A folder named after the Google Drive folder you provided
2. A JSON database that tracks changes, updates, and deleted files
3. A merged file containing all active text documents from the folder / subfolders

Files exceeding the size limits will be automatically split into multiple chunks.













