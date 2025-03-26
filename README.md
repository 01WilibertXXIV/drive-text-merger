# drive-text-merger
## Overview
This script is used to merge text files from a Google Drive folder into a single file and keep track of the changes and the files that were updated or deleted.

Files will be split into chunks of 200MB or 450,000 words, whichever comes first.
*This is to avoid compatibility issues with NotebookLM.*

**Limitations:**
 - Not all files are merged, only Doc, DocX and some PDF files are merged. *Will try to merge more file types in the future.*


## Installation
```bash
pip install -r requirements.txt
```

## Usage
```bash
python complete-drive-sync.py "COMPLETE_GOOGLE_DRIVE_URL"
```

## Example
```bash
python complete-drive-sync.py "https://drive.google.com/drive/folders/1234567890"
```

## Output
The output will be saved in the `synced_content` folder under the name of the folder you provided in the URL.
It will create a database in json format to keep track of the changes and the files that were updated or deleted.
It will also create a merged file with all the active documents in the folder.














