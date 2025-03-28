import os
import json

def init():
    if not os.path.exists("version.json"):
        default_version_file = os.path.join(os.getcwd(), "version.json")
        with open(default_version_file, "w") as f:
            json.dump({"commit_sha": "", "commit_date": "", "commit_message": "Initial version"}, f)


