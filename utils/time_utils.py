import datetime
import time

START_TIME = time.time()                        # Start time in seconds
START_TIME_STRING = datetime.datetime.now()     # Start time in string format

def get_current_utc_iso_string():
    return datetime.datetime.now(datetime.UTC).isoformat() + 'Z'

def parse_iso_time_string(time_str):
    # ... (logic from process_documents to handle different formats) ...
    # Return a timezone-aware datetime object
    pass

def format_time_local(dt_object):
    # ... (logic to format for printing) ...
    pass