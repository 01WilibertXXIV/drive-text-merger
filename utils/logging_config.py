import logging
from config import LOG_FILE

def setup_logging():
    # Set up logging
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
