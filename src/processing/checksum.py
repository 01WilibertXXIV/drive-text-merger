import hashlib
def compute_checksum(text):
    """
    Compute a checksum for the document text.
    The checksum will be returned and used to check if the document has been modified since the last sync.
    Previous checksums are stored in the document database per file.

    Args:
        text (str): The text of the document

    Returns:
        str: The checksum of the document
    """
    return hashlib.md5(text.encode('utf-8')).hexdigest()