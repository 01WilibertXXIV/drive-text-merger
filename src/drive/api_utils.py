import io
import logging
import googleapiclient
from googleapiclient.http import MediaIoBaseDownload
from typing import List, Tuple, Optional, Dict, Any

# Optional: Define custom exceptions for clearer error handling downstream
class DownloadPermissionsError(Exception):
    """Raised when download fails due to permissions."""
    pass

class DownloadAbuseError(Exception):
    """Raised when download fails due to abuse/policy reasons."""
    pass

class GoogleApiError(Exception):
    """Generic wrapper for Google API errors during util execution."""
    pass


def get_name_for_id(service: Any, file_id: str) -> Optional[str]:
    """Fetches the name of a file/folder given its ID."""
    try:
        file_metadata = service.files().get(
            fileId=file_id,
            fields='name',
            supportsAllDrives=True # Important for Shared Drives
        ).execute()
        return file_metadata.get('name')
    except googleapiclient.errors.HttpError as error:
        logging.error(f"API error getting name for ID {file_id}: {error}", exc_info=True)
        # Propagate error or return None based on desired handling
        # raise GoogleApiError(f"Failed to get name for {file_id}") from error
        return None # Return None if name lookup fails
    except Exception as e:
        logging.error(f"Unexpected error getting name for ID {file_id}: {e}", exc_info=True)
        return None


def list_files_in_folder(
    service: Any,
    folder_id: str,
    mime_types: List[str],
    page_token: Optional[str],
    page_size: int,
    fields: str
) -> Dict[str, Any]:
    """Lists files matching specific criteria within a given folder."""

    # Construct the MIME type part of the query dynamically
    mime_query_part = " or ".join([f"mimeType='{mt}'" for mt in mime_types])
    # Add other constant parts of your query
    query = f"({mime_query_part}) and not name contains '.docm' and '{folder_id}' in parents and trashed = false"

    list_params = {
        'q': query,
        'pageSize': page_size,
        'fields': fields,
        'spaces': 'drive',
        'supportsAllDrives': True,
        'includeItemsFromAllDrives': True,
        'orderBy': 'folder, name' # Optional: for consistent ordering
    }
    if page_token:
        list_params['pageToken'] = page_token

    try:
        results = service.files().list(**list_params).execute()
        return results
    except googleapiclient.errors.HttpError as error:
        logging.error(f"API error listing files in folder {folder_id}: {error}", exc_info=True)
        # Option 1: Raise a custom error
        raise GoogleApiError(f"Failed to list files in folder {folder_id}") from error
        # Option 2: Return an empty dictionary or specific error structure
        # return {"error": True, "message": str(error), "files": []}
    except Exception as e:
        logging.error(f"Unexpected error listing files in folder {folder_id}: {e}", exc_info=True)
        raise GoogleApiError(f"Unexpected error listing files in folder {folder_id}") from e


def download_file_content(
    service: Any,
    file_id: str,
    mime_type: str
) -> Tuple[Optional[bytes], Optional[str], int]:
    """
    Downloads or exports file content.

    Handles Google Docs/Sheets export and direct download for other types.

    Returns:
        A tuple containing:
        - content_bytes (bytes or None on failure)
        - downloaded_as_mime_type (str or None)
        - size (int, 0 on failure)
    """
    request = None
    export_mime_type = None

    # Determine if export or direct download is needed
    if mime_type == 'application/vnd.google-apps.document':
        export_mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' # Export as DOCX
        request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
    elif mime_type == 'application/vnd.google-apps.spreadsheet':
        export_mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' # Export as XLSX
        request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
    elif mime_type in [
        'application/pdf',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document', # DOCX
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', # XLSX
        'text/csv'
    ]:
        export_mime_type = mime_type # It's already in a downloadable format
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    else:
        # This type is listed in QUERY_MIME_TYPES but not handled here? Or an unexpected type.
        logging.warning(f"Skipping download for unhandled mimeType: {mime_type} for file {file_id}")
        return None, None, 0 # Indicate skip/failure

    file_data = io.BytesIO()
    downloader = MediaIoBaseDownload(file_data, request)
    done = False

    try:
        while not done:
            # status object contains progress if needed, but we removed the print
            status, done = downloader.next_chunk()
            if status:
                 logging.debug(f"Downloading file {file_id}: {int(status.progress() * 100)}%")
        logging.info(f"Successfully downloaded/exported file {file_id} as {export_mime_type}")
        content_bytes = file_data.getvalue()
        size = len(content_bytes)
        return content_bytes, export_mime_type, size

    except googleapiclient.errors.HttpError as error:
        error_content = getattr(error, 'content', b'').decode('utf-8')
        error_reason = getattr(error, 'reason', str(error))
        logging.error(f"HTTP error downloading file {file_id}: {error_reason} - {error_content}", exc_info=True)

        # Check for specific error conditions based on content/reason
        if "userRateLimitExceeded" in error_content or "rateLimitExceeded" in error_content:
            # Maybe raise a specific exception to handle rate limits (e.g., backoff and retry)
            raise GoogleApiError(f"Rate limit exceeded for file {file_id}") from error
        elif "fileNotDownloadable" in error_content or "forbidden" in error_reason.lower():
             raise DownloadPermissionsError(f"File not downloadable (Permissions?): {file_id}") from error
        elif "cannotDownloadAbusiveFile" in error_content:
             raise DownloadAbuseError(f"File blocked for abuse/policy: {file_id}") from error
        else:
             # General HTTP error during download
             raise GoogleApiError(f"HTTP error during download for {file_id}") from error

    except Exception as e:
        # Catch any other unexpected exceptions during download process
        logging.error(f"Unexpected error downloading file {file_id}: {e}", exc_info=True)
        raise GoogleApiError(f"Unexpected error during download for {file_id}") from e

    # Should not be reached if exceptions are raised, but as a fallback
    return None, None, 0