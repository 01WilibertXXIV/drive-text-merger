import json
import os
import logging # Added logging import
from typing import Optional, Dict, Any, Set # Added Set and Any
from config import DATA_FOLDER, DOCUMENT_DB_FILE, LAST_SYNC_FILE, DEFAULT_START_TIME
from utils.file_system import ensure_data_folder
# Assuming ProcessedDocument might still be used by update_document
# from processing.document import ProcessedDocument # If using dataclass

class LocalDBManager:
    def __init__(self, output_folder_path: str):
        self.output_folder_path = output_folder_path
        # Construct paths using os.path.join for cross-platform compatibility
        self.data_dir_path = os.path.join(output_folder_path, DATA_FOLDER)
        self.db_file_path = os.path.join(self.data_dir_path, DOCUMENT_DB_FILE)
        self.last_sync_file_path = os.path.join(self.data_dir_path, LAST_SYNC_FILE)
        ensure_data_folder(self.data_dir_path) # Ensure folder exists on init using the full path
        self.db = self._load()

    def _load(self) -> Dict[str, Any]:
        if os.path.exists(self.db_file_path):
            try:
                with open(self.db_file_path, 'r', encoding='utf-8') as f:
                    # Add basic structure validation
                    data = json.load(f)
                    if not isinstance(data, dict) or "documents" not in data or "metadata" not in data:
                        logging.warning(f"JSON file {self.db_file_path} has incorrect structure. Re-initializing.")
                        return {"documents": {}, "metadata": {}}
                    return data
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON from {self.db_file_path}. Re-initializing.", exc_info=True)
                return {"documents": {}, "metadata": {}} # Handle corruption
            except Exception as e:
                logging.error(f"Unexpected error loading DB from {self.db_file_path}: {e}", exc_info=True)
                return {"documents": {}, "metadata": {}}
        return {"documents": {}, "metadata": {}} # Return default structure if file doesn't exist

    def save(self):
        # Ensure metadata structure exists
        if "metadata" not in self.db: self.db["metadata"] = {}
        # Update metadata before saving (moved from _update_metadata for clarity)
        self.db["metadata"]["total_documents"] = len(self.db.get("documents", {}))
        self.db["metadata"]["active_documents"] = len([
            doc_id for doc_id, doc in self.db.get("documents", {}).items() if not doc.get("deleted", False)
        ])
        # Ensure last_updated is set (should be done by set_last_updated_time)
        self.db["metadata"].setdefault("last_updated", "")

        try:
            # Ensure the directory exists right before writing
            os.makedirs(self.data_dir_path, exist_ok=True)
            with open(self.db_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.db, f, ensure_ascii=False, indent=2)
            logging.info(f"Document database saved to {self.db_file_path}")
        except IOError as e:
             logging.error(f"Failed to save document database to {self.db_file_path}: {e}", exc_info=True)
        except Exception as e:
             logging.error(f"Unexpected error saving database: {e}", exc_info=True)


    def get_document_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves stored information for a specific document ID."""
        return self.db.get("documents", {}).get(file_id)

    # Modified update_document to accept a dictionary for flexibility
    # Alternatively, keep using ProcessedDocument if strongly preferred
    def update_document(self, doc_data: Dict[str, Any], sync_time_str: str):
        """Updates or adds a document entry in the local database."""
        file_id = doc_data.get("id")
        if not file_id:
             logging.error("Attempted to update document with missing ID.")
             return False

        if "documents" not in self.db: self.db["documents"] = {}

        # Ensure checksum exists in doc_data if it's a content update
        if "checksum" not in doc_data:
             logging.warning(f"Checksum missing for document update {file_id}. Proceeding without it.")

        self.db["documents"][file_id] = {
            "name": doc_data.get("name", "Unknown"),
            "url": doc_data.get("url", ""),
            "mimeType": doc_data.get("mime_type", "unknown"),
            "modifiedTime": doc_data.get("modified_time_str", ""),
            "createdTime": doc_data.get("created_time_str", ""),
            "lastSynced": sync_time_str,
            "checksum": doc_data.get("checksum"), # Store checksum if available
            # "content": doc_data.get("content"), # Decide if you REALLY need full content here
            "deleted": False # Explicitly mark as not deleted on update
        }
        # Remove deletedTime if it exists from a previous deletion
        self.db["documents"][file_id].pop("deletedTime", None)
        logging.debug(f"Updated local DB entry for {file_id}")
        return True

    # --- NEW METHOD ---
    def update_document_sync_time(self, file_id: str, sync_time_str: str, file_url: Optional[str] = None) -> bool:
        """
        Updates only the last synced time and optionally the URL for an existing document.
        Ensures the document is marked as not deleted.
        """
        if "documents" not in self.db:
             logging.warning(f"Attempted update_document_sync_time but 'documents' key missing in DB.")
             return False # Should not happen if loaded correctly

        doc_entry = self.db["documents"].get(file_id)

        if doc_entry:
            doc_entry["lastSynced"] = sync_time_str
            if file_url:
                doc_entry["url"] = file_url
            # Ensure it's marked as active
            doc_entry["deleted"] = False
            doc_entry.pop("deletedTime", None) # Remove deletion marker if present
            logging.debug(f"Updated sync time/URL for existing document {file_id}")
            return True
        else:
            logging.warning(f"Attempted to update sync time for non-existent document ID: {file_id}")
            return False
    # --- END NEW METHOD ---

    def mark_deleted(self, file_id: str, sync_time_str: str) -> bool:
         """Marks a document as deleted in the local database."""
         if "documents" not in self.db: return False # Should not happen

         doc_entry = self.db["documents"].get(file_id)
         if doc_entry:
             if not doc_entry.get("deleted", False): # Only mark if not already marked
                 doc_entry["deleted"] = True
                 doc_entry["deletedTime"] = sync_time_str
                 logging.info(f"Marked document {file_id} as deleted in local DB.")
                 return True
             else:
                 logging.debug(f"Document {file_id} was already marked as deleted.")
                 return False # Indicate no change was made
         else:
            logging.warning(f"Attempted to mark non-existent document ID as deleted: {file_id}")
            return False # Indicate document not found

    def get_all_document_ids(self) -> Set[str]:
        """Returns a set of all document IDs currently in the database."""
        return set(self.db.get("documents", {}).keys())

    def set_last_updated_time(self, sync_time_str: str):
         """Sets the 'last_updated' timestamp in the metadata."""
         if "metadata" not in self.db: self.db["metadata"] = {}
         self.db["metadata"]["last_updated"] = sync_time_str
         logging.debug(f"Set DB last_updated time to {sync_time_str}")


    def load_last_sync_time(self) -> str:
        """Loads the last successful sync timestamp from its dedicated file."""
        try:
            if os.path.exists(self.last_sync_file_path):
                with open(self.last_sync_file_path, 'r', encoding='utf-8') as f:
                    timestamp = f.read().strip()
                    # Basic validation: check if it looks like an ISO timestamp
                    if timestamp and 'T' in timestamp and ('Z' in timestamp or '+' in timestamp or '-' in timestamp[10:]):
                         logging.info(f"Loaded last sync time: {timestamp}")
                         return timestamp
                    else:
                         logging.warning(f"Invalid format found in {self.last_sync_file_path}. Using default start time.")
                         return DEFAULT_START_TIME
            else:
                logging.info("Last sync time file not found. Using default start time.")
                return DEFAULT_START_TIME
        except IOError as e:
            logging.error(f"Error reading last sync time file {self.last_sync_file_path}: {e}", exc_info=True)
            return DEFAULT_START_TIME # Fallback to default on error
        except Exception as e:
            logging.error(f"Unexpected error loading last sync time: {e}", exc_info=True)
            return DEFAULT_START_TIME


    def save_last_sync_time(self, sync_time_str: str):
        """Saves the latest successful sync timestamp to its dedicated file."""
        try:
            # Ensure the directory exists right before writing
            os.makedirs(self.data_dir_path, exist_ok=True)
            with open(self.last_sync_file_path, 'w', encoding='utf-8') as f:
                f.write(sync_time_str)
            logging.info(f"Saved last sync time {sync_time_str} to {self.last_sync_file_path}")
        except IOError as e:
             logging.error(f"Failed to save last sync time to {self.last_sync_file_path}: {e}", exc_info=True)
        except Exception as e:
             logging.error(f"Unexpected error saving last sync time: {e}", exc_info=True)