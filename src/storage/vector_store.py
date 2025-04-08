import marqo
import logging
from config import MARQO_URL, MARQO_INDEX_NAME
from src.processing.document import ProcessedDocument

class MarqoManager:
    def __init__(self, url=MARQO_URL, index_name=MARQO_INDEX_NAME):
        self.url = url
        self.index_name = f"i{index_name}"
        self.client = None
        self._connect()

    def _connect(self):
        try:
            self.client = marqo.Client(url=self.url)
            print(f"Connected to Marqo at {self.url}, index '{self.index_name}'")
            logging.info(f"Connected to Marqo at {self.url}, index '{self.index_name}'")
        except Exception as e:
            logging.error(f"Failed to connect to Marqo at {self.url}: {e}", exc_info=True)
            self.client = None # Ensure client is None on failure

    def ensure_index_exists(self):
         if not self.client: return False
         try:
             # Simplified check - attempts to get stats, if fails assumes index might not exist
             self.client.index(self.index_name).get_stats()
         except marqo.errors.MarqoWebError as e:
              if e.code == "index_not_found":
                   logging.warning(f"Marqo index '{self.index_name}' not found. Attempting to create.")
                   try:
                        print(f"Creating Marqo index '{self.index_name}'")
                        settings = {
                            "model": "hf/e5-large-v2",
                            "normalizeEmbeddings": True, # Top-level field in v2.16
                            "textPreprocessing": {      # Top-level field in v2.16
                                "splitLength": 2,       # Default Marqo splitting settings
                                "splitOverlap": 0,      # Less critical if you pre-chunk well
                                "splitMethod": "sentence"
                            },
                            "annParameters": {          # Top-level field in v2.16
                                "spaceType": "angular",
                                "parameters": {
                                    "efConstruction": 128,
                                    "m": 16
                                }
                            },
                            "treatUrlsAndPointersAsImages": False, # Top-level field in v2.16
                        }


                        self.client.create_index(index, settings_dict=settings) # Add model/settings if needed
                        print(f"Successfully created Marqo index '{self.index_name}'")
                        logging.info(f"Successfully created Marqo index '{self.index_name}'")
                        return True
                   except Exception as create_error:
                        logging.error(f"Failed to create Marqo index '{self.index_name}': {create_error}", exc_info=True)
                        print(f"Failed to create Marqo index '{self.index_name}': {create_error}")
                        return False
              else:
                   logging.error(f"Error accessing Marqo index '{self.index_name}': {e}", exc_info=True)
                   return False
         return True


    def upsert_document(self, doc: ProcessedDocument):
        if not self.client or not doc.chunks: return False # Need client and chunks

        vector_store_docs = []
        for i, chunk in enumerate(doc.chunks):
            vector_store_docs.append({
                "_id": f"{doc.id}_chunk_{i}", # Unique ID per chunk
                "title": doc.name,
                "content": chunk,
                "file_id": doc.id, # Link back to the original file
                "url": doc.url,
                "mime_type": doc.mime_type,
                "modified_time": doc.modified_time_str,
                "created_time": doc.created_time_str,
            })

        if not vector_store_docs:
             logging.warning(f"No chunks generated for {doc.name} ({doc.id}). Skipping vector store upsert.")
             return False

        try:
             # Use add_documents with client_batch_size for efficiency
             response = self.client.index(self.index_name).add_documents(
                 documents=vector_store_docs,
                 tensor_fields=["content"], # Field to vectorize
                 client_batch_size=50 # Adjust batch size as needed
             )
             logging.info(f"Upserted {len(doc.chunks)} chunks for {doc.name} ({doc.id}) to Marqo.")
             # Basic check for errors in response if needed
             if response.get("errors", False):
                  logging.warning(f"Potential errors during Marqo upsert for {doc.id}: {response['errors']}")
             return True
        except Exception as e:
             logging.error(f"Failed to upsert document {doc.id} to Marqo: {e}", exc_info=True)
             return False

    def delete_document(self, file_id):
         if not self.client: return False
         try:
             # Marqo deletes by document ID. We need to delete all chunks.
             # Method 1: Query for all chunk IDs and delete them (safer if chunk IDs are stable)
             # Method 2: Use filter string deletion (more efficient)
             response = self.client.index(self.index_name).delete_documents(
                 filter_string=f"file_id:{file_id}"
             )
             logging.info(f"Deleted chunks for file_id {file_id} from Marqo. Response: {response}")
             return True
         except Exception as e:
             logging.error(f"Failed to delete document {file_id} from Marqo: {e}", exc_info=True)
             return False