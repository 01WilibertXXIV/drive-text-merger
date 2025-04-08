# helpers/vector_store_marqo.py
import marqo
import logging
import uuid # To generate unique chunk IDs if needed

def setup_marqo_index(index_name: str):
    """Creates the Marqo index if it doesn't exist with recommended settings."""

    print("Setting up Marqo Vector Store...")
    MARQO_URL = "http://gk4k0ckgck04g04ow8w08wws.100.71.51.35.sslip.io/"
    mq = marqo.Client(url=MARQO_URL)
    print("Connecting to Marqo...")

    try:
        print(f"Checking if Marqo index '{index_name}' exists...")
        mq.index(index_name).get_stats()
        logging.info(f"Marqo index '{index_name}' already exists.")
        return mq
    except Exception: # TODO: Check for specific "index not found" error
        logging.info(f"Creating Marqo index '{index_name}'...")
        try:
            # Define index settings - adjust model as needed
            # Make file_id filterable for easy deletion/updates

            # Corrected FLATTENED structure for Marqo v2.16:
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
            print(f"Attempting to create Marqo index '{index_name}' with v2.16 settings: {settings}") # Updated log
            logging.info(f"Attempting to create Marqo index '{index_name}' with v2.16 settings: {settings}") # Updated log
            mq.create_index(index_name, settings_dict=settings)
            print(f"Successfully created Marqo index '{index_name}'.")
            logging.info(f"Successfully created Marqo index '{index_name}'.")
            return mq
        except Exception as e:
            logging.error(f"Failed to create Marqo index '{index_name}': {e}", exc_info=True)
            raise # Re-raise the exception to signal failure

def upsert_document_marqo(
    vector_store_client: marqo.Client,
    index_name: str,
    file_id: str,
    chunks: list[str],
    metadata: dict # Contains file_name, url, mimeType, etc.
):
    """
    Upserts document chunks into a Marqo index.
    It first deletes existing chunks for the file_id, then adds the new ones.

    Args:
        vector_store_client: An initialized Marqo client instance.
        index_name: The name of the Marqo index.
        file_id: The unique identifier for the original document (Google Drive file ID).
        chunks: A list of text chunks from the document.
        metadata: A dictionary containing metadata associated with the original document.
    """
    if not chunks:
        logging.warning(f"No chunks provided for file_id {file_id}. Skipping Marqo upsert.")
        return

    # 1. Delete existing chunks for this file_id to ensure freshness
    #    This uses filtering based on the 'file_id' metadata field.
    try:
        logging.debug(f"Attempting to delete existing chunks for file_id: {file_id} from index '{index_name}'")
        # Use wait_for_completion=True for synchronous deletion, False for async
        delete_response = vector_store_client.index(index_name).delete_documents(
            filter_string=f"file_id:\"{file_id}\"", # Ensure file_id is filterable in index settings
            wait_for_completion=True # Easier to reason about for sync process
        )
        logging.debug(f"Marqo deletion response for file_id {file_id}: {delete_response}")
        if delete_response.get('status') == 'succeeded':
             logging.info(f"Successfully deleted existing chunks ({delete_response.get('deletedDocuments', 'N/A')}) for file_id: {file_id}")
        elif delete_response.get('status') in ['processing', 'deleting']:
             logging.warning(f"Marqo deletion still processing for file_id {file_id}. New chunks will be added.")
        else:
             logging.warning(f"Marqo deletion might have failed or no documents found for file_id {file_id}. Status: {delete_response.get('status', 'Unknown')}, Details: {delete_response.get('details', '')}")

    except Exception as e:
        # Handle cases where the index might not exist yet or filter fails
        logging.warning(f"Could not delete existing chunks for file_id {file_id} (maybe none existed): {e}")


    # 2. Prepare documents for Marqo's add_documents
    marqo_docs = []
    for i, chunk_text in enumerate(chunks):
        # Create a unique ID for each chunk, but keep file_id for filtering/grouping
        chunk_id = f"{file_id}_{i}" # Simple, predictable chunk ID
        # chunk_id = str(uuid.uuid4()) # Or use UUIDs if predictability isn't needed

        doc = {
            "_id": chunk_id,         # Unique ID for this specific chunk
            "chunk_text": chunk_text, # The actual text to be vectorized (tensor field)
            "file_id": file_id,       # Original document ID (filterable metadata)
            **metadata             # Add other metadata (name, url, mimeType, etc.)
                                   # Ensure these keys match filterable_attributes if needed
        }
        marqo_docs.append(doc)

    try:
        for doc in marqo_docs:
            try:
                add_response = vector_store_client.index(index_name).add_documents(
                    documents=[doc], tensor_fields=["chunk_text"]
                )
                logging.info(f"Successfully added document {doc['_id']}")
            except Exception as e:
                #print(f"Error adding document {doc['_id']}: {e}")
                logging.error(f"Error adding document {doc['_id']}: {e}", exc_info=True)
    

    except Exception as e:
        logging.error(f"Failed to add documents to Marqo for file_id {file_id}: {e}", exc_info=True)
        # Re-raise the exception to indicate the vector store update failed
        raise e

def delete_document_marqo(
    vector_store_client: marqo.Client,
    index_name: str,
    file_id: str
):
    """
    Deletes all chunks associated with a specific file_id from the Marqo index.

    Args:
        vector_store_client: An initialized Marqo client instance.
        index_name: The name of the Marqo index.
        file_id: The unique identifier for the original document to delete.
    """
    try:
        logging.info(f"Attempting to delete all chunks for file_id: {file_id} from index '{index_name}'")
        # Use filtering based on the 'file_id' metadata field.
        delete_response = vector_store_client.index(index_name).delete_documents(
            filter_string=f"file_id:\"{file_id}\"",
            wait_for_completion=True
        )
        logging.debug(f"Marqo deletion response for file_id {file_id}: {delete_response}")
        if delete_response.get('status') == 'succeeded':
             logging.info(f"Successfully deleted {delete_response.get('deletedDocuments', 'N/A')} chunks for file_id: {file_id} from Marqo.")
        else:
            logging.warning(f"Marqo deletion status for file_id {file_id}: {delete_response.get('status', 'Unknown')}. Details: {delete_response.get('details', '')}")

    except Exception as e:
        logging.error(f"Error deleting document chunks from Marqo for file_id {file_id}: {e}", exc_info=True)
        # Depending on requirements, you might want to raise e

upsert_document = upsert_document_marqo
delete_document = delete_document_marqo
setup_vector_store = setup_marqo_index
