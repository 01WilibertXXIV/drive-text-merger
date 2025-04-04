from docx import Document
from docx.opc.exceptions import PackageNotFoundError
from typing import List, Dict, Any, Optional
import io
import uuid

def extract_and_chunk_docx_for_marqo(
    docx_bytes: bytes,
    file_url: Optional[str] = None,
    file_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Extracts text from a DOCX file, chunks it by paragraph,
    and formats it as a list of dictionaries suitable for Marqo indexing.

    Args:
        docx_bytes: The content of the DOCX file as bytes.
        file_url: Optional URL or path of the source file, used for metadata
                  and generating document ID.

    Returns:
        A list of dictionaries. Each dictionary represents a text chunk (paragraph)
        and contains the text content ('text' key) and associated metadata.
        Returns an empty list if the document cannot be processed or is empty.
    """
    chunks = []
    
    try:
        doc = Document(io.BytesIO(docx_bytes))
    except PackageNotFoundError:
        print(f"Warning: Could not open DOCX file (possibly corrupted or not a DOCX). URL: {file_url}")
        return [] # Return empty list for corrupted/invalid files
    except Exception as e:
        print(f"Error opening DOCX file {file_url}: {e}")
        return []


    # --- Metadata Extraction ---
    # Use file_url if available, otherwise generate a unique ID for the document
    # Consider sanitizing file_url if it contains sensitive info or weird characters
    document_id = file_id if file_id else f"doc_{uuid.uuid4()}"
    
    metadata = {
        "document_id": document_id,
        "source": file_url if file_url else "BytesIO",
        "file_type": "docx",
        # Initialize core properties with None
        "title": None,
        "author": None,
        "subject": None,
        "keywords": None,
        "created_date": None,
        "last_modified_date": None,
        "revision": None,
    }

    try:
        # Attempt to extract core properties
        core_properties = doc.core_properties
        metadata.update({
            "title": core_properties.title if core_properties.title else None,
            "author": core_properties.author if core_properties.author else None,
            "subject": core_properties.subject if core_properties.subject else None,
            "keywords": core_properties.keywords if core_properties.keywords else None,
             # Convert datetime objects to ISO 8601 string format if they exist
            "created_date": core_properties.created.isoformat() if core_properties.created else None,
            "last_modified_date": core_properties.modified.isoformat() if core_properties.modified else None,
            "revision": core_properties.revision,
        })
    except AttributeError:
        # Some documents might not have core_properties initialized properly
        print(f"Warning: Could not access core_properties for {file_url}")
    except Exception as e:
         print(f"Error extracting metadata for {file_url}: {e}")

    # --- Chunking ---
    chunk_index = 0
    for i, para in enumerate(doc.paragraphs):
        text = para.text.strip()

        # Skip empty paragraphs
        if not text:
            continue

        # Create a unique ID for this specific chunk within the document
        # Format: {document_id}_chunk_{index}
        chunk_id = f"{document_id}_chunk_{chunk_index}"

        chunk_data = {
            "_id": chunk_id,  # Marqo uses _id by default
            "text": text,      # The actual text content of the chunk (paragraph)
            "chunk_index": chunk_index, # Sequence number of the chunk
            # Add all collected document-level metadata to each chunk
            **metadata
        }

        chunks.append(chunk_data)
        chunk_index += 1
        
    # You could potentially add table content extraction here as well,
    # treating each cell or row as a chunk, similar to paragraphs.

    # Add paragraph count to metadata after processing
    if chunks: # Only update if chunks were generated
      for chunk in chunks:
          chunk["total_chunks_in_doc"] = chunk_index

    return chunks