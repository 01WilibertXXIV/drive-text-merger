import marqo
import hashlib
import json
import re

class MarqoDocumentManager:
    """Class to manage documents in Marqo with chunking and checksum support"""
    
    def __init__(self, marqo_client, index_name):
        """Initialize with a Marqo client and index name
        
        Args:
            marqo_client: Initialized Marqo client
            index_name: Name of the index to use
        """
        self.mq = marqo_client
        self.index_name = index_name
    
    def chunk_text(self, text, chunk_size=400, overlap=100):
        """Split text into overlapping chunks
        
        Args:
            text: Text to split
            chunk_size: Target size of each chunk in characters
            overlap: Character overlap between chunks
            
        Returns:
            list: List of text chunks
        """
        if len(text) <= chunk_size:
            return [text]
        
        # Split text into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        chunks = []
        current_chunk = []
        current_length = 0
        
        for sentence in sentences:
            sentence_length = len(sentence)
            
            if current_length + sentence_length > chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                
                # Keep some sentences for overlap
                overlap_size = 0
                overlap_chunk = []
                
                for s in reversed(current_chunk):
                    overlap_size += len(s)
                    overlap_chunk.insert(0, s)
                    if overlap_size >= overlap:
                        break
                        
                current_chunk = overlap_chunk
                current_length = sum(len(s) for s in current_chunk)
            
            current_chunk.append(sentence)
            current_length += sentence_length
        
        # Add the last chunk
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    def add_or_update_document(self, document, check_for_changes=True):
        """Add or update a single document in Marqo
        
        Args:
            document: Document to add or update (must include 'id')
            check_for_changes: Whether to check if content has changed before updating
            
        Returns:
            dict: Result of the operation
        """
        if 'id' not in document:
            raise ValueError("Document must have an 'id' field")
        
        doc_id = document['id']
        
        # Create a copy without checksum field
        doc_copy = document.copy()
        doc_copy.pop('checksum', None)
        
        # Calculate checksum
        checksum = document.get('checksum', None)
        
        # Check if document exists and has the same checksum
        if check_for_changes:
            try:
                existing_doc = self.mq.index(self.index_name).get_document(doc_id)
                if existing_doc and existing_doc.get('checksum') == checksum:
                    return {
                        'status': 'unchanged',
                        'message': f'Document {doc_id} has not changed, skipping update',
                        'checksum': checksum
                    }
            except Exception as e:
                # Document doesn't exist or other error, continue with add
                pass
        
        # Add checksum to document
        doc_with_checksum = doc_copy.copy()
        doc_with_checksum['checksum'] = checksum
        
        # Add/update the document
        response = self.mq.index(self.index_name).add_documents([doc_with_checksum])
        
        return {
            'status': 'updated',
            'message': f'Document {doc_id} added or updated',
            'checksum': checksum,
            'response': response
        }
    
    def add_or_update_chunked_document(self, document, chunk_size=400, overlap=100, check_for_changes=True):
        """Add or update a document with chunking
        
        Args:
            document: Document to add or update (must include 'id' and 'content')
            chunk_size: Size of chunks in characters
            overlap: Character overlap between chunks
            check_for_changes: Whether to check if content has changed before updating
            
        Returns:
            dict: Result of the operation
        """
        if 'id' not in document:
            raise ValueError("Document must have an 'id' field")
        if 'content' not in document:
            raise ValueError("Document must have a 'content' field")
        
        doc_id = document['id']
        content = document['content']
        
        # Create a copy of document without content for metadata
        metadata_doc = document.copy()
        metadata_doc.pop('content')
        
        # Calculate checksum of the content only
        content_checksum = document.get('checksum', None)
        
        # See if document exists and has the same checksum
        if check_for_changes:
            try:
                # Check for metadata document
                metadata_doc_id = f"{doc_id}_metadata"
                existing_meta = self.mq.index(self.index_name).get_document(metadata_doc_id)
                
                if existing_meta and existing_meta.get('content_checksum') == content_checksum:
                    return {
                        'status': 'unchanged',
                        'message': f'Document {doc_id} content has not changed, skipping update',
                        'checksum': content_checksum
                    }
            except Exception as e:
                # Document doesn't exist or other error, continue with add
                pass
                
        # Content has changed or document doesn't exist
        # First, delete all existing chunks and metadata
        try:
            # Delete metadata document if it exists
            self.mq.index(self.index_name).delete_documents(
                document_ids=[f"{doc_id}_metadata"]
            )
            # Delete all chunks
            self.mq.index(self.index_name).delete_documents(
                filter_string=f"parent_id:{doc_id}"
            )
        except Exception as e:
            # May fail if documents don't exist yet, that's okay
            pass
            
        # Create metadata document
        metadata_doc_id = f"{doc_id}_metadata"
        metadata_doc['id'] = metadata_doc_id
        metadata_doc['parent_id'] = doc_id
        metadata_doc['content_checksum'] = content_checksum
        metadata_doc['document_type'] = 'metadata'
        metadata_doc['chunks_count'] = 0  # Will update after chunking
        
        # Create chunks
        chunks = self.chunk_text(content, chunk_size, overlap)
        
        # Update metadata with chunk count
        metadata_doc['chunks_count'] = len(chunks)
        
        # Prepare all documents (metadata + chunks)
        all_docs = [metadata_doc]
        
        # Create chunk documents
        for i, chunk_content in enumerate(chunks):
            chunk_doc = {
                'id': f"{doc_id}_chunk_{i}",
                'parent_id': doc_id,
                'chunk_index': i,
                'total_chunks': len(chunks),
                'content': chunk_content,
                'document_type': 'chunk',
                'content_checksum': content_checksum  # Same checksum on all chunks
            }
            
            # Copy any other metadata fields from the original document
            for key, value in document.items():
                if key not in ['id', 'content'] and key not in chunk_doc:
                    chunk_doc[key] = value
            
            all_docs.append(chunk_doc)
        
        # Add all documents to index
        response = self.mq.index(self.index_name).add_documents(all_docs)
        
        return {
            'status': 'updated',
            'message': f'Document {doc_id} added or updated with {len(chunks)} chunks',
            'checksum': content_checksum,
            'chunks_count': len(chunks),
            'response': response
        }
    
    def search(self, query, filter_string=None, limit=10):
        """Search the index
        
        Args:
            query: Search query
            filter_string: Optional filter string
            limit: Maximum results to return
            
        Returns:
            dict: Search results
        """
        search_params = {
            'q': query,
            'limit': limit
        }
        
        if filter_string:
            search_params['filter_string'] = filter_string
        
        # Exclude metadata documents from results
        if filter_string:
            search_params['filter_string'] = f"({filter_string}) AND NOT document_type:metadata"
        else:
            search_params['filter_string'] = "NOT document_type:metadata"
        
        results = self.mq.index(self.index_name).search(**search_params)
        return results


# Example of using the manager:
def demo_usage():
    # Initialize Marqo client
    mq = marqo.Client(url="http://localhost:8882")
    index_name = "documents"
    
    # Create the manager
    doc_manager = MarqoDocumentManager(mq, index_name)
    
    # Example documents
    short_doc = {
        "id": "doc1",
        "title": "Short Document",
        "content": "This is a short document that won't be chunked."
    }
    
    long_doc = {
        "id": "doc2",
        "title": "Long Document",
        "content": """This is a longer document that will be split into multiple chunks for better searching.
        It contains multiple sentences and paragraphs that will be processed independently.
        Each chunk will maintain a reference to the parent document and will include the content checksum.
        This allows for efficient updates since we only re-chunk and re-index when the content actually changes."""
    }
    
    # Add/update documents
    doc_manager.add_or_update_document(short_doc)
    doc_manager.add_or_update_chunked_document(long_doc)
    
    # Search example
    search_results = doc_manager.search("efficient updates")
    
    # Print results
    print(f"Found {len(search_results['hits'])} results:")
    for hit in search_results["hits"]:
        print(f"- Score: {hit['_score']:.4f}")
        print(f"  Title: {hit.get('title', 'No title')}")
        print(f"  Content: {hit['content'][:100]}...")
        
        # Show chunk information if it's a chunk
        if hit.get('document_type') == 'chunk':
            print(f"  Chunk {hit['chunk_index'] + 1} of {hit['total_chunks']} (Parent ID: {hit['parent_id']})")
        
        print()

if __name__ == "__main__":
    demo_usage()