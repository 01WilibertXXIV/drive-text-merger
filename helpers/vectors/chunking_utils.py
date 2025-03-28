import re
def markdown_based_chunking(text, max_chunk_size=500):
    # Split by major headings first (## or higher)
    sections = re.split(r'(?<=\n)#{1,3}\s+[^\n]+\n', text)
    
    chunks = []
    
    for section in sections:
        # If section is small enough, keep it whole
        if len(section.split()) <= max_chunk_size:
            chunks.append(section)
        else:
            # Split further by subheadings or paragraphs
            subsections = re.split(r'(?<=\n)#{4,6}\s+[^\n]+\n|(?<=\n\n)', section)
            
            current_chunk = []
            current_size = 0
            
            for subsection in subsections:
                subsection_size = len(subsection.split())
                
                # If adding this subsection exceeds max size, save current chunk and start new one
                if current_size + subsection_size > max_chunk_size and current_size > 0:
                    chunks.append(" ".join(current_chunk))
                    current_chunk = [subsection]
                    current_size = subsection_size
                else:
                    current_chunk.append(subsection)
                    current_size += subsection_size
            
            # Don't forget the last chunk
            if current_chunk:
                chunks.append(" ".join(current_chunk))
    
    return chunks