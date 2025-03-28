import docx
import io
import logging
from typing import Union, Optional
from docx import Document
from pdfminer.high_level import extract_text

def extract_text_from_pdf(pdf_bytes: Union[bytes, io.BytesIO], file_url: Optional[str] = None) -> str:
    """
    Extract text from a PDF while maintaining full lines and proper paragraph structure.
    """
    extraction_methods = [
        _extract_with_pymupdf,
        _extract_with_pdfminer,
        _extract_with_ocr
    ]

    text = ""
    metadata_lines = []
    page_count = 0

    # Try to extract metadata if using PyMuPDF
    try:
        import fitz
        with fitz.open("pdf", pdf_bytes) as doc:
            page_count = len(doc)
            if doc.metadata:
                if doc.metadata.get('author'):
                    metadata_lines.append(f"Author: {doc.metadata['author']}")
                if doc.metadata.get('creationDate'):
                    metadata_lines.append(f"Created: {doc.metadata['creationDate']}")
    except:
        pass

    metadata_lines.append(f"## END METADATA ##")

    # Try each method in sequence
    for method in extraction_methods:
        try:
            text = method(pdf_bytes)
            if text and text.strip():
                break  # Stop at first successful extraction
        except Exception as e:
            logging.warning(f"Extraction method {method.__name__} failed: {e}")

    if not text.strip():
        return "## METADATA ##\nType: PDF\nStatus: Extraction Failed\n## END METADATA ##\n\n## CONTENT ##\nThis PDF format is not supported yet.\n## END CONTENT ##"

    # Normalize newlines while maintaining paragraphs
    lines = text.splitlines()
    content_lines = []
    paragraph = []

    for line in lines:
        stripped_line = line.strip()

        if stripped_line:
            paragraph.append(stripped_line)  # Add non-empty lines to paragraph
        else:
            if paragraph:
                content_lines.append(" ".join(paragraph))  # Join broken sentences into a full paragraph
                paragraph = []  # Reset for the next paragraph

    # Append last paragraph if exists
    if paragraph:
        content_lines.append(" ".join(paragraph))
    
    # Add basic metadata if not already extracted
    metadata_lines.append(f"Type: PDF")
    if page_count:
        metadata_lines.append(f"Pages: {page_count}")
    metadata_lines.append(f"Paragraphs: {len(content_lines)}")
    
    # Build the final output with section markers
    output = []
    output.append("\n".join(metadata_lines))
    output.append("## END METADATA ##")
    output.append("")
    output.append("## CONTENT ##") 
    output.append("\n\n".join(content_lines))
    output.append("## END CONTENT ##")
    
    return "\n".join(output)

def _extract_with_pymupdf(pdf_bytes: Union[bytes, io.BytesIO]) -> Optional[str]:
    """
    Extract text using PyMuPDF (fitz)
    """
    import fitz
    
    with fitz.open("pdf", pdf_bytes) as doc:
        if doc.is_encrypted:
            return "PDF is encrypted"
        
        text = ""
        for page in doc:
            text += page.get_text() + "\n"
        
        return text.strip() or None

def _extract_with_pdfminer(pdf_bytes: Union[bytes, io.BytesIO]) -> Optional[str]:
    """
    Extract text using PDFMiner
    """
    from pdfminer.high_level import extract_text
    import io
    
    text = extract_text(io.BytesIO(pdf_bytes))
    return text.strip() or None

def _extract_with_ocr(pdf_bytes: Union[bytes, io.BytesIO]) -> Optional[str]:
    """
    Extract text using OCR as a last resort
    """
    from pdf2image import convert_from_bytes
    import pytesseract
    
    # Convert PDF to images
    images = convert_from_bytes(pdf_bytes)
    
    # Try multiple languages
    languages = ['fra', 'eng']
    
    for lang in languages:
        try:
            text = "\n".join(
                pytesseract.image_to_string(img, lang=lang) 
                for img in images
            )
            
            if text.strip():
                return text.strip()
        except Exception as e:
            logging.warning(f"OCR with {lang} language failed: {e}")
    
    # Final fallback - try without language specification
    try:
        text = "\n".join(
            pytesseract.image_to_string(img) 
            for img in images
        )
        return text.strip() or None
    except Exception as e:
        logging.error(f"Final OCR attempt failed: {e}")
        return None



def extract_text_from_docx(docx_bytes, file_url: Optional[str] = None):
    """Extracts text from a DOCX file and converts it to Markdown."""
    doc = Document(io.BytesIO(docx_bytes))
    content_lines = []
    
    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue

        # Handle headings
        if para.style.name.startswith("Heading"):
            level = int(para.style.name[-1])  # Get heading level (e.g., "Heading 1" → level 1)
            content_lines.append(f"{'#' * level} {text}")
        # Handle bold and italic text
        elif any(run.bold for run in para.runs):
            content_lines.append(f"**{text}**")
        elif any(run.italic for run in para.runs):
            content_lines.append(f"*{text}*")
        else:
            content_lines.append(text)
    
    content = "\n\n".join(content_lines)
    
    # Extract document properties if available
    metadata_lines = []
    try:
        core_properties = doc.core_properties
        if core_properties.author:
            metadata_lines.append(f"Author: {core_properties.author}")
        if core_properties.created:
            metadata_lines.append(f"Created: {core_properties.created}")
    except:
        pass
    
    metadata_lines.append(f"Type: Document")
    metadata_lines.append(f"Paragraphs: {len(content_lines)}")
    
    # Build the final output with section markers
    output = []
    output.append("\n".join(metadata_lines))
    output.append("## END METADATA ##")
    output.append("")
    output.append("## CONTENT ##")
    output.append(content)
    output.append("## END CONTENT ##")
    
    return "\n".join(output)