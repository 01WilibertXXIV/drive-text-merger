import docx
import io
import logging
from typing import Union, Optional

def extract_text_from_pdf(pdf_bytes: Union[bytes, io.BytesIO]) -> str:
    """
    Comprehensive PDF text extraction with multiple fallback methods.
    
    Args:
        pdf_bytes (bytes or BytesIO): PDF file content in bytes
    
    Returns:
        str: Extracted text from the PDF
    """
    # List of extraction methods to try
    extraction_methods = [
        _extract_with_pymupdf,
        _extract_with_pdfminer,
        _extract_with_ocr
    ]
    
    # Try each method in sequence
    for method in extraction_methods:
        try:
            text = method(pdf_bytes)
            if text and text.strip():
                return text
        except Exception as e:
            logging.warning(f"Extraction method {method.__name__} failed: {e}")
    
    return "This PDF format is not supported yet."

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

def extract_text_from_docx(file_data):
    """Extract text from a Word document."""
    doc = docx.Document(io.BytesIO(file_data))
    return "\n".join([paragraph.text for paragraph in doc.paragraphs])