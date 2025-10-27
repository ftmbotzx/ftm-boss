"""
Translation module for FTM Boss Assistant
Handles translation of Gujarati text to English
"""

import asyncio
import logging
from typing import Optional

from googletrans import Translator as GoogleTranslator
from googletrans.models import Translated

from config import Config

logger = logging.getLogger(__name__)


class Translator:
    """Translation service for converting Gujarati text to English"""
    
    def __init__(self):
        self.config = Config()
        self.translator = GoogleTranslator()
        
        # Translation cache to avoid repeated translations
        self._translation_cache = {}
    
    def _clean_text(self, text: str) -> str:
        """Clean text before translation"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove special characters that might cause issues
        # Keep Gujarati characters, English characters, numbers, and basic punctuation
        import re
        text = re.sub(r'[^\u0A80-\u0AFF\u0900-\u097F\w\s\.\,\!\?\:\;\-\(\)\[\]\"\']+', '', text)
        
        return text.strip()
    
    def _detect_language(self, text: str) -> Optional[str]:
        """Detect language of the text"""
        try:
            detection = self.translator.detect(text)
            if hasattr(detection, 'lang'):
                return detection.lang
            elif isinstance(detection, list) and len(detection) > 0:
                return detection[0]
            else:
                return None
        except Exception as e:
            logger.warning(f"Language detection failed: {e}")
            return None
    
    async def translate_to_english(self, text: str) -> str:
        """Translate text to English"""
        if not text or not text.strip():
            return ""
        
        # Clean the text
        cleaned_text = self._clean_text(text)
        if not cleaned_text:
            return ""
        
        # Check cache first
        if cleaned_text in self._translation_cache:
            logger.debug("Using cached translation")
            return self._translation_cache[cleaned_text]
        
        def _translate():
            """Synchronous translation function"""
            try:
                # Detect language first
                detected_lang = self._detect_language(cleaned_text)
                
                # If already in English, return as is
                if detected_lang == 'en':
                    logger.debug("Text is already in English")
                    return cleaned_text
                
                # Translate to English
                if detected_lang == 'gu':  # Gujarati
                    result = self.translator.translate(cleaned_text, src='gu', dest='en')
                else:
                    # Auto-detect source language
                    result = self.translator.translate(cleaned_text, dest='en')
                
                if isinstance(result, Translated) and result.text:
                    translated_text = result.text.strip()
                    logger.debug(f"Translation successful: '{cleaned_text[:50]}...' -> '{translated_text[:50]}...'")
                    return translated_text
                else:
                    logger.warning("Translation returned empty result")
                    return "[Translation failed]"
                    
            except Exception as e:
                logger.error(f"Translation error: {e}")
                return "[Translation unavailable]"
        
        # Run translation in executor to avoid blocking
        loop = asyncio.get_event_loop()
        translation = await loop.run_in_executor(None, _translate)
        
        # Cache the translation
        if translation and not translation.startswith('['):
            self._translation_cache[cleaned_text] = translation
            
            # Limit cache size
            if len(self._translation_cache) > 1000:
                # Remove oldest entries (simple FIFO)
                oldest_keys = list(self._translation_cache.keys())[:100]
                for key in oldest_keys:
                    del self._translation_cache[key]
        
        return translation
    
    async def translate_batch(self, texts: list) -> list:
        """Translate multiple texts in batch"""
        translations = []
        
        for text in texts:
            translation = await self.translate_to_english(text)
            translations.append(translation)
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.1)
        
        return translations
    
    def clear_cache(self):
        """Clear translation cache"""
        self._translation_cache.clear()
        logger.info("Translation cache cleared")
    
    def get_cache_size(self) -> int:
        """Get current cache size"""
        return len(self._translation_cache)


# Test function for development
async def test_translator():
    """Test function for the translator"""
    translator = Translator()
    
    # Test Gujarati text
    gujarati_text = "શૈક્ષણિક વર્ષ:૨૦૨૫-૨૬માં અનુસ્નાતક કક્ષાના વિદ્યાર્થીઓ માટે પાંચમાં તબક્કાના સમયપત્રક બાબત"
    
    logger.info(f"Testing translation of: {gujarati_text}")
    
    translation = await translator.translate_to_english(gujarati_text)
    
    logger.info(f"Translation result: {translation}")
    
    # Test English text
    english_text = "This is already in English"
    english_result = await translator.translate_to_english(english_text)
    
    logger.info(f"English text result: {english_result}")


if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Run test
    asyncio.run(test_translator())
