"""
Language detection service using langdetect library.
"""
from langdetect import detect, LangDetectException
import logging

logger = logging.getLogger(__name__)


class LanguageDetector:
    """Robust language detection service."""

    KNOWN_LANGUAGES = {
        'en', 'pt', 'es', 'fr', 'de', 'it', 'ja', 'zh-cn', 'zh-tw', 'ko',
        'ar', 'ru', 'nl', 'pl', 'tr', 'vi', 'th', 'id', 'hi', 'sv'
    }

    @staticmethod
    def detect_language(text: str) -> str:
        """
        Detect language from text using langdetect.

        Args:
            text: Input text

        Returns:
            ISO 639-1 language code (default: 'en')
        """
        if not text or len(text.strip()) < 3:
            return 'en'

        try:
            detected = detect(text)
            return detected if detected in LanguageDetector.KNOWN_LANGUAGES else 'en'
        except LangDetectException:
            logger.debug(f"Language detection failed for text: {text[:50]}...")
            return 'en'

    @staticmethod
    def get_language_name(code: str) -> str:
        """Get human-readable language name."""
        language_names = {
            'en': 'English', 'pt': 'Portuguese', 'es': 'Spanish', 'fr': 'French',
            'de': 'German', 'it': 'Italian', 'ja': 'Japanese', 'zh-cn': 'Chinese (Simplified)',
            'zh-tw': 'Chinese (Traditional)', 'ko': 'Korean', 'ar': 'Arabic', 'ru': 'Russian',
            'nl': 'Dutch', 'pl': 'Polish', 'tr': 'Turkish', 'vi': 'Vietnamese',
            'th': 'Thai', 'id': 'Indonesian', 'hi': 'Hindi', 'sv': 'Swedish'
        }
        return language_names.get(code, 'Unknown')