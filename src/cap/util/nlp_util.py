import simplemma
import logging
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

@staticmethod
def lemmatize_text(text: str, lang: str = 'en') -> str:
    """
    Convert all words to their base form (lemma) using simplemma.
    Handles plurals, verb conjugations, and morphological variations.

    Args:
        text: Text to lemmatize
        lang: Language code (en, de, es, fr, it, nl, pt, etc.)

    Returns:
        Lemmatized text with preserved placeholder tokens
    """
    words = text.split()

    lemmatized_words = []
    for word in words:
        # Preserve placeholder patterns (ENTITY_, <<...>>)
        if word.startswith('ENTITY_') or (word.startswith('<<') and word.endswith('>>')):
            lemmatized_words.append(word)
        # Preserve underscored expressions (stake_pool, etc.)
        elif '_' in word:
            lemmatized_words.append(word)
        else:
            # Lemmatize regular words using simplemma
            lemma = simplemma.lemmatize(word, lang=lang)
            lemmatized_words.append(lemma)

    return ' '.join(lemmatized_words)