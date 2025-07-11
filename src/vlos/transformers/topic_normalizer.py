"""
Topic Normalizer for VLOS Processing

Handles normalization of topic text by removing common prefixes and standardizing format
for more accurate fuzzy matching.
"""

import re
from typing import List

from ..config import VlosConfig


class TopicNormalizer:
    """Normalizes topic text for better matching"""
    
    def __init__(self, config: VlosConfig):
        self.config = config
        self._build_prefix_regex()
    
    def normalize(self, text: str) -> str:
        """Normalize topic text by removing prefixes and standardizing format"""
        if not text:
            return ''
        
        text = text.strip().lower()
        
        # Remove common topic prefix once
        text = self._prefix_regex.sub('', text, count=1)
        
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _build_prefix_regex(self):
        """Build regex pattern for common topic prefixes"""
        prefixes = self.config.matching.common_topic_prefixes
        escaped_prefixes = [re.escape(p) for p in prefixes]
        pattern = r'^(' + '|'.join(escaped_prefixes) + r')[\s:,-]+'
        self._prefix_regex = re.compile(pattern, re.IGNORECASE) 