"""
VLOS (Verslag Online System) Processing Package

A modular, scalable system for processing parliamentary session data from VLOS XML files
and linking them to TK API entities with comprehensive parliamentary discourse analysis.
"""

__version__ = "2.0.0"
__author__ = "Parliamentary Data Analysis Team"

from .pipeline.vlos_pipeline import VlosPipeline
from .config.vlos_config import VlosConfig

__all__ = [
    'VlosPipeline',
    'VlosConfig'
] 