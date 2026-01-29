"""
Scrapers package - Data source implementations.

Provides scraper classes for each data source:
- WBOScraper: World Beyblade Organization forum data
- JPScraper: Japanese tournament data from okuyama3093.com
- DEScraper: German tournament data from BLG Instagram
"""

from .wbo import WBOScraper
from .jp import JPScraper
from .de import DEScraper

__all__ = ["WBOScraper", "JPScraper", "DEScraper"]
