"""
Scrapers package - Data source implementations.

Provides scraper classes for each data source:
- WBOScraper: World Beyblade Organization forum data
- JPScraper: Japanese tournament data from okuyama3093.com
"""

from .wbo import WBOScraper
from .jp import JPScraper
from .fandom import FandomScraper

__all__ = ["WBOScraper", "JPScraper", "FandomScraper"]
