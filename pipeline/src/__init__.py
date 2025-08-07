"""
Tree11 Data Pipeline Source Package
=====================================

This package contains all the core modules for the Tree11 data pipeline:

- data_extractor: API data extraction from Gymly and Google Sheets
- data_transformer: Data transformation and schema mapping 
- database_manager: SQL Server database operations
- google_sheets_manager: Google Sheets integration
- logger: Logging and monitoring setup
- pipeline_runner: Main pipeline orchestration
- utils: Common utility functions

Author: Greit IT Consultancy
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "Greit IT Consultancy"

# Import main classes for easy access
from .data_extractor import DataExtractor, GymlyAPIClient
from .data_transformer import DataTransformer
from .database_manager import DatabaseManager
from .logger import setup_logging
from .utils import load_config

__all__ = [
    "DataExtractor",
    "GymlyAPIClient", 
    "DataTransformer",
    "DatabaseManager",
    "setup_logging",
    "load_config"
] 