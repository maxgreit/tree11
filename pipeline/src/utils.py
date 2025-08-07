"""
Tree11 Data Pipeline - Utility Functions
Common utility functions for configuration, date/time handling, and environment variables
"""

import json
import os
import re
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import logging

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def load_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load JSON configuration file with environment variable substitution
    
    Args:
        config_path: Path to JSON configuration file
        
    Returns:
        Configuration dictionary with substituted environment variables
    """
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_text = f.read()
        
        # Substitute environment variables
        config_text = substitute_env_vars(config_text)
        
        # Parse JSON
        config = json.loads(config_text)
        
        logger.debug(f"Configuration loaded - config_file={str(config_path)}")
        return config
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file - config_file={str(config_path)}, error={str(e)}")
        raise ValueError(f"Invalid JSON in {config_path}: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to load configuration - config_file={str(config_path)}, error={str(e)}")
        raise


def substitute_env_vars(text: str) -> str:
    """
    Substitute environment variables in text using ${VAR_NAME} syntax
    
    Args:
        text: Text containing environment variable references
        
    Returns:
        Text with environment variables substituted
    """
    def replace_env_var(match):
        var_name = match.group(1)
        var_value = os.getenv(var_name)
        
        if var_value is None:
            logger.warning(f"Environment variable not found - var_name={var_name}")
            return match.group(0)  # Return original if not found
        
        return var_value
    
    # Pattern to match ${VAR_NAME}
    pattern = r'\$\{([^}]+)\}'
    return re.sub(pattern, replace_env_var, text)


def load_environment_file(env_file: str = '.env'):
    """
    Load environment variables from .env file
    
    Args:
        env_file: Path to .env file
    """
    env_path = Path(env_file)
    
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Environment file loaded - env_file={str(env_path)}")
    else:
        logger.warning(f"Environment file not found - env_file={str(env_path)}")


def get_env_var(var_name: str, default: Optional[str] = None, 
               required: bool = False) -> Optional[str]:
    """
    Get environment variable with optional default and validation
    
    Args:
        var_name: Name of environment variable
        default: Default value if variable not found
        required: Whether the variable is required
        
    Returns:
        Environment variable value or default
        
    Raises:
        ValueError: If required variable is not found
    """
    value = os.getenv(var_name, default)
    
    if required and value is None:
        raise ValueError(f"Required environment variable not found: {var_name}")
    
    return value


def parse_date_string(date_str: str, formats: Optional[List[str]] = None) -> Optional[date]:
    """
    Parse date string using multiple formats
    
    Args:
        date_str: Date string to parse
        formats: List of date formats to try (uses defaults if None)
        
    Returns:
        Parsed date object or None if parsing fails
    """
    if not date_str or not date_str.strip():
        return None
    
    if formats is None:
        formats = [
            '%Y-%m-%d',      # 2023-12-01
            '%d-%m-%Y',      # 01-12-2023
            '%d/%m/%Y',      # 01/12/2023
            '%m/%d/%Y',      # 12/01/2023
            '%d-%m-%y',      # 01-12-23
            '%d/%m/%y',      # 01/12/23
            '%Y%m%d',        # 20231201
        ]
    
    date_str = date_str.strip()
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    logger.warning(f"Unable to parse date string - date_str={date_str}")
    return None


def parse_datetime_string(datetime_str: str, formats: Optional[List[str]] = None) -> Optional[datetime]:
    """
    Parse datetime string using multiple formats
    
    Args:
        datetime_str: Datetime string to parse
        formats: List of datetime formats to try (uses defaults if None)
        
    Returns:
        Parsed datetime object or None if parsing fails
    """
    if not datetime_str or not datetime_str.strip():
        return None
    
    if formats is None:
        formats = [
            '%Y-%m-%dT%H:%M:%S',       # 2023-12-01T14:30:00
            '%Y-%m-%dT%H:%M:%SZ',      # 2023-12-01T14:30:00Z
            '%Y-%m-%dT%H:%M:%S.%f',    # 2023-12-01T14:30:00.123456
            '%Y-%m-%dT%H:%M:%S.%fZ',   # 2023-12-01T14:30:00.123456Z
            '%Y-%m-%d %H:%M:%S',       # 2023-12-01 14:30:00
            '%d-%m-%Y %H:%M:%S',       # 01-12-2023 14:30:00
        ]
    
    datetime_str = datetime_str.strip()
    
    # Handle ISO format with timezone offset
    if datetime_str.endswith('Z'):
        datetime_str = datetime_str[:-1] + '+00:00'
    
    # Try built-in ISO parsing first
    try:
        return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
    except ValueError:
        pass
    
    # Try manual formats
    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Unable to parse datetime string - datetime_str={datetime_str}")
    return None


def get_date_range(start_date: Optional[date] = None, 
                  end_date: Optional[date] = None,
                  days_back: int = 1,
                  days_forward: int = 0) -> tuple[date, date]:
    """
    Get date range with sensible defaults
    
    Args:
        start_date: Start date (defaults to days_back from today)
        end_date: End date (defaults to days_forward from today)
        days_back: Days back from today for start_date
        days_forward: Days forward from today for end_date
        
    Returns:
        Tuple of (start_date, end_date)
    """
    today = date.today()
    
    if start_date is None:
        start_date = today - timedelta(days=days_back)
    
    if end_date is None:
        end_date = today + timedelta(days=days_forward)
    
    return start_date, end_date


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human readable string
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_number(number: Union[int, float], 
                 decimal_places: int = 0,
                 thousands_separator: str = '.') -> str:
    """
    Format number with thousands separator (Dutch style)
    
    Args:
        number: Number to format
        decimal_places: Number of decimal places
        thousands_separator: Thousands separator character
        
    Returns:
        Formatted number string
    """
    if decimal_places == 0:
        formatted = f"{int(number):,}".replace(',', thousands_separator)
    else:
        formatted = f"{number:,.{decimal_places}f}".replace(',', 'TEMP').replace('.', ',').replace('TEMP', thousands_separator)
    
    return formatted


def sanitize_filename(filename: str, replacement: str = '_') -> str:
    """
    Sanitize filename by removing/replacing invalid characters
    
    Args:
        filename: Original filename
        replacement: Character to replace invalid characters with
        
    Returns:
        Sanitized filename
    """
    # Remove or replace invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, replacement)
    
    # Remove leading/trailing whitespace and dots
    filename = filename.strip(' .')
    
    # Limit length
    if len(filename) > 255:
        filename = filename[:255]
    
    return filename


def deep_merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries
    
    Args:
        dict1: Base dictionary
        dict2: Dictionary to merge into dict1
        
    Returns:
        Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result


def flatten_dict(d: Dict[str, Any], parent_key: str = '', separator: str = '.') -> Dict[str, Any]:
    """
    Flatten nested dictionary
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key prefix
        separator: Key separator
        
    Returns:
        Flattened dictionary
    """
    items = []
    
    for key, value in d.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    
    return dict(items)


def validate_email(email: str) -> bool:
    """
    Validate email address format
    
    Args:
        email: Email address to validate
        
    Returns:
        True if valid email format, False otherwise
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def mask_sensitive_data(data: str, mask_char: str = '*', 
                       visible_chars: int = 4) -> str:
    """
    Mask sensitive data for logging
    
    Args:
        data: Sensitive data to mask
        mask_char: Character to use for masking
        visible_chars: Number of characters to leave visible at start/end
        
    Returns:
        Masked data string
    """
    if len(data) <= visible_chars * 2:
        return mask_char * len(data)
    
    start = data[:visible_chars]
    end = data[-visible_chars:]
    middle = mask_char * (len(data) - visible_chars * 2)
    
    return f"{start}{middle}{end}"


def retry_operation(operation, max_attempts: int = 3, delay: float = 1.0,
                   backoff_factor: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Retry operation with exponential backoff
    
    Args:
        operation: Function to retry
        max_attempts: Maximum number of attempts
        delay: Initial delay between attempts
        backoff_factor: Multiplier for delay on each retry
        exceptions: Tuple of exceptions to catch and retry on
        
    Returns:
        Result of successful operation
        
    Raises:
        Last exception if all attempts fail
    """
    import time
    
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return operation()
        except exceptions as e:
            last_exception = e
            
            if attempt == max_attempts - 1:
                # Last attempt, don't sleep
                break
            
            sleep_time = delay * (backoff_factor ** attempt)
            logger.warning(f"Operation failed, retrying - attempt={attempt + 1}, max_attempts={max_attempts}, sleep_time={sleep_time}, error={str(e)}")
            time.sleep(sleep_time)
    
    # All attempts failed
    raise last_exception


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks of specified size
    
    Args:
        lst: List to split
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    chunks = []
    for i in range(0, len(lst), chunk_size):
        chunks.append(lst[i:i + chunk_size])
    return chunks


def main():
    """Main function for testing utilities"""
    # Test configuration loading
    print("Testing utility functions...")
    
    # Test date parsing
    test_dates = [
        "2023-12-01",
        "01-12-2023", 
        "01/12/2023",
        "2023-12-01T14:30:00Z"
    ]
    
    for date_str in test_dates:
        parsed_date = parse_date_string(date_str)
        parsed_datetime = parse_datetime_string(date_str)
        print(f"Date: {date_str} -> {parsed_date}, DateTime: {parsed_datetime}")
    
    # Test number formatting
    test_numbers = [1234, 1234.56, 1234567.89]
    for num in test_numbers:
        formatted = format_number(num, decimal_places=2)
        print(f"Number: {num} -> {formatted}")
    
    # Test duration formatting
    test_durations = [30, 90, 3600, 3661]
    for duration in test_durations:
        formatted = format_duration(duration)
        print(f"Duration: {duration}s -> {formatted}")
    
    print("Utility functions test completed")


if __name__ == '__main__':
    main() 