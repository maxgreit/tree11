"""
Tree11 Data Pipeline - Google Sheets Manager
Handles Google Sheets integration for Personal Training data extraction
"""

import json
import logging
import os
from datetime import datetime, date
from typing import Dict, List, Any, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets errors"""
    pass


class GoogleSheetsManager:
    """
    Google Sheets Manager for Tree11 Personal Training data
    Handles authentication and data extraction from Google Sheets
    """
    
    # Google Sheets API scope
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Google Sheets manager
        
        Args:
            config: Configuration dictionary from api_endpoints.json
        """
        self.config = config
        self.sheets_config = config.get('google_sheets', {})
        self.personal_training_config = self.sheets_config.get('personal_training', {})
        
        self.service = None
        self._initialize_service()
        
        logger.info("Google Sheets manager initialized")
    
    def _initialize_service(self):
        """Initialize Google Sheets API service"""
        try:
            credentials = self._get_credentials()
            self.service = build('sheets', 'v4', credentials=credentials)
            
            logger.info("Google Sheets API service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service - error={str(e)}")
            raise GoogleSheetsError(f"Google Sheets initialization failed: {str(e)}")
    
    def _get_credentials(self) -> Credentials:
        """
        Get Google API credentials
        
        Returns:
            Google API credentials object
        """
        credentials_file = self.personal_training_config.get('credentials_file')
        
        if not credentials_file:
            raise GoogleSheetsError("No credentials file specified in configuration")
        
        if not os.path.exists(credentials_file):
            raise GoogleSheetsError(f"Credentials file not found: {credentials_file}")
        
        try:
            # Try service account credentials first
            credentials = ServiceAccountCredentials.from_service_account_file(
                credentials_file, scopes=self.SCOPES
            )
            logger.info("Using service account credentials")
            return credentials
            
        except Exception as service_error:
            logger.warning(f"Service account credentials failed, trying OAuth flow - error={str(service_error)}")
            
            try:
                # Fallback to OAuth flow
                return self._oauth_flow(credentials_file)
                
            except Exception as oauth_error:
                logger.error(f"Both authentication methods failed - service_error={str(service_error)}, oauth_error={str(oauth_error)}")
                raise GoogleSheetsError("Failed to authenticate with Google Sheets API")
    
    def _oauth_flow(self, credentials_file: str) -> Credentials:
        """
        Perform OAuth flow for user credentials
        
        Args:
            credentials_file: Path to OAuth credentials file
            
        Returns:
            User credentials
        """
        creds = None
        token_file = 'token.json'
        
        # Load existing token if available
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, self.SCOPES)
        
        # If no valid credentials, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next run
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        
        return creds
    
    def extract_personal_training_data(self) -> List[Dict]:
        """
        Extract Personal Training data from Google Sheets
        
        Returns:
            List of personal training records
        """
        if not self.service:
            raise GoogleSheetsError("Google Sheets service not initialized")
        
        spreadsheet_id = self.personal_training_config.get('spreadsheet_id')
        range_name = self.personal_training_config.get('range', 'A:D')
        
        if not spreadsheet_id:
            raise GoogleSheetsError("No spreadsheet ID specified in configuration")
        
        logger.info(f"Extracting Personal Training data - spreadsheet_id={spreadsheet_id}, range={range_name}")
        
        try:
            # Call the Sheets API
            sheet = self.service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                logger.warning("No data found in Google Sheet")
                return []
            
            # Process the data
            records = self._process_sheet_data(values)
            
            logger.info(f"Personal Training data extracted successfully - raw_rows={len(values)}, processed_records={len(records)}")
            
            return records
            
        except HttpError as e:
            logger.error(f"Google Sheets API error - error={str(e)}")
            raise GoogleSheetsError(f"Failed to read Google Sheet: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error reading Google Sheet - error={str(e)}")
            raise GoogleSheetsError(f"Unexpected error: {str(e)}")
    
    def _process_sheet_data(self, values: List[List[str]]) -> List[Dict]:
        """
        Process raw sheet data into structured records
        
        Args:
            values: Raw values from Google Sheets
            
        Returns:
            List of processed records
        """
        if not values:
            return []
        
        # Assume first row contains headers
        headers = values[0] if values else []
        data_rows = values[1:] if len(values) > 1 else []
        
        # Expected headers: Voornaam, Achternaam, Datum, Uren
        expected_headers = ['Voornaam', 'Achternaam', 'Datum', 'Uren']
        
        # Map headers to expected format
        header_mapping = self._map_headers(headers, expected_headers)
        
        records = []
        for row_index, row in enumerate(data_rows, start=2):  # Start at 2 (row 1 is headers)
            try:
                record = self._process_row(row, header_mapping, row_index)
                if record:
                    records.append(record)
            except Exception as e:
                logger.warning(f"Failed to process row - row_index={row_index}, row_data={row}, error={str(e)}")
                continue
        
        return records
    
    def _map_headers(self, actual_headers: List[str], 
                    expected_headers: List[str]) -> Dict[str, int]:
        """
        Map actual sheet headers to expected column positions
        
        Args:
            actual_headers: Headers from the sheet
            expected_headers: Expected header names
            
        Returns:
            Dictionary mapping expected header names to column indices
        """
        header_mapping = {}
        
        for expected in expected_headers:
            # Try exact match first
            for i, actual in enumerate(actual_headers):
                if actual.strip().lower() == expected.lower():
                    header_mapping[expected] = i
                    break
            
            # Try partial match if exact match not found
            if expected not in header_mapping:
                for i, actual in enumerate(actual_headers):
                    if expected.lower() in actual.strip().lower():
                        header_mapping[expected] = i
                        break
        
        logger.debug(f"Header mapping created - mapping={header_mapping}")
        return header_mapping
    
    def _process_row(self, row: List[str], header_mapping: Dict[str, int], 
                    row_index: int) -> Optional[Dict]:
        """
        Process a single row of data
        
        Args:
            row: Row data from sheet
            header_mapping: Mapping of headers to indices
            row_index: Row number for error reporting
            
        Returns:
            Processed record or None if invalid
        """
        # Skip empty rows
        if not row or all(cell.strip() == '' for cell in row):
            return None
        
        record = {}
        
        # Extract Voornaam
        voornaam_idx = header_mapping.get('Voornaam')
        if voornaam_idx is not None and voornaam_idx < len(row):
            record['Voornaam'] = row[voornaam_idx].strip()
        else:
            record['Voornaam'] = ''
        
        # Extract Achternaam  
        achternaam_idx = header_mapping.get('Achternaam')
        if achternaam_idx is not None and achternaam_idx < len(row):
            record['Achternaam'] = row[achternaam_idx].strip()
        else:
            record['Achternaam'] = ''
        
        # Extract and parse Datum
        datum_idx = header_mapping.get('Datum')
        if datum_idx is not None and datum_idx < len(row):
            datum_str = row[datum_idx].strip()
            record['Datum'] = self._parse_date(datum_str, row_index)
        else:
            record['Datum'] = None
        
        # Extract and parse Uren
        uren_idx = header_mapping.get('Uren')
        if uren_idx is not None and uren_idx < len(row):
            uren_str = row[uren_idx].strip()
            record['Uren'] = self._parse_hours(uren_str, row_index)
        else:
            record['Uren'] = 0
        
        # Validate required fields
        if not record['Voornaam'] and not record['Achternaam']:
            logger.debug(f"Skipping row with no name - row_index={row_index}")
            return None
        
        if not record['Datum']:
            logger.warning(f"Row missing valid date - row_index={row_index}")
            return None
        
        return record
    
    def _parse_date(self, date_str: str, row_index: int) -> Optional[date]:
        """
        Parse date string to date object
        
        Args:
            date_str: Date string from sheet
            row_index: Row number for error reporting
            
        Returns:
            Parsed date or None if invalid
        """
        if not date_str:
            return None
        
        # Try different date formats
        date_formats = [
            '%d-%m-%Y',     # 01-12-2023
            '%d/%m/%Y',     # 01/12/2023
            '%Y-%m-%d',     # 2023-12-01
            '%d-%m-%y',     # 01-12-23
            '%d/%m/%y',     # 01/12/23
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date - date_str={date_str}, row_index={row_index}")
        return None
    
    def _parse_hours(self, hours_str: str, row_index: int) -> int:
        """
        Parse hours string to integer
        
        Args:
            hours_str: Hours string from sheet
            row_index: Row number for error reporting
            
        Returns:
            Parsed hours as integer
        """
        if not hours_str:
            return 0
        
        try:
            # Remove any non-numeric characters except decimal point
            cleaned = ''.join(c for c in hours_str if c.isdigit() or c == '.')
            
            if not cleaned:
                return 0
            
            # Convert to float first, then to int
            hours = float(cleaned)
            return int(hours)
            
        except (ValueError, TypeError):
            logger.warning(f"Could not parse hours - hours_str={hours_str}, row_index={row_index}")
            return 0
    
    def test_connection(self) -> bool:
        """
        Test Google Sheets connection
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if not self.service:
                return False
            
            spreadsheet_id = self.personal_training_config.get('spreadsheet_id')
            if not spreadsheet_id:
                return False
            
            # Try to get spreadsheet metadata
            sheet = self.service.spreadsheets()
            result = sheet.get(spreadsheetId=spreadsheet_id).execute()
            
            title = result.get('properties', {}).get('title', 'Unknown')
            logger.info(f"Google Sheets connection test successful - spreadsheet_title={title}")
            
            return True
            
        except Exception as e:
            logger.error("Google Sheets connection test failed - error=%s", str(e))
            return False


def main():
    """Main function for testing"""
    import sys
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Sample configuration for testing
    test_config = {
        "google_sheets": {
            "personal_training": {
                "spreadsheet_id": "1NENouGIUVqXp4lowiMHIm3oqmcFj3cS1",
                "sheet_gid": "365999809",
                "range": "A:D",
                "credentials_file": "google_sheets_credentials.json"
            }
        }
    }
    
    try:
        sheets_manager = GoogleSheetsManager(test_config)
        
        # Test connection
        if sheets_manager.test_connection():
            print("✓ Google Sheets connection successful")
            
            # Extract data
            data = sheets_manager.extract_personal_training_data()
            print(f"✓ Extracted {len(data)} Personal Training records")
            
            # Show sample records
            for i, record in enumerate(data[:3]):
                print(f"Record {i+1}: {record}")
        else:
            print("✗ Google Sheets connection failed")
            
    except Exception as e:
        logger.error(f"Google Sheets manager test failed - error={str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main() 