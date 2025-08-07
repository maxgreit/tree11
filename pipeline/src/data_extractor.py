"""
Tree11 Data Pipeline - Data Extractor
Handles all API data extraction from Gymly API and Google Sheets
"""

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Dict, List, Optional, Any, Tuple
from datetime import timedelta, date
from urllib.parse import urlencode
import requests
import logging
import json
import time
import os

class APIError(Exception):
    """Custom exception for API errors"""
    pass


class RateLimitError(Exception):
    """Custom exception for rate limit errors"""
    pass


class GymlyAPIClient:
    """
    Gymly API Client voor Tree11 data extractie
    Handles authentication, pagination, rate limiting, and error recovery
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the API client with configuration
        
        Args:
            config: Configuration dictionary from api_endpoints.json
        """
        self.config = config
        self.base_config = config['base_config']
        self.auth_config = config['auth']
        self.endpoints = config['endpoints']
        
        # Get API token from environment
        token_env_var = self.auth_config['token_env_var']
        self.api_token = os.getenv(token_env_var)
        if not self.api_token:
            raise ValueError(f"API token not found in environment variable: {token_env_var}")
        
        # Setup session with default headers
        self.session = requests.Session()
        self.session.headers.update(self.auth_config['headers'])
        self.session.headers['Authorization'] = f"Bearer {self.api_token}"
        
        # Rate limiting
        self.last_request_time = 0
        self.request_interval = 60 / self.base_config['rate_limit_requests_per_minute']
        
        logging.info(f"Gymly API Client geïnitialiseerd")
    
    def _wait_for_rate_limit(self):
        """Enforce rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_interval:
            sleep_time = self.request_interval - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _build_url(self, endpoint_name: str, **kwargs) -> str:
        """
        Build complete URL from endpoint template
        
        Args:
            endpoint_name: Name of the endpoint from config
            **kwargs: Additional parameters for URL building
            
        Returns:
            Complete URL string
        """
        endpoint_config = self.endpoints[endpoint_name]
        url_template = endpoint_config['url_template']
        
        format_params = {}
        for key, value in {**self.base_config, **kwargs}.items():
            if isinstance(value, (str, int, float, bool)):
                format_params[key] = value
            elif isinstance(value, (list, dict)):
                # Skip lists and dicts in format_params to avoid unhashable type errors
                logging.debug(f"Non-string parameter overslaan in URL formatting: {key}={value}")
            else:
                format_params[key] = str(value)
        
        url = url_template.format(**format_params)
        
        # Add query parameters if specified
        params = {}
        if 'parameters' in endpoint_config:
            for param_name, param_value in endpoint_config['parameters'].items():
                if isinstance(param_value, str) and param_value.startswith('{'):
                    # Format parameter value
                    try:
                        formatted_value = param_value.format(**format_params)
                        params[param_name] = formatted_value
                    except (KeyError, ValueError) as e:
                        logging.warning(f"Failed to format parameter {param_name}={param_value}: {e}")
                        # Use original value if formatting fails
                        params[param_name] = param_value
                else:
                    params[param_name] = param_value
        
        # Add any additional parameters from kwargs that aren't in the config
        for key, value in kwargs.items():
            if key not in params and key not in format_params:
                params[key] = value
        
        if params:
            url += '?' + urlencode(params)
        
        return url
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, RateLimitError))
    )
    def _make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """
        Make HTTP request with error handling and retries
        
        Args:
            url: Complete URL to request
            params: Optional query parameters
            
        Returns:
            Response object
            
        Raises:
            APIError: For API-specific errors
            RateLimitError: When rate limited
        """
        self._wait_for_rate_limit()
        
        try:
            logging.debug(f"Making API request - url={url}, params={params}")
            
            response = self.session.get(
                url, 
                params=params,
                timeout=self.base_config['timeout']
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logging.warning(f"Rate limited, waiting - retry_after={retry_after}")
                time.sleep(retry_after)
                raise RateLimitError("Rate limit exceeded")
            
            # Handle client errors
            if response.status_code in [400, 401, 403, 404]:
                error_msg = f"API Error {response.status_code}: {response.text}"
                logging.error(f"API client error - status_code={response.status_code}, error={response.text}, url={url}")
                raise APIError(error_msg)
            
            # Handle server errors (will be retried)
            if response.status_code >= 500:
                error_msg = f"Server Error {response.status_code}: {response.text}"
                logging.error(f"API server error - status_code={response.status_code}, error={response.text}, url={url}")
                response.raise_for_status()
            
            response.raise_for_status()
            
            # Log successful response
            logging.debug(f"API response successful - status_code={response.status_code}")
            
            return response
            
        except requests.RequestException as e:
            logging.error(f"Request failed - error={e}, url={url}")
            raise
    
    def _extract_data_from_response(self, response: requests.Response, 
                                  endpoint_config: Dict) -> Tuple[List[Dict], Optional[Dict]]:
        """
        Extract data from API response based on endpoint configuration
        
        Args:
            response: HTTP response object
            endpoint_config: Endpoint configuration from config
            
        Returns:
            Tuple of (data_list, pagination_info)
        """
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response - error={e}")
            raise APIError(f"Invalid JSON response: {e}")
        
        response_type = endpoint_config['response_type']
        data_path = endpoint_config.get('data_path', '')
        
        # Extract data based on response type
        if response_type == 'array':
            return data, None
        elif response_type == 'paginated':
            if data_path:
                items = data.get(data_path, [])
            else:
                items = data.get('content', data.get('data', []))
            
            # Extract pagination info
            pagination_info = {
                'totalElements': data.get('totalElements', len(items)),
                'totalPages': data.get('totalPages', 1),
                'currentPage': data.get('number', 0),
                'pageSize': data.get('size', len(items))
            }
            return items, pagination_info
        elif response_type == 'object':
            return [data], None
        else:
            logging.warning(f"Unknown response type - response_type={response_type}")
            return [data], None
    
    def extract_endpoint_data(self, endpoint_name: str, 
                            start_date: Optional[date] = None,
                            end_date: Optional[date] = None,
                            **kwargs) -> List[Dict]:
        """
        Extract all data from a specific endpoint
        
        Args:
            endpoint_name: Name of endpoint from configuration
            start_date: Optional start date for date-filtered endpoints
            end_date: Optional end date for date-filtered endpoints
            **kwargs: Additional parameters
            
        Returns:
            List of all records from the endpoint
        """
        if endpoint_name not in self.endpoints:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")
        
        endpoint_config = self.endpoints[endpoint_name]
        
        logging.info(f"Data extractie beginnen voor {endpoint_name}, start datum: {start_date}, eind datum: {end_date}")
        
        all_data = []
        
        # Handle date parameters
        url_params = {}
        if start_date:
            url_params['start_date'] = start_date.isoformat()
        if end_date:
            url_params['end_date'] = end_date.isoformat()
        
        # Handle endpoint variants (for analytics endpoints)
        variants = endpoint_config.get('variants', [{}])
        
        for variant in variants:
            variant_params = {**url_params, **variant, **kwargs}
            variant_data = self._extract_variant_data(endpoint_name, variant_params)
            
            # Add variant metadata to each record
            for record in variant_data:
                record.update(variant)
                
            # Add endpoint metadata (category, etc.)
            if 'category' in endpoint_config:
                for record in variant_data:
                    record['endpoint_type'] = endpoint_name  
                    record['endpoint_category'] = endpoint_config['category']
                    
            # Add payment_type_filter from variant
            for record in variant_data:
                if 'payment_type' in variant:
                    record['payment_type_filter'] = variant['payment_type']
            
            all_data.extend(variant_data)
        
        # Log summary of all variants
        payment_types = set()
        for record in all_data:
            if 'payment_type_filter' in record:
                payment_types.add(record['payment_type_filter'])
        
        logging.info(f"Data extractie afgerond")
        logging.debug(f"Betaal types gevonden: {payment_types}")
        
        return all_data
    
    def _extract_variant_data(self, endpoint_name: str, params: Dict) -> List[Dict]:
        """Extract data for a specific variant of an endpoint"""
        endpoint_config = self.endpoints[endpoint_name]
        all_data = []
        
        # Handle pagination
        pagination_config = endpoint_config.get('pagination', {})
        pagination_type = pagination_config.get('type', 'none')
        
        if pagination_type == 'none':
            # Single request
            url = self._build_url(endpoint_name, **params)
            
            # Log the full URL and parameters for debugging
            payment_type = params.get('payment_type', 'UNKNOWN')
            
            response = self._make_request(url)
            data, _ = self._extract_data_from_response(response, endpoint_config)
            
            all_data.extend(data)
            
        elif pagination_type == 'page_based':
            # Page-based pagination
            page = 1
            page_size = pagination_config.get('default_size', 100)
            
            while True:
                page_params = {
                    pagination_config['page_param']: page,
                    pagination_config['size_param']: page_size
                }
                
                url = self._build_url(endpoint_name, **params)
                
                # Log the full URL and parameters for debugging (first page only)
                if page == 1:
                    payment_type = params.get('payment_type', 'UNKNOWN')
                
                response = self._make_request(url, page_params)
                data, pagination_info = self._extract_data_from_response(response, endpoint_config)
                
                if not data:
                    break
                
                all_data.extend(data)
                
                # Log page results
                logging.info(f"Opgehaalde rijen voor {endpoint_name}: {len(all_data)} (pagina {page})")
                
                # Check if we have more pages
                if pagination_info:
                    current_page = pagination_info.get('currentPage', page)
                    total_pages = pagination_info.get('totalPages', 1)
                    
                    if current_page >= total_pages - 1:
                        break
                
                page += 1
                
                # Safety check to prevent infinite loops
                if page > 1000:
                    logging.error(f"Te veel pagina's, stoppen met pagineren")
                    break
        
        return all_data
    
    def get_date_range_for_endpoint(self, endpoint_name: str) -> Tuple[date, date]:
        """
        Get appropriate date range for an endpoint based on its configuration
        
        Args:
            endpoint_name: Name of the endpoint
            
        Returns:
            Tuple of (start_date, end_date)
        """
        endpoint_config = self.endpoints[endpoint_name]
        date_range_config = endpoint_config.get('date_range', {})
        
        if not date_range_config:
            # Default to yesterday if no config
            yesterday = date.today() - timedelta(days=1)
            return yesterday, yesterday
        
        range_type = date_range_config.get('type', 'daily')
        today = date.today()
        
        if range_type == 'daily':
            days_back = date_range_config.get('days_back', 1)
            days_forward = date_range_config.get('days_forward', 0)
            start_date = today - timedelta(days=days_back)
            end_date = today + timedelta(days=days_forward)
            
        elif range_type == 'monthly':
            months_back = date_range_config.get('months_back', 1)
            start_date = today.replace(day=1) - timedelta(days=months_back * 30)
            end_date = today
            
        else:
            # Default fallback
            start_date = today - timedelta(days=1)
            end_date = today
        
        return start_date, end_date


class GoogleSheetsExtractor:
    """
    Google Sheets data extractor for PersonalTraining data
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Google Sheets client"""
        self.config = config
        self.sheets_config = config.get('google_sheets', {})
        
        # TODO: Implement Google Sheets authentication
        logging.info("Google Sheets extractor geïnitialiseerd")
    
    def extract_personal_training_data(self) -> List[Dict]:
        """
        Extract Personal Training data from Google Sheets
        
        Returns:
            List of personal training records
        """
        # TODO: Implement Google Sheets data extraction
        logging.warning("Google Sheets extractie niet geïmplementeerd")
        return []


class DataExtractor:
    """
    Main data extractor class that coordinates API and Google Sheets extraction
    """
    
    def __init__(self, config_path: str):
        """
        Initialize data extractor
        
        Args:
            config_path: Path to API endpoints configuration file
        """
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        self.api_client = GymlyAPIClient(config)
        self.sheets_extractor = GoogleSheetsExtractor(config.get('google_sheets', {}))
        
        logging.info("Data extractor geïnitialiseerd")
    
    def extract_endpoint_data(self, endpoint_name: str, 
                            historical: bool = False, 
                            start_date: str = None, 
                            end_date: str = None) -> List[Dict]:
        """
        Extract data from a specific endpoint
        
        Args:
            endpoint_name: Name of the endpoint to extract from
            historical: If True, use provided start_date and end_date instead of default date range
            start_date: Start date for historical data (YYYY-MM-DD format)
            end_date: End date for historical data (YYYY-MM-DD format)
            
        Returns:
            List of extracted records
        """
        logging.info(f"Beginnen met data extractie van endpoint: {endpoint_name}, historisch: {historical}")
        
        if historical:
            if not start_date or not end_date:
                raise ValueError("Historical mode requires both start_date and end_date")
            
            # Convert string dates to date objects
            from datetime import datetime
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            logging.info(f"Historische data extractie: {start_date} tot {end_date}")
            
            # Extract historical data using provided dates
            return self.api_client.extract_endpoint_data(
                endpoint_name,
                start_date=start_dt,
                end_date=end_dt
            )
        else:
            # Use default date range for daily data
            logging.info("Gebruik standaard datum bereik voor dagelijkse data extractie")
            return self.api_client.extract_endpoint_data(endpoint_name)
    
    def extract_all_data(self, specific_tables: Optional[List[str]] = None) -> Dict[str, List[Dict]]:
        """
        Extract data for all tables or specific tables
        
        Args:
            specific_tables: Optional list of specific tables to extract
            
        Returns:
            Dictionary mapping table names to extracted data
        """
        logging.info(f"Extraheren van alle data voor: {specific_tables}")
        
        all_data = {}
        
        # Define table to endpoint mappings
        table_endpoints = {
            'Leden': 'users',
            'Abonnementen': 'memberships',
            'Lessen': 'activity_events',
            'LesDeelname': 'courses_members',
            'Omzet': 'analytics_revenue',
            'GrootboekRekening': 'analytics_revenue',
            'AbonnementStatistieken': [
                'analytics_memberships_new',
                'analytics_memberships_paused', 
                'analytics_memberships_active',
                'analytics_memberships_expired'
            ],
            'OpenstaandeFacturen': 'invoices_pending',
            'PersonalTraining': 'google_sheets'
        }
        
        # Filter to specific tables if requested
        if specific_tables:
            table_endpoints = {k: v for k, v in table_endpoints.items() if k in specific_tables}
        
        for table_name, endpoint_name in table_endpoints.items():
            try:
                logging.info(f"Extraheren van data voor tabel: {table_name}")
                
                if endpoint_name == 'google_sheets':
                    # Google Sheets extraction
                    data = self.sheets_extractor.extract_personal_training_data()
                elif table_name == 'LesDeelname':
                    # Special handling for LesDeelname - requires course IDs from past lessons
                    data = self._extract_les_deelname_data()
                elif isinstance(endpoint_name, list):
                    # Multiple endpoints (like AbonnementStatistieken)
                    data = []
                    for endpoint in endpoint_name:
                        endpoint_data = self.api_client.extract_endpoint_data(endpoint)
                        data.extend(endpoint_data)
                        logging.debug(f"Gegevens opgehaald voor: {endpoint}, aantal rijen: {len(endpoint_data)}")
                else:
                    # Single API endpoint
                    # Check if endpoint requires date parameters
                    endpoint_config = self.api_client.endpoints[endpoint_name]
                    if 'parameters' in endpoint_config and any('date' in param.lower() for param in endpoint_config['parameters']):
                        # Get date range for this endpoint
                        start_date, end_date = self.api_client.get_date_range_for_endpoint(endpoint_name)
                        data = self.api_client.extract_endpoint_data(endpoint_name, start_date=start_date, end_date=end_date)
                    else:
                        # No date parameters required
                        data = self.api_client.extract_endpoint_data(endpoint_name)
                
                all_data[table_name] = data
                logging.info(f"Gegevens opgehaald voor tabel: {table_name}, aantal rijen: {len(data)}")
                
            except Exception as e:
                logging.error(f"Fout bij extraheren van data voor tabel: {table_name}, fout: {str(e)}")
                all_data[table_name] = []
        
        return all_data
    
    def _extract_les_deelname_data(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[Dict]:
        """
        Speciale extractie voor LesDeelname - haalt eerst Lessen op en dan voor elke les de members
        
        Args:
            start_date: Optionele startdatum voor historische data
            end_date: Optionele einddatum voor historische data
            
        Returns:
            Lijst met alle lesdeelname records
        """
        logging.info("Speciale extractie voor LesDeelname - haalt course IDs op van Lessen")
        
        try:
            # Eerst Lessen ophalen om course IDs te krijgen
            if start_date and end_date:
                logging.info(f"Historische LesDeelname extractie: {start_date} tot {end_date}")
                lessen_data = self.api_client.extract_endpoint_data('activity_events', start_date=start_date, end_date=end_date)
            else:
                # Voor dagelijkse extractie: afgelopen week (vanaf gisteren)
                from datetime import datetime, timedelta
                yesterday = datetime.now().date() - timedelta(days=1)
                week_ago = yesterday - timedelta(days=7)
                
                logging.info(f"LesDeelname dagelijkse extractie: {week_ago} tot {yesterday}")
                lessen_data = self.api_client.extract_endpoint_data('activity_events', start_date=week_ago, end_date=yesterday)
            
            if not lessen_data:
                logging.warning("Geen lessen gevonden voor LesDeelname extractie")
                return []
            
            # Filter lessen die in het verleden liggen (niet vandaag of in de toekomst)
            from datetime import datetime
            today = datetime.now().date()
            past_lessons = []
            
            for les in lessen_data:
                try:
                    # Parse starttijd van de les
                    start_tijd_str = les.get('startAt')
                    if start_tijd_str:
                        if isinstance(start_tijd_str, str):
                            # Parse ISO datetime string
                            start_tijd = datetime.fromisoformat(start_tijd_str.replace('Z', '+00:00'))
                        else:
                            start_tijd = start_tijd_str
                        
                        # Check of les in het verleden ligt
                        if start_tijd.date() < today:
                            past_lessons.append(les)
                except Exception as e:
                    logging.warning(f"Fout bij parsen van les datum: {e}")
                    continue
            
            logging.info(f"Gevonden {len(past_lessons)} lessen in het verleden voor LesDeelname extractie")
            
            # Voor elke les in het verleden, haal de members op
            all_les_deelname = []
            
            for les in past_lessons:
                course_id = les.get('id')
                if not course_id:
                    logging.warning(f"Geen course ID gevonden voor les: {les}")
                    continue
                
                try:
                    logging.info(f"Haalt members op voor course: {course_id}")
                    
                    # Haal members op voor deze course
                    members_data = self.api_client.extract_endpoint_data('courses_members', course_id=course_id)
                    
                    # Voeg course_id toe aan elke member record
                    for member in members_data:
                        member['course_id'] = course_id
                    
                    all_les_deelname.extend(members_data)
                    logging.info(f"{len(members_data)} members gevonden voor course {course_id}")
                    
                except Exception as e:
                    logging.error(f"Fout bij ophalen members voor course {course_id}: {e}")
                    continue
            
            logging.info(f"Totaal {len(all_les_deelname)} lesdeelname records geëxtraheerd")
            return all_les_deelname
            
        except Exception as e:
            logging.error(f"Fout bij LesDeelname extractie: {e}")
            raise