from dotenv import load_dotenv
import pandas as pd
import requests
import json
import os
from collections import defaultdict

load_dotenv()

token = os.getenv('token')

def analyze_json_structure(obj, prefix="", max_depth=5, current_depth=0):
    """Recursief analyseren van JSON structuur"""
    structure = {}
    
    if current_depth >= max_depth:
        return {prefix: "... (max depth reached)"}
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            field_path = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                structure.update(analyze_json_structure(value, field_path, max_depth, current_depth + 1))
            elif isinstance(value, list):
                if value:  # Als lijst niet leeg is
                    structure[field_path] = f"array[{type(value[0]).__name__}]"
                    if isinstance(value[0], (dict, list)):
                        structure.update(analyze_json_structure(value[0], f"{field_path}[0]", max_depth, current_depth + 1))
                else:
                    structure[field_path] = "array[empty]"
            else:
                structure[field_path] = type(value).__name__
    
    elif isinstance(obj, list):
        if obj:
            structure[prefix] = f"array[{type(obj[0]).__name__}]"
            if isinstance(obj[0], (dict, list)):
                structure.update(analyze_json_structure(obj[0], f"{prefix}[0]", max_depth, current_depth + 1))
        else:
            structure[prefix] = "array[empty]"
    else:
        structure[prefix] = type(obj).__name__
    
    return structure

def create_nested_json_schema(flat_schema):
    """Convert flat schema to nested JSON structure"""
    nested = {}
    
    for field_path, field_type in flat_schema.items():
        # Split het pad op punten
        parts = field_path.split('.')
        current = nested
        
        # Navigeer door de structuur
        for i, part in enumerate(parts[:-1]):
            # Handle array indices like [0]
            if '[0]' in part:
                clean_part = part.replace('[0]', '')
                if clean_part not in current:
                    current[clean_part] = {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {}
                        }
                    }
                # Zorg ervoor dat items altijd de juiste structuur heeft
                if "properties" not in current[clean_part]["items"]:
                    current[clean_part]["items"]["properties"] = {}
                current = current[clean_part]["items"]["properties"]
            else:
                if part not in current:
                    current[part] = {
                        "type": "object", 
                        "properties": {}
                    }
                current = current[part]["properties"]
        
        # Laatste deel van het pad
        final_part = parts[-1]
        if '[0]' in final_part:
            clean_final = final_part.replace('[0]', '')
            # Voor array fields die eindigen met [0]
            current[clean_final] = {
                "type": "array", 
                "items": {"type": field_type}
            }
        else:
            # Voor directe array fields of primitive types
            if field_type.startswith('array['):
                item_type = field_type[6:-1]  # Remove 'array[' and ']'
                if item_type == 'empty':
                    current[final_part] = {
                        "type": "array",
                        "items": {"type": "unknown"}
                    }
                elif item_type == 'dict':
                    current[final_part] = {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {}
                        }
                    }
                else:
                    current[final_part] = {
                        "type": "array",
                        "items": {"type": item_type}
                    }
            else:
                current[final_part] = {"type": field_type}
    
    return nested

def discover_endpoint_schema(url, headers=None):
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        
        # Check wat voor response type we hebben
        if isinstance(data, dict) and 'content' in data and data['content']:
            # GEPAGINEERDE RESPONSE (zoals users)
            all_fields = {}
            
            # Neem eerste paar records voor schema analyse
            sample_records = data['content'][:3]  # Eerste 3 records
            
            for record in sample_records:
                record_structure = analyze_json_structure(record)
                all_fields.update(record_structure)
            
            return {
                'endpoint': url,
                'status': response.status_code,
                'response_type': 'paginated',
                'flat_schema': all_fields,
                'nested_schema': create_nested_json_schema(all_fields),
                'sample_record': data['content'][0],
                'record_count': data.get('totalElements', len(data['content'])),
                'pagination_info': {
                    'totalElements': data.get('totalElements'),
                    'totalPages': data.get('totalPages'),
                    'pageSize': data.get('size'),
                    'currentPage': data.get('number')
                }
            }
            
        elif isinstance(data, list):
            # DIRECTE ARRAY RESPONSE (zoals jouw nieuwe voorbeeld)
            all_fields = {}
            
            # Analyseer eerste paar items in de array
            sample_records = data[:3] if len(data) > 3 else data
            
            for record in sample_records:
                record_structure = analyze_json_structure(record)
                all_fields.update(record_structure)
            
            return {
                'endpoint': url,
                'status': response.status_code,
                'response_type': 'array',
                'flat_schema': all_fields,
                'nested_schema': create_nested_json_schema(all_fields),
                'sample_record': data[0] if data else None,
                'record_count': len(data),
                'pagination_info': None
            }
            
        elif isinstance(data, dict):
            # SINGLE OBJECT RESPONSE
            flat_schema = analyze_json_structure(data)
            return {
                'endpoint': url,
                'status': response.status_code,
                'response_type': 'object',
                'flat_schema': flat_schema,
                'nested_schema': create_nested_json_schema(flat_schema),
                'sample_record': data,
                'record_count': 1,
                'pagination_info': None
            }
        else:
            # ANDERE RESPONSE TYPES
            return {
                'endpoint': url,
                'status': response.status_code,
                'response_type': 'other',
                'flat_schema': {f"root_{type(data).__name__}": type(data).__name__},
                'nested_schema': {f"root_{type(data).__name__}": {"type": type(data).__name__}},
                'sample_record': data,
                'record_count': 1,
                'pagination_info': None
            }
            
    except Exception as e:
        return {'endpoint': url, 'error': str(e)}

def extract_endpoint_name(url):
    """Extract a meaningful endpoint name from URL"""
    # Remove base URL parts and get the meaningful part
    parts = url.split('/')
    
    # Find the meaningful parts (skip common parts like 'api', 'v1', etc.)
    meaningful_parts = []
    skip_parts = {'api', 'v1', 'v2', 'user', 'businesses', 'https:', '', 'http:'}
    
    for part in parts:
        # Skip UUIDs (long hex strings with dashes)
        if len(part) == 36 and part.count('-') == 4:
            continue
        # Skip common API parts
        if part.lower() in skip_parts:
            continue
        # Skip empty parts
        if not part:
            continue
        
        meaningful_parts.append(part)
    
    # Join the meaningful parts
    if meaningful_parts:
        endpoint_name = '_'.join(meaningful_parts[-2:])  # Take last 2 parts max
        # Clean up the name
        endpoint_name = endpoint_name.replace('-', '_')
        return endpoint_name
    else:
        # Fallback to last part of URL
        return parts[-1] if parts[-1] else 'unknown_endpoint'

def create_complete_catalog(urls_list, headers):
    """Maak een complete catalogus van alle URLs"""
    catalog = {
        "api_info": {
            "generated_at": "2025-07-16",
            "total_urls": len(urls_list)
        },
        "endpoints": {}
    }
    
    for url in urls_list:
        # Extract endpoint name from URL for naming
        endpoint_name = extract_endpoint_name(url)
        print(f"Analyzing endpoint: {endpoint_name} ({url})...")
        
        result = discover_endpoint_schema(url, headers)
        
        if 'error' not in result:
            catalog["endpoints"][endpoint_name] = {
                "url": url,
                "response_type": result.get('response_type', 'unknown'),
                "record_count": result['record_count'],
                "status": result['status'],
                "flat_schema": result['flat_schema'],
                "nested_schema": result['nested_schema'],
                "pagination_info": result.get('pagination_info')
            }
            response_type = result.get('response_type', 'unknown')
            print(f"  ✓ Success: {result['record_count']} records found ({response_type} format)")
        else:
            catalog["endpoints"][endpoint_name] = {"error": result['error'], "url": url}
            print(f"  ✗ Error: {result['error']}")
    
    return catalog

def save_catalog_to_files(catalog, base_filename="api_catalog"):
    """Save catalog to multiple formats"""
    
    # JSON format
    with open(f"{base_filename}.json", 'w') as f:
        json.dump(catalog, f, indent=2)
    
    # Pretty text format
    with open(f"{base_filename}_summary.txt", 'w') as f:
        f.write("API CATALOG SUMMARY\n")
        f.write("==================\n\n")
        f.write(f"Generated: {catalog['api_info']['generated_at']}\n")
        f.write(f"Total URLs: {catalog['api_info']['total_urls']}\n\n")
        
        for endpoint, info in catalog['endpoints'].items():
            if 'error' not in info:
                f.write(f"\n--- {endpoint.upper()} ---\n")
                f.write(f"Records: {info['record_count']}\n")
                f.write(f"URL: {info['url']}\n")
                f.write(f"Response Type: {info['response_type']}\n")
                f.write("Schema Structure:\n")
                
                # Write flat schema
                for field, field_type in sorted(info['flat_schema'].items()):
                    f.write(f"  ├── {field}: {field_type}\n")
            else:
                f.write(f"\n--- {endpoint.upper()} --- ERROR: {info['error']}\n")
                f.write(f"URL: {info['url']}\n")

# Headers
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# GEBRUIK: Met volledige URL lijst
if __name__ == "__main__":
    
    # Definieer je volledige URLs hier
    urls = [
        "https://api.gymly.io/api/v2/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/users",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/rooms",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/memberships",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/data-views/members",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/locations",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/lead-stages",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/webshop/items?page=0&size=100",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/invoices?page=0&size=100&status=PENDING"
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/invoices?page=0&size=100", #??
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/course-types?page=0&size=100",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/webshop/categories?page=0&size=100",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/calendars",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/leads/assignees",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/activity-events?startDate=2025-07-16&endDate=2025-07-17&locations=759cf904-4133-4fd8-af4c-ded2cedb6192",
        "https://api.gymly.io/api/v2/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/leads?size=15&page=0",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/analytics/revenue?startDate=2025-07-01&endDate=2025-07-31&label=periodSelect.thisMonth&locations=759cf904-4133-4fd8-af4c-ded2cedb6192",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/analytics/memberships/new?period.startDate=2025-07-01&period.endDate=2025-07-31&period.granularity=WEEK&filter.LOCATION=759cf904-4133-4fd8-af4c-ded2cedb6192",
        "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/analytics/memberships/paused?period.startDate=2025-07-01&period.endDate=2025-07-31&period.granularity=WEEK&filter.LOCATION=759cf904-4133-4fd8-af4c-ded2cedb6192"
    ]
    
    # Analyseer alle URLs
    catalog = create_complete_catalog(urls, headers)
    save_catalog_to_files(catalog)
    print(f"\n✓ Complete catalog saved to files")
    
    # Print summary
    print(f"\n=== ANALYSIS SUMMARY ===")
    for endpoint_name, info in catalog['endpoints'].items():
        if 'error' not in info:
            print(f"{endpoint_name}: {info['record_count']} records ({info['response_type']})")
        else:
            print(f"{endpoint_name}: ERROR - {info['error']}")

    """
    # Voorbeeld: Als je een mix wilt van verschillende API's
    mixed_urls = [
        "https://api.gymly.io/api/v1/user/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/users",
        "https://api.gymly.io/api/v1/different/path/endpoint",
        "https://completely-different-api.com/v2/data",
        "https://another-service.com/api/customers"
    ]
    
    catalog = create_complete_catalog(mixed_urls, headers)
    """