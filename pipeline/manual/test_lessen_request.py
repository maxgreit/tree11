from dotenv import load_dotenv
import requests
import json
import os
from datetime import date, timedelta

load_dotenv()

token = os.getenv('GYMLY_API_TOKEN')

# Base config values
base_url = "https://api.gymly.io/api"
business_id = "df5acf01-8dfd-476b-9ba3-1d939f73fe1e"
location_id = "759cf904-4133-4fd8-af4c-ded2cedb6192"

# Calculate date range (same logic as pipeline)
today = date.today()
days_back = 1
days_forward = 7

start_date = today - timedelta(days=days_back)
end_date = today + timedelta(days=days_forward)

print("=== LESSEN API REQUEST ANALYSE ===")
print()

# 1. URL Template
url_template = "{base_url}/v1/businesses/{business_id}/activity-events"
print("1. URL Template:")
print(f"   {url_template}")
print()

# 2. Filled URL
filled_url = f"{base_url}/v1/businesses/{business_id}/activity-events"
print("2. Filled URL:")
print(f"   {filled_url}")
print()

# 3. Date Range
print("3. Date Range:")
print(f"   Vandaag: {today}")
print(f"   Start datum: {start_date} (gisteren)")
print(f"   Eind datum: {end_date} (over {days_forward} dagen)")
print(f"   Totaal bereik: {days_back + days_forward + 1} dagen")
print()

# 4. Query Parameters
print("4. Query Parameters:")
print(f"   startDate: {start_date}")
print(f"   endDate: {end_date}")
print(f"   locations: {location_id}")
print()

# 5. Complete GET Request
print("5. Complete GET Request:")
print(f"   URL: {filled_url}")
print(f"   Method: GET")
print(f"   Headers: Authorization: Bearer {token[:20]}...")
print(f"   Query params: startDate={start_date}&endDate={end_date}&locations={location_id}")
print()

# 6. Full URL with parameters
full_url = f"{filled_url}?startDate={start_date}&endDate={end_date}&locations={location_id}"
print("6. Full URL with Parameters:")
print(f"   {full_url}")
print()

# 7. Make actual request
print("7. Making Actual Request...")
print()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

params = {
    'startDate': str(start_date),
    'endDate': str(end_date),
    'locations': location_id
}

try:
    response = requests.get(filled_url, headers=headers, params=params, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        
        print("✅ Request successful!")
        print(f"   Status: {response.status_code}")
        print(f"   Response size: {len(str(data))} characters")
        
        if isinstance(data, list):
            print(f"   Number of lessons: {len(data)}")
            
            if data:
                print()
                print("8. Sample Lesson Data:")
                sample_lesson = data[0]
                print(json.dumps(sample_lesson, indent=2, ensure_ascii=False))
        else:
            print(f"   Response type: {type(data)}")
            print(f"   Response keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
            
    else:
        print(f"❌ Request failed!")
        print(f"   Status: {response.status_code}")
        print(f"   Error: {response.text}")
        
except Exception as e:
    print(f"❌ Request error: {e}")

print()
print("=== EINDE ANALYSE ===")
