from dotenv import load_dotenv
import requests
import json
import os
from datetime import date, timedelta

load_dotenv()

token = os.getenv('GYMLY_API_TOKEN')

today = date.today()
days_back = 1
days_forward = 7
business_id = "df5acf01-8dfd-476b-9ba3-1d939f73fe1e"
location_id = "759cf904-4133-4fd8-af4c-ded2cedb6192"

start_date = today - timedelta(days=days_back)
end_date = today + timedelta(days=days_forward)

url = f"https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/activity-events?startDate={start_date}&endDate={end_date}&locations={location_id}"

params = {'page': 1, 'size': 100}


headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))
else:
    print(f"Fout {response.status_code}:")
    print(response.text)
    
    