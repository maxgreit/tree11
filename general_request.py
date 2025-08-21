from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

token = os.getenv('token')


url = "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/pos/statistics?startDate=2025-08-14&endDate=2025-08-22"

params = {'page': 1, 'size': 10}


headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))
else:
    print(f"Fout {response.status_code}:")
    print(response.text)
    
    