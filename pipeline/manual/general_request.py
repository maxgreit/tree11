from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

token = os.getenv('GYMLY_API_TOKEN')


url = "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/users"

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
    
    