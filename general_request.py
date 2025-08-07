from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

token = os.getenv('token')

url = "https://api.gymly.io/api/v1/courses/5f351c25-27f7-486f-8f7b-29e0af1d5ece/members"

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
    
    