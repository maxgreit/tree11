from dotenv import load_dotenv
import requests
import json
import os


load_dotenv()

url = "https://api.gymly.io/api/v1/user/auth/login"

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json'}

data = {
    "email": os.getenv('EMAIL'),
    "password": os.getenv('PASSWORD')
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))
else:
    print(f"Fout {response.status_code}:")
    print(response.text)