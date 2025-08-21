import requests
import json

url = "https://api.gymly.io/api/v1/user/auth/login"

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json'}

data = {
    "email": "max@greit.nl",
    "password": "RVX6dxh@hkr_thx1mcg"
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))
else:
    print(f"Fout {response.status_code}:")
    print(response.text)