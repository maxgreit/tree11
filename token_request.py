import requests
import json

url = "https://api.gymly.io/api/v1/user/auth/login"
payload = {
    "email": "max@greit.nl",
    "password": "RVX6dxh@hkr_thx1mcg"
}
headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

# Check of de login gelukt is
if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))
    token = data.get("token") or data.get("access_token")
    print("Login gelukt. Token:", token)
    print(token)
else:
    print("Login mislukt:", response.status_code)
    print(response.text)