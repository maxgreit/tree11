from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

# Definieer welke velden je wilt
gewenste_velden = ["id", "name", "description", "paymentType"]

token = os.getenv('token')
url = "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/memberships"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    
    # Filter dynamisch op basis van gewenste_velden
    filtered_data = []
    for item in data:
        filtered_item = {veld: item.get(veld) for veld in gewenste_velden}
        filtered_data.append(filtered_item)
    
    print(json.dumps(filtered_data, indent=4))
else:
    print(f"Fout {response.status_code}:")
    print(response.text)