from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

token = os.getenv('GYMLY_API_TOKEN')

url = "https://api.gymly.io/api/v1/businesses/df5acf01-8dfd-476b-9ba3-1d939f73fe1e/invoices"

params = {
    'page': 0, 
    'size': 100,
    'status': 'PENDING',
    'sort': '-createdAt'
}

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    
    # Analyseer de data
    total_records = data.get('totalElements', 0)
    records = data.get('content', [])
    
    print(f"Totaal aantal openstaande facturen: {total_records}")
    print(f"Aantal records in deze response: {len(records)}")
    print()
    
    # Controleer businessUser aanwezigheid
    records_with_business_user = 0
    records_without_business_user = 0
    records_with_name = 0
    
    for i, record in enumerate(records):
        business_user = record.get('businessUser')
        
        if business_user:
            records_with_business_user += 1
            full_name = business_user.get('fullName', 'Geen naam')
            if full_name and full_name != 'Geen naam':
                records_with_name += 1
                
            print(f"Record {i+1}: ✅ businessUser aanwezig - Naam: {full_name}")
        else:
            records_without_business_user += 1
            print(f"Record {i+1}: ❌ GEEN businessUser - ID: {record.get('id')}")
    
    print()
    print("=== SAMENVATTING ===")
    print(f"Records MET businessUser: {records_with_business_user}")
    print(f"Records ZONDER businessUser: {records_without_business_user}")
    print(f"Records met geldige naam: {records_with_name}")
    print(f"Percentage met businessUser: {(records_with_business_user/len(records)*100):.1f}%")
    
    # Toon een voorbeeld record
    if records:
        print()
        print("=== VOORBEELD RECORD ===")
        example = records[0]
        print(json.dumps(example, indent=2, ensure_ascii=False))
        
else:
    print(f"Fout {response.status_code}:")
    print(response.text)
