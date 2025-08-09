import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def get_service(creds_file: str):
    creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets()


def get_sheet_titles(svc, spreadsheet_id: str) -> List[str]:
    meta = svc.get(spreadsheetId=spreadsheet_id, ranges=[], includeGridData=False).execute()
    sheets = meta.get("sheets", [])
    return [s["properties"]["title"] for s in sheets]


def fetch_values(svc, spreadsheet_id: str, a1_range: str) -> List[List[str]]:
    res = svc.values().get(spreadsheetId=spreadsheet_id, range=a1_range).execute()
    return res.get("values", [])


def pad_row(row: List[str], length: int) -> List[str]:
    if len(row) >= length:
        return row[:length]
    return row + [None] * (length - len(row))


def sheet_to_dataframe(svc, spreadsheet_id: str, sheet_title: str) -> pd.DataFrame:
    # Headers in rij 2, data vanaf rij 3 t/m 1000
    headers_range = f"{sheet_title}!A2:AG2"
    data_range = f"{sheet_title}!A3:AG1000"
    # Optioneel: dagen van de maand (C1:AG1)
    days_range = f"{sheet_title}!C1:AG1"

    headers_rows = fetch_values(svc, spreadsheet_id, headers_range)
    headers = headers_rows[0] if headers_rows else []
    if not headers:
        # Fallback: genereer kolomnamen als A..AG
        headers = [f"col_{i+1}" for i in range(33)]  # A..AG = 33 kolommen

    data_rows = fetch_values(svc, spreadsheet_id, data_range)
    padded = [pad_row(r, len(headers)) for r in data_rows]
    df = pd.DataFrame(padded, columns=headers)

    # Voeg dag-headers toe als metadata-kolomnamen (optioneel, kan later gebruikt worden)
    days = fetch_values(svc, spreadsheet_id, days_range)
    if days and days[0]:
        df.attrs["days_row"] = days[0]

    return df


def main():
    load_dotenv()

    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    creds_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "google_sheets_credentials.json")

    if not spreadsheet_id:
        raise SystemExit("GOOGLE_SHEETS_SPREADSHEET_ID ontbreekt in .env")

    creds_path = Path(creds_file)
    if not creds_path.exists():
        raise SystemExit(f"Credentials bestand niet gevonden: {creds_path}")

    svc = get_service(str(creds_path))
    titles = get_sheet_titles(svc, spreadsheet_id)

    out_dir = Path("outputs/google_sheets")
    out_dir.mkdir(parents=True, exist_ok=True)

    summary: Dict[str, int] = {}
    for title in titles:
        df = sheet_to_dataframe(svc, spreadsheet_id, title)
        # Verwijder volledig lege rijen
        df = df.dropna(how="all")
        df.to_csv(out_dir / f"{title}.csv", index=False)
        summary[title] = len(df)

    print("Sheets → CSV weggeschreven:")
    for t, n in summary.items():
        print(f"- {t}: {n} rijen")


if __name__ == "__main__":
    main()

