#!/usr/bin/env python3
"""
Check what sheets exist in the Google Spreadsheet
"""

import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Environment variables
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
MASTER_CPA_DATA = os.getenv("MASTER_CPA_DATA")

def init_google_sheets():
    """Initialize Google Sheets service"""
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is required")
    
    credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=credentials)

def extract_sheet_id(url_or_id: str) -> str:
    """Extract sheet ID from URL or return as-is if already an ID"""
    import re
    if url_or_id.startswith("http"):
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url_or_id)
        return match.group(1) if match else url_or_id
    return url_or_id

def check_sheets():
    """Check what sheets exist"""
    print("🔍 Checking Google Sheets structure...")
    
    try:
        sheets_service = init_google_sheets()
        sheet_id = extract_sheet_id(MASTER_CPA_DATA)
        
        # Get spreadsheet metadata
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        
        print(f"📊 Spreadsheet Title: {spreadsheet['properties']['title']}")
        print(f"📋 Available Sheets:")
        
        for sheet in spreadsheet['sheets']:
            sheet_name = sheet['properties']['title']
            sheet_id = sheet['properties']['sheetId']
            print(f"   - {sheet_name} (ID: {sheet_id})")
            
            # Try to read a small sample from each sheet
            try:
                range_name = f"{sheet_name}!A1:Z10"
                result = sheets_service.spreadsheets().values().get(
                    spreadsheetId=extract_sheet_id(MASTER_CPA_DATA),
                    range=range_name
                ).execute()
                
                values = result.get('values', [])
                print(f"     Sample data: {len(values)} rows")
                if values:
                    print(f"     Headers: {values[0] if values else 'No data'}")
                    
            except Exception as e:
                print(f"     Error reading sheet: {e}")
        
    except Exception as e:
        print(f"❌ Error checking sheets: {e}")

if __name__ == "__main__":
    check_sheets()
