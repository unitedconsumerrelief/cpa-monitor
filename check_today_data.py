#!/usr/bin/env python3
"""
Check data specifically for today (09/11)
"""

import os
import json
from datetime import datetime, timezone
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

def check_today_data():
    """Check data specifically for today (09/11)"""
    print("🔍 Checking data for today (09/11)...")
    
    try:
        sheets_service = init_google_sheets()
        sheet_id = extract_sheet_id(MASTER_CPA_DATA)
        
        # Read all Ringba Raw data
        range_name = "Ringba Raw!A:N"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            print("❌ No data found in Ringba Raw sheet")
            return
        
        headers = values[0]
        rows = values[1:]
        
        print(f"📊 Total rows in sheet: {len(rows)}")
        print(f"📋 Headers: {headers}")
        
        # Find column indices
        call_id_idx = headers.index("call_id") if "call_id" in headers else 0
        call_start_idx = headers.index("call_start_utc") if "call_start_utc" in headers else 1
        caller_id_idx = headers.index("caller_id") if "caller_id" in headers else 4
        
        # Filter for today's data (09/11)
        today_rows = []
        today_caller_ids = set()
        
        for row in rows:
            if len(row) > max(call_id_idx, call_start_idx, caller_id_idx):
                call_start = row[call_start_idx]
                caller_id = row[caller_id_idx]
                
                # Check if the call is from today (09/11)
                if "2024-09-11" in call_start or "2025-09-11" in call_start:
                    today_rows.append(row)
                    if caller_id:
                        today_caller_ids.add(caller_id)
        
        print(f"\n📅 TODAY'S DATA (09/11):")
        print(f"   Total calls for today: {len(today_rows)}")
        print(f"   Unique caller IDs for today: {len(today_caller_ids)}")
        
        # Check for the specific missing caller ID
        missing_caller = "+19564541202"
        if missing_caller in today_caller_ids:
            print(f"✅ Found missing caller ID for today: {missing_caller}")
        else:
            print(f"❌ Missing caller ID not found for today: {missing_caller}")
        
        # Show sample of today's data
        print(f"\n📞 Sample caller IDs from today:")
        for caller_id in sorted(list(today_caller_ids))[:10]:
            print(f"   {caller_id}")
        
        # Show recent entries from today
        print(f"\n📅 Recent entries from today (last 5):")
        for i, row in enumerate(today_rows[-5:], 1):
            if len(row) > max(call_id_idx, call_start_idx, caller_id_idx):
                print(f"   {i}. Call ID: {row[call_id_idx]}, Caller: {row[caller_id_idx]}, Time: {row[call_start_idx]}")
        
        # Check for calls with specific patterns
        print(f"\n🔍 Looking for calls with '9564541202' pattern:")
        found_pattern = False
        for row in today_rows:
            if len(row) > max(call_id_idx, call_start_idx, caller_id_idx):
                caller_id = row[caller_id_idx]
                if "9564541202" in caller_id:
                    print(f"   Found: {caller_id}")
                    found_pattern = True
        
        if not found_pattern:
            print("   No calls found with '9564541202' pattern")
        
    except Exception as e:
        print(f"❌ Error checking today's data: {e}")

if __name__ == "__main__":
    check_today_data()
