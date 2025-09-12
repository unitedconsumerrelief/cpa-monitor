#!/usr/bin/env python3
"""
Check data specifically for Sep 12
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

def check_sep12_data():
    """Check data specifically for Sep 12"""
    print("🔍 Checking data for Sep 12...")
    
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
        
        # Find column indices
        call_id_idx = headers.index("call_id") if "call_id" in headers else 0
        call_start_idx = headers.index("call_start_utc") if "call_start_utc" in headers else 1
        caller_id_idx = headers.index("caller_id") if "caller_id" in headers else 4
        
        # Filter for Sep 12 data
        sep12_rows = []
        sep12_caller_ids = set()
        
        for row in rows:
            if len(row) > max(call_id_idx, call_start_idx, caller_id_idx):
                call_start = row[call_start_idx]
                caller_id = row[caller_id_idx]
                
                # Check if the call is from Sep 12
                if "2025-09-12" in call_start:
                    sep12_rows.append(row)
                    if caller_id:
                        sep12_caller_ids.add(caller_id)
        
        print(f"\n📅 SEP 12 DATA:")
        print(f"   Total calls for Sep 12: {len(sep12_rows)}")
        print(f"   Unique caller IDs for Sep 12: {len(sep12_caller_ids)}")
        
        # Check for the specific missing caller IDs from Ringba
        ringba_caller_ids = ["+13213392856", "+13215224608", "+18563618680"]
        
        print(f"\n🔍 Checking Ringba caller IDs:")
        for caller_id in ringba_caller_ids:
            if caller_id in sep12_caller_ids:
                print(f"   ✅ Found: {caller_id}")
            else:
                print(f"   ❌ Missing: {caller_id}")
        
        # Show all Sep 12 caller IDs
        print(f"\n📞 All Sep 12 caller IDs in sheets:")
        for caller_id in sorted(list(sep12_caller_ids)):
            print(f"   {caller_id}")
        
        # Show recent entries from Sep 12
        print(f"\n📅 Recent entries from Sep 12:")
        for i, row in enumerate(sep12_rows, 1):
            if len(row) > max(call_id_idx, call_start_idx, caller_id_idx):
                print(f"   {i}. Call ID: {row[call_id_idx]}, Caller: {row[caller_id_idx]}, Time: {row[call_start_idx]}")
        
    except Exception as e:
        print(f"❌ Error checking Sep 12 data: {e}")

if __name__ == "__main__":
    check_sep12_data()
