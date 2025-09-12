#!/usr/bin/env python3
"""
Script to check current data in Google Sheets and compare with Ringba
"""

import os
import json
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Environment variables
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
MASTER_CPA_DATA = os.getenv("MASTER_CPA_DATA")
RINGBA_API_TOKEN = os.getenv("RINGBA_API_TOKEN")
RINGBA_ACCOUNT_ID = os.getenv("RINGBA_ACCOUNT_ID", "RA092c10a91f7c461098e354a1bbeda598")

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

def read_sheet_data(sheets_service, sheet_name: str):
    """Read all data from the specified sheet"""
    try:
        sheet_id = extract_sheet_id(MASTER_CPA_DATA)
        range_name = f"{sheet_name}!A:N"
        
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        return result.get('values', [])
    except Exception as e:
        print(f"Error reading from {sheet_name} sheet: {e}")
        return []

async def fetch_ringba_data():
    """Fetch recent data from Ringba API"""
    url = f"https://api.ringba.com/v2/{RINGBA_ACCOUNT_ID}/insights"
    
    headers = {
        "Authorization": f"Token {RINGBA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Get data for last 7 days
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=7)
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "reportStart": start_str,
        "reportEnd": end_str,
        "groupByColumns": [{"column": "callId", "displayName": "Call ID"}],
        "valueColumns": [
            {"column": "callCount", "aggregateFunction": None},
            {"column": "callerId", "aggregateFunction": None},
            {"column": "publisherName", "aggregateFunction": None},
            {"column": "campaignName", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    error_text = await response.text()
                    print(f"Ringba API error {response.status}: {error_text}")
                    return None
    except Exception as e:
        print(f"Error fetching Ringba data: {e}")
        return None

def analyze_data():
    """Analyze the data in Google Sheets"""
    print("🔍 Analyzing Google Sheets data...")
    
    try:
        sheets_service = init_google_sheets()
        
        # Read Ringba Raw data
        raw_data = read_sheet_data(sheets_service, "Ringba Raw")
        
        if not raw_data or len(raw_data) <= 1:
            print("❌ No data found in Ringba Raw sheet")
            return
        
        headers = raw_data[0]
        rows = raw_data[1:]
        
        print(f"📊 Total rows in sheet: {len(rows)}")
        print(f"📋 Headers: {headers}")
        
        # Find column indices
        call_id_idx = headers.index("call_id") if "call_id" in headers else 0
        caller_id_idx = headers.index("caller_id") if "caller_id" in headers else 4
        campaign_idx = headers.index("campaign") if "campaign" in headers else 7
        publisher_idx = headers.index("publisher_name") if "publisher_name" in headers else 10
        
        # Analyze data
        call_ids = set()
        caller_ids = set()
        campaigns = set()
        publishers = set()
        
        for row in rows:
            if len(row) > max(call_id_idx, caller_id_idx, campaign_idx, publisher_idx):
                call_ids.add(row[call_id_idx])
                if row[caller_id_idx]:
                    caller_ids.add(row[caller_id_idx])
                if row[campaign_idx]:
                    campaigns.add(row[campaign_idx])
                if row[publisher_idx]:
                    publishers.add(row[publisher_idx])
        
        print(f"\n📈 Data Analysis:")
        print(f"   Unique Call IDs: {len(call_ids)}")
        print(f"   Unique Caller IDs: {len(caller_ids)}")
        print(f"   Unique Campaigns: {len(campaigns)}")
        print(f"   Unique Publishers: {len(publishers)}")
        
        print(f"\n📞 Sample Caller IDs:")
        for caller_id in sorted(list(caller_ids))[:10]:
            print(f"   {caller_id}")
        
        print(f"\n🏢 Campaigns:")
        for campaign in sorted(list(campaigns)):
            print(f"   {campaign}")
        
        print(f"\n👥 Publishers:")
        for publisher in sorted(list(publishers)):
            print(f"   {publisher}")
        
        # Check for the specific missing caller ID
        missing_caller = "+19564541202"
        if missing_caller in caller_ids:
            print(f"\n✅ Found missing caller ID: {missing_caller}")
        else:
            print(f"\n❌ Missing caller ID not found: {missing_caller}")
            
        # Show recent entries
        print(f"\n📅 Recent entries (last 5):")
        for i, row in enumerate(rows[-5:], 1):
            if len(row) > max(call_id_idx, caller_id_idx):
                print(f"   {i}. Call ID: {row[call_id_idx]}, Caller: {row[caller_id_idx]}")
        
    except Exception as e:
        print(f"❌ Error analyzing data: {e}")

async def main():
    """Main function"""
    print("🚀 Google Sheets Data Analysis")
    print("=" * 50)
    
    # Check environment variables
    if not GOOGLE_CREDENTIALS_JSON:
        print("❌ GOOGLE_CREDENTIALS_JSON not set")
        return
    
    if not MASTER_CPA_DATA:
        print("❌ MASTER_CPA_DATA not set")
        return
    
    # Analyze current data
    analyze_data()
    
    print(f"\n🔍 To check if webhook is working:")
    print(f"   1. Start your webhook server: python app.py")
    print(f"   2. Test with: python test_webhook_debug.py")
    print(f"   3. Check logs for webhook activity")

if __name__ == "__main__":
    asyncio.run(main())
