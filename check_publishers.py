#!/usr/bin/env python3
"""Check what publishers the API actually returns"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def check_publishers():
    """Check what publishers the API actually returns"""
    
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # 1pm-3pm EST for 09/11/2025
    start_est = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
    end_est = datetime(2025, 9, 11, 15, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
    
    start_utc = start_est.astimezone(timezone.utc)
    end_utc = end_est.astimezone(timezone.utc)
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {
        "Authorization": f"Token {ringba_token}",
        "Content-Type": "application/json"
    }
    
    # Simple payload - just get publisher names
    payload = {
        "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
        "valueColumns": [{"column": "callCount", "aggregateFunction": None}],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print(f"🔍 CHECKING WHAT PUBLISHERS API ACTUALLY RETURNS")
    print(f"⏰ Time range: 1pm-3pm EST (09/11/2025)")
    print(f"🔗 Calling Ringba API...")
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                records = data["report"]["records"]
                
                print(f"✅ API returned {len(records)} records")
                print(f"\n📋 RAW PUBLISHER NAMES FROM API:")
                
                for i, record in enumerate(records):
                    pub_name = record.get("publisherName", "MISSING")
                    calls = record.get("callCount", 0)
                    
                    # Show exactly what we get
                    if pub_name == "":
                        print(f"  {i+1:2d}. publisherName: \"\" (empty string) | calls: {calls}")
                    elif pub_name is None:
                        print(f"  {i+1:2d}. publisherName: None | calls: {calls}")
                    else:
                        print(f"  {i+1:2d}. publisherName: \"{pub_name}\" | calls: {calls}")
                
                # Check if any are empty/None
                empty_count = sum(1 for r in records if not r.get("publisherName", "").strip())
                print(f"\n🔍 ANALYSIS:")
                print(f"  • Total records: {len(records)}")
                print(f"  • Empty/None publisher names: {empty_count}")
                print(f"  • Valid publisher names: {len(records) - empty_count}")
                
                if empty_count > 0:
                    print(f"  ❌ PROBLEM: API is returning empty publisher names!")
                else:
                    print(f"  ✅ All publisher names are valid")
                    
            else:
                print(f"❌ API error: {response.status}")

if __name__ == "__main__":
    asyncio.run(check_publishers())
