#!/usr/bin/env python3
"""Simple test to fetch Ringba data from 1pm-3pm EST"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, timezone

async def test_ringba_api():
    """Test the Ringba API with 1pm-3pm EST data"""
    ringba_api_token = os.getenv("RINGBA_API_TOKEN")
    ringba_account_id = os.getenv("RINGBA_ACCOUNT_ID", "RA092c10a91f7c461098e354a1bbeda598")
    
    if not ringba_api_token:
        print("❌ RINGBA_API_TOKEN not found in environment variables")
        return
    
    url = f"https://api.ringba.com/v2/{ringba_account_id}/insights"
    
    headers = {
        "Authorization": f"Token {ringba_api_token}",
        "Content-Type": "application/json"
    }
    
    # Set up 1pm-3pm EST time range
    est_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    start_time_est = est_now.replace(hour=13, minute=0, second=0, microsecond=0)  # 1pm EST
    end_time_est = est_now.replace(hour=15, minute=0, second=0, microsecond=0)    # 3pm EST
    
    # Convert to UTC for API call
    start_time = start_time_est.astimezone(timezone.utc)
    end_time = end_time_est.astimezone(timezone.utc)
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "reportStart": start_str,
        "reportEnd": end_str,
        "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
        "valueColumns": [
            {"column": "callCount", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "generateRollups": True,
        "maxResultsPerGroup": 100,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print(f"🕐 Current EST time: {est_now}")
    print(f"📅 Testing 1pm-3pm EST data fetch")
    print(f"⏰ Start time (UTC): {start_time}")
    print(f"⏰ End time (UTC): {end_time}")
    print(f"🔗 API URL: {url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                print(f"📡 Response status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ SUCCESS: API call successful")
                    print(f"📊 Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    
                    if "data" in data and "rows" in data["data"]:
                        rows = data["data"]["rows"]
                        print(f"📈 Found {len(rows)} publisher records")
                        
                        if rows:
                            print(f"\n🏆 Top 5 Publishers (1pm-3pm EST):")
                            for i, row in enumerate(rows[:5], 1):
                                publisher = row.get("publisherName", "Unknown")
                                calls = row.get("callCount", 0)
                                completed = row.get("completedCalls", 0)
                                revenue = row.get("conversionAmount", 0.0)
                                cpa = revenue / completed if completed > 0 else 0
                                print(f"  {i}. {publisher}: {completed} completed, ${cpa:.2f} CPA")
                            
                            # Calculate totals
                            total_calls = sum(row.get("callCount", 0) for row in rows)
                            total_completed = sum(row.get("completedCalls", 0) for row in rows)
                            total_revenue = sum(row.get("conversionAmount", 0.0) for row in rows)
                            total_cpa = total_revenue / total_completed if total_completed > 0 else 0
                            
                            print(f"\n📊 TOTALS:")
                            print(f"  • Total Calls: {total_calls}")
                            print(f"  • Completed Calls: {total_completed}")
                            print(f"  • Revenue: ${total_revenue:.2f}")
                            print(f"  • CPA: ${total_cpa:.2f}")
                        else:
                            print("⚠️  No data found for 1pm-3pm EST time range")
                    else:
                        print(f"⚠️  Unexpected data structure: {data}")
                else:
                    error_text = await response.text()
                    print(f"❌ API error {response.status}: {error_text}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_ringba_api())
