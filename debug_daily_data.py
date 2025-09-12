#!/usr/bin/env python3
"""Debug script to compare Ringba API response with processed data"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def fetch_raw_ringba_data():
    """Fetch raw Ringba data to debug missing publishers"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # Get today's date range (9am-9pm EDT = 13:00-01:00 UTC next day)
    today = datetime.now(timezone.utc).date()
    start_time = datetime.combine(today, datetime.min.time().replace(hour=13), tzinfo=timezone.utc)
    end_time = datetime.combine(today, datetime.min.time().replace(hour=1), tzinfo=timezone.utc) + timedelta(days=1)
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    payload = {
        "reportStart": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportEnd": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
        "valueColumns": [
            {"column": "callCount", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None},
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print(f"🔍 Fetching Ringba data for: {start_time} to {end_time} UTC")
    print(f"📅 Date range: {today} (9am-9pm EDT)")
    print()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    print("✅ API Response received")
                    print(f"📊 Response keys: {list(data.keys())}")
                    
                    if "report" in data and "records" in data["report"]:
                        records = data["report"]["records"]
                        print(f"📋 Total records returned: {len(records)}")
                        print()
                        
                        # Calculate totals
                        total_completed = 0
                        total_payout = 0.0
                        total_revenue = 0.0
                        
                        print("📊 RAW RINGBA DATA:")
                        print("=" * 80)
                        print(f"{'Publisher':<25} {'Completed':<10} {'Payout':<10} {'Revenue':<10}")
                        print("-" * 80)
                        
                        for i, row in enumerate(records):
                            publisher = row.get("publisherName", "MISSING")
                            completed = int(row.get("completedCalls", 0))
                            payout = float(row.get("payoutAmount", 0))
                            revenue = float(row.get("conversionAmount", 0))
                            
                            total_completed += completed
                            total_payout += payout
                            total_revenue += revenue
                            
                            print(f"{publisher[:24]:<25} {completed:<10} ${payout:<9.2f} ${revenue:<9.2f}")
                            
                            # Show first 10 and last 10 records
                            if i == 9 and len(records) > 20:
                                print("... (showing first 10 and last 10)")
                                continue
                            if i >= len(records) - 10 or i < 10:
                                continue
                        
                        print("-" * 80)
                        print(f"{'TOTALS':<25} {total_completed:<10} ${total_payout:<9.2f} ${total_revenue:<9.2f}")
                        print()
                        
                        print(f"🎯 EXPECTED FROM RINGBA SCREENSHOT:")
                        print(f"   Completed: 96")
                        print(f"   Payout: $375.00")
                        print(f"   Revenue: $0.00")
                        print()
                        print(f"📊 ACTUAL API RESPONSE:")
                        print(f"   Completed: {total_completed}")
                        print(f"   Payout: ${total_payout:.2f}")
                        print(f"   Revenue: ${total_revenue:.2f}")
                        print()
                        
                        if total_completed != 96:
                            print(f"❌ MISMATCH: Expected 96 completed, got {total_completed}")
                            print(f"   Missing: {96 - total_completed} calls")
                        else:
                            print("✅ Completed calls match!")
                            
                        if abs(total_payout - 375.0) > 0.01:
                            print(f"❌ MISMATCH: Expected $375.00 payout, got ${total_payout:.2f}")
                        else:
                            print("✅ Payout matches!")
                    
                    else:
                        print("❌ No report data in response")
                        print(f"Response structure: {json.dumps(data, indent=2)[:500]}...")
                        
                else:
                    print(f"❌ API Error: {response.status}")
                    error_text = await response.text()
                    print(f"Error: {error_text}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(fetch_raw_ringba_data())
