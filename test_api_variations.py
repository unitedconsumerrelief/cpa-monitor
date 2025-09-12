#!/usr/bin/env python3
"""Test different API parameter variations"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def test_api_variations():
    """Test different API parameter variations"""
    
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
    
    print(f"🔍 TESTING API PARAMETER VARIATIONS")
    print(f"⏰ Time range: 1pm-3pm EST (09/11/2025)")
    
    # Test different variations
    variations = [
        {
            "name": "With Publisher Filter (exclude empty)",
            "payload": {
                "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
                "valueColumns": [{"column": "callCount", "aggregateFunction": None}],
                "orderByColumns": [{"column": "callCount", "direction": "desc"}],
                "maxResultsPerGroup": 1000,
                "filters": [
                    {"column": "publisherName", "operator": "isNotEmpty", "value": ""}
                ],
                "formatTimeZone": "America/New_York"
            }
        },
        {
            "name": "With Publisher Filter (not equal to empty)",
            "payload": {
                "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
                "valueColumns": [{"column": "callCount", "aggregateFunction": None}],
                "orderByColumns": [{"column": "callCount", "direction": "desc"}],
                "maxResultsPerGroup": 1000,
                "filters": [
                    {"column": "publisherName", "operator": "notEqual", "value": ""}
                ],
                "formatTimeZone": "America/New_York"
            }
        },
        {
            "name": "With Publisher Filter (not null)",
            "payload": {
                "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
                "valueColumns": [{"column": "callCount", "aggregateFunction": None}],
                "orderByColumns": [{"column": "callCount", "direction": "desc"}],
                "maxResultsPerGroup": 1000,
                "filters": [
                    {"column": "publisherName", "operator": "notNull", "value": ""}
                ],
                "formatTimeZone": "America/New_York"
            }
        },
        {
            "name": "Different GroupBy (publisherId instead of publisherName)",
            "payload": {
                "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "groupByColumns": [{"column": "publisherId", "displayName": "Publisher"}],
                "valueColumns": [
                    {"column": "callCount", "aggregateFunction": None},
                    {"column": "publisherName", "aggregateFunction": None}
                ],
                "orderByColumns": [{"column": "callCount", "direction": "desc"}],
                "maxResultsPerGroup": 1000,
                "filters": [],
                "formatTimeZone": "America/New_York"
            }
        }
    ]
    
    async with aiohttp.ClientSession() as session:
        for i, variation in enumerate(variations, 1):
            print(f"\n🧪 TEST {i}: {variation['name']}")
            
            try:
                async with session.post(url, headers=headers, json=variation['payload']) as response:
                    print(f"📡 Response status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        
                        if "report" in data and "records" in data["report"]:
                            records = data["report"]["records"]
                            print(f"✅ Found {len(records)} records")
                            
                            # Check publisher names
                            publishers = []
                            for record in records:
                                pub_name = record.get("publisherName", "MISSING")
                                calls = record.get("callCount", 0)
                                publishers.append((pub_name, calls))
                            
                            print(f"📋 Publishers found:")
                            for j, (pub_name, calls) in enumerate(publishers, 1):
                                if pub_name == "MISSING" or not pub_name.strip():
                                    print(f"  {j:2d}. ❌ '{pub_name}' | calls: {calls}")
                                else:
                                    print(f"  {j:2d}. ✅ '{pub_name}' | calls: {calls}")
                            
                            # Check for missing/unknown publishers
                            missing_count = sum(1 for pub, _ in publishers if pub == "MISSING" or not pub.strip())
                            print(f"🔍 Analysis: {missing_count} missing/unknown publishers out of {len(publishers)}")
                            
                            if missing_count == 0:
                                print(f"🎉 SUCCESS! No missing publishers found!")
                                return variation
                            
                        else:
                            print(f"❌ Unexpected data structure")
                    else:
                        error_text = await response.text()
                        print(f"❌ API error {response.status}: {error_text}")
                        
            except Exception as e:
                print(f"❌ Error: {e}")
    
    print(f"\n❌ No variation found that eliminates missing publishers")

if __name__ == "__main__":
    asyncio.run(test_api_variations())
