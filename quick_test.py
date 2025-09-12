#!/usr/bin/env python3
"""Quick test to see what the API actually returns"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def quick_test():
    """Quick test of API response"""
    
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
    
    payload = {
        "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
        "valueColumns": [
            {"column": "callCount", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                records = data["report"]["records"]
                
                print(f"API returned {len(records)} records")
                print(f"\nPublisher names in API response:")
                
                for i, record in enumerate(records):
                    pub_name = record.get("publisherName")
                    calls = record.get("callCount", 0)
                    completed = record.get("completedCalls", 0)
                    payout = record.get("payoutAmount", 0)
                    
                    print(f"  {i+1:2d}. publisherName: '{pub_name}' | calls: {calls} | completed: {completed} | payout: {payout}")
                    
                    # Check what our fix would do
                    if not pub_name or pub_name.strip() == "":
                        print(f"      -> Would become: 'Unknown Publisher'")
                    else:
                        print(f"      -> Would stay: '{pub_name}'")
            else:
                print(f"API error: {response.status}")

if __name__ == "__main__":
    asyncio.run(quick_test())