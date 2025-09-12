#!/usr/bin/env python3
"""Test exact match with Ringba interface"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def test_exact_match():
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # 1pm-3pm EST for 09/11/2025 (UTC-5)
    start_utc = datetime(2025, 9, 11, 18, 0, 0, tzinfo=timezone.utc)  # 6pm UTC = 1pm EST
    end_utc = datetime(2025, 9, 11, 20, 0, 0, tzinfo=timezone.utc)    # 8pm UTC = 3pm EST
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
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
        "formatTimespans": True,
        "formatPercentages": True,
        "generateRollups": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print("TESTING EXACT MATCH WITH RINGBA INTERFACE")
    print("Expected: 10 publishers, 87 calls, 68 completed, $515 payout")
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                records = data["report"]["records"]
                
                print(f"\nRAW API RESPONSE: {len(records)} records")
                
                # Show all records first
                for i, record in enumerate(records):
                    pub_name = record.get("publisherName", "MISSING")
                    calls = record.get("callCount", 0)
                    completed = record.get("completedCalls", 0)
                    payout = record.get("payoutAmount", 0)
                    print(f"  {i+1:2d}. Publisher: '{pub_name}' | Calls: {calls} | Completed: {completed} | Payout: {payout}")
                
                # Apply filtering logic (same as monitor.py)
                print(f"\nAPPLYING FILTERING LOGIC:")
                filtered_publishers = []
                total_calls = 0
                total_completed = 0
                total_payout = 0
                
                for record in records:
                    publisher_name = record.get("publisherName", "")
                    if not publisher_name or publisher_name.strip() == "" or publisher_name.strip().lower() == "unknown":
                        publisher_name = "Unknown Publisher"
                    
                    # Skip "Unknown Publisher" to match Ringba interface display
                    if publisher_name == "Unknown Publisher":
                        print(f"  SKIPPING: {publisher_name}")
                        continue
                    
                    calls = record.get("callCount", 0)
                    completed = record.get("completedCalls", 0)
                    payout = record.get("payoutAmount", 0)
                    
                    total_calls += calls
                    total_completed += completed
                    if payout:
                        total_payout += float(payout)
                    
                    filtered_publishers.append(publisher_name)
                    print(f"  KEEPING: {publisher_name} | Calls: {calls} | Completed: {completed} | Payout: {payout}")
                
                print(f"\nFILTERED RESULTS:")
                print(f"  Publishers: {len(filtered_publishers)} (Expected: 10) - DIFF: {len(filtered_publishers) - 10}")
                print(f"  Total calls: {total_calls} (Expected: 87) - DIFF: {total_calls - 87}")
                print(f"  Total completed: {total_completed} (Expected: 68) - DIFF: {total_completed - 68}")
                print(f"  Total payout: ${total_payout} (Expected: $515) - DIFF: ${total_payout - 515}")
                
                # Check against Ringba expected publishers
                ringba_publishers = ['Koji Digital', 'FITZ', 'TDES008-YT', 'TDES023-YT', 'TDES082-YT', 'TDES085-YT', 'TDES089-YT', 'TDES092-YT', 'LeadVIPs', 'TDES093-YT']
                missing = [p for p in ringba_publishers if p not in filtered_publishers]
                extra = [p for p in filtered_publishers if p not in ringba_publishers]
                
                print(f"\nPUBLISHER COMPARISON:")
                print(f"  Ringba expects: {len(ringba_publishers)} publishers")
                print(f"  We have: {len(filtered_publishers)} publishers")
                if missing:
                    print(f"  Missing: {missing}")
                if extra:
                    print(f"  Extra: {extra}")
                
                if len(filtered_publishers) == 10 and total_calls == 87 and total_completed == 68:
                    print("\n✅ EXACT MATCH! Ready to commit!")
                else:
                    print("\n❌ Still not exact - need to investigate further")
            else:
                print(f"Error: {response.status}")

if __name__ == "__main__":
    asyncio.run(test_exact_match())


