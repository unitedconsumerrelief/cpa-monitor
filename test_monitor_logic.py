#!/usr/bin/env python3
"""Test the exact monitor.py logic"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

def safe_float(value, default=0.0):
    """Safely convert value to float, handling percentages"""
    if not value:
        return default
    try:
        # Remove % and convert to float
        clean_value = str(value).replace('%', '')
        return float(clean_value)
    except:
        return default

def safe_int(value, default=0):
    """Safely convert value to int"""
    if not value:
        return default
    try:
        return int(float(value))
    except:
        return default

async def test_monitor_logic():
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
            {"column": "liveCallCount", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "endedCalls", "aggregateFunction": None},
            {"column": "connectedCallCount", "aggregateFunction": None},
            {"column": "payoutCount", "aggregateFunction": None},
            {"column": "convertedCalls", "aggregateFunction": None},
            {"column": "nonConnectedCallCount", "aggregateFunction": None},
            {"column": "duplicateCalls", "aggregateFunction": None},
            {"column": "blockedCalls", "aggregateFunction": None},
            {"column": "incompleteCalls", "aggregateFunction": None},
            {"column": "earningsPerCallGross", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None},
            {"column": "profitGross", "aggregateFunction": None},
            {"column": "profitMarginGross", "aggregateFunction": None},
            {"column": "convertedPercent", "aggregateFunction": None},
            {"column": "callLengthInSeconds", "aggregateFunction": None},
            {"column": "avgHandleTime", "aggregateFunction": None},
            {"column": "totalCost", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "generateRollups": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print("TESTING EXACT MONITOR.PY LOGIC")
    print("Expected: 10 publishers, 87 calls, 68 completed, $515 payout")
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                records = data["report"]["records"]
                
                print(f"\nRAW API DATA: {len(records)} records")
                
                # Apply EXACT same logic as monitor.py
                publishers = []
                for row in records:
                    # Handle missing, empty, or "Unknown" publisher names
                    publisher_name = row.get("publisherName", "")
                    if not publisher_name or publisher_name.strip() == "" or publisher_name.strip().lower() in ["unknown", "missing"]:
                        publisher_name = "Unknown Publisher"
                    
                    # Skip "Unknown Publisher" and "MISSING" to match Ringba interface display
                    if publisher_name == "Unknown Publisher" or row.get("publisherName", "").upper() == "MISSING":
                        print(f"SKIPPING: {row.get('publisherName', '')} -> {publisher_name}")
                        continue
                    
                    print(f"KEEPING: {row.get('publisherName', '')} -> {publisher_name}")
                    
                    publisher = {
                        "name": publisher_name,
                        "calls": safe_int(row.get("callCount", 0)),
                        "completed": safe_int(row.get("completedCalls", 0)),
                        "revenue": safe_float(row.get("conversionAmount", 0)),
                        "payout": safe_float(row.get("payoutAmount", 0)),
                        "profit": safe_float(row.get("profitGross", 0)),
                        "conversion_rate": safe_float(row.get("convertedPercent", 0)),
                        "connected": safe_int(row.get("connectedCallCount", 0)),
                        "live": safe_int(row.get("liveCallCount", 0)),
                        "ended": safe_int(row.get("endedCalls", 0)),
                        "paid": safe_int(row.get("payoutCount", 0)),
                        "converted": safe_int(row.get("convertedCalls", 0)),
                        "no_connect": safe_int(row.get("nonConnectedCallCount", 0)),
                        "duplicate": safe_int(row.get("duplicateCalls", 0)),
                        "blocked": safe_int(row.get("blockedCalls", 0)),
                        "ivr_hangup": safe_int(row.get("incompleteCalls", 0)),
                        "tcl_seconds": safe_int(row.get("callLengthInSeconds", 0)),
                        "acl_seconds": safe_int(row.get("avgHandleTime", 0)),
                        "total_cost": safe_float(row.get("totalCost", 0))
                    }
                    # Calculate CPA (Payout / Completed)
                    publisher["cpa"] = publisher["payout"] / publisher["completed"] if publisher["completed"] > 0 else 0.0
                    publishers.append(publisher)
                
                # Calculate totals
                total_calls = sum(p["calls"] for p in publishers)
                total_completed = sum(p["completed"] for p in publishers)
                total_revenue = sum(p["revenue"] for p in publishers)
                total_payout = sum(p["payout"] for p in publishers)
                total_profit = sum(p["profit"] for p in publishers)
                total_cpa = total_payout / total_completed if total_completed > 0 else 0
                
                print(f"\nFILTERED RESULTS:")
                print(f"  Publishers: {len(publishers)} (Expected: 10)")
                print(f"  Total calls: {total_calls} (Expected: 87)")
                print(f"  Total completed: {total_completed} (Expected: 68)")
                print(f"  Total payout: ${total_payout} (Expected: $515)")
                print(f"  Total CPA: ${total_cpa:.2f}")
                
                print(f"\nALL PUBLISHERS:")
                for i, p in enumerate(publishers, 1):
                    print(f"  {i:2d}. {p['name']:<20} | {p['completed']:3d} completed | ${p['cpa']:6.2f} CPA | ${p['payout']:8.2f} payout")
                
                if len(publishers) == 10 and total_calls == 87 and total_completed == 68:
                    print(f"\n🎉 EXACT MATCH! Ready to commit!")
                else:
                    print(f"\n❌ Still not exact match")
            else:
                print(f"Error: {response.status}")

if __name__ == "__main__":
    asyncio.run(test_monitor_logic())


