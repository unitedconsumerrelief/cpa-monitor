#!/usr/bin/env python3
"""Debug why 12pm-2pm gave us 1pm-3pm data"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from dataclasses import dataclass

@dataclass
class PublisherMetrics:
    publisher_name: str
    incoming: int = 0
    completed: int = 0
    payout: float = 0.0
    revenue: float = 0.0

def parse_ringba_data(data: Dict[str, Any]) -> List[PublisherMetrics]:
    """Parse Ringba API response"""
    metrics_list = []
    
    try:
        if "report" in data and "records" in data["report"]:
            for row in data["report"]["records"]:
                publisher_name = row.get("publisherName", "")
                if not publisher_name or publisher_name.strip() == "":
                    publisher_name = "Unknown Publisher"
                
                metrics = PublisherMetrics(
                    publisher_name=publisher_name,
                    incoming=int(row.get("callCount", 0)),
                    completed=int(row.get("completedCalls", 0)),
                    payout=float(row.get("payoutAmount", 0)),
                    revenue=float(row.get("conversionAmount", 0))
                )
                metrics_list.append(metrics)
    except Exception as e:
        print(f"Error parsing data: {e}")
        
    return metrics_list

async def test_exact_1pm_3pm():
    """Test the exact 1pm-3pm EST time range from the dashboard"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # EXACT 1pm-3pm EST from dashboard screenshot
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
            {"column": "payoutAmount", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print("🔍 TESTING EXACT 1PM-3PM EST FROM DASHBOARD")
    print("=" * 60)
    print("Dashboard shows: 87 calls, 68 completed, $515 payout")
    print("Time range: 1pm-3pm EST (UTC 18:00-20:00)")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    metrics = parse_ringba_data(data)
                    
                    total_calls = sum(m.incoming for m in metrics)
                    total_completed = sum(m.completed for m in metrics)
                    total_payout = sum(m.payout for m in metrics)
                    total_revenue = sum(m.revenue for m in metrics)
                    
                    print(f"📊 API RESULTS FOR 1PM-3PM EST:")
                    print(f"   Publishers: {len(metrics)}")
                    print(f"   Total Calls: {total_calls}")
                    print(f"   Completed: {total_completed}")
                    print(f"   Total Payout: ${total_payout:.2f}")
                    print(f"   Total Revenue: ${total_revenue:.2f}")
                    
                    # Check if this matches dashboard
                    calls_match = total_calls == 87
                    completed_match = total_completed == 68
                    payout_match = abs(total_payout - 515) < 1
                    
                    print(f"\n📈 ACCURACY CHECK:")
                    print(f"   Calls: {'✅' if calls_match else '❌'} (Got: {total_calls}, Expected: 87)")
                    print(f"   Completed: {'✅' if completed_match else '❌'} (Got: {total_completed}, Expected: 68)")
                    print(f"   Payout: {'✅' if payout_match else '❌'} (Got: ${total_payout:.2f}, Expected: $515)")
                    
                    if calls_match and completed_match and payout_match:
                        print(f"\n🎯 PERFECT MATCH! 1pm-3pm EST is correct!")
                        return True, metrics
                    else:
                        print(f"\n❌ 1pm-3pm EST doesn't match dashboard exactly")
                        return False, metrics
                        
                else:
                    print(f"❌ API Error: {response.status}")
                    return False, []
    except Exception as e:
        print(f"❌ Error: {e}")
        return False, []

async def test_why_12pm_2pm_worked():
    """Test why 12pm-2pm gave us the right numbers"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # Test 12pm-2pm EST again
    start_utc = datetime(2025, 9, 11, 17, 0, 0, tzinfo=timezone.utc)  # 5pm UTC = 12pm EST
    end_utc = datetime(2025, 9, 11, 19, 0, 0, tzinfo=timezone.utc)    # 7pm UTC = 2pm EST
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    payload = {
        "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
        "valueColumns": [
            {"column": "callCount", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print(f"\n🔍 TESTING 12PM-2PM EST (WHY IT WORKED)")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    metrics = parse_ringba_data(data)
                    
                    total_calls = sum(m.incoming for m in metrics)
                    total_completed = sum(m.completed for m in metrics)
                    total_payout = sum(m.payout for m in metrics)
                    
                    print(f"📊 API RESULTS FOR 12PM-2PM EST:")
                    print(f"   Publishers: {len(metrics)}")
                    print(f"   Total Calls: {total_calls}")
                    print(f"   Completed: {total_completed}")
                    print(f"   Total Payout: ${total_payout:.2f}")
                    
                    print(f"\n🤔 MYSTERY: Why did 12pm-2pm give us 1pm-3pm data?")
                    print(f"   This suggests there might be a timezone offset issue")
                    print(f"   or the API is returning data from a different time range")
                    
                else:
                    print(f"❌ API Error: {response.status}")
    except Exception as e:
        print(f"❌ Error: {e}")

async def test_timezone_offsets():
    """Test different timezone interpretations"""
    print(f"\n🔍 TESTING TIMEZONE OFFSETS")
    print("=" * 60)
    
    # Test if there's a timezone offset issue
    test_cases = [
        {
            "name": "1pm-3pm EST (UTC-5)",
            "start_utc": datetime(2025, 9, 11, 18, 0, 0, tzinfo=timezone.utc),  # 6pm UTC = 1pm EST
            "end_utc": datetime(2025, 9, 11, 20, 0, 0, tzinfo=timezone.utc),    # 8pm UTC = 3pm EST
        },
        {
            "name": "1pm-3pm EST (UTC-4, Daylight Time)",
            "start_utc": datetime(2025, 9, 11, 17, 0, 0, tzinfo=timezone.utc),  # 5pm UTC = 1pm EDT
            "end_utc": datetime(2025, 9, 11, 19, 0, 0, tzinfo=timezone.utc),    # 7pm UTC = 3pm EDT
        },
        {
            "name": "1pm-3pm EST (UTC-6, Central Time)",
            "start_utc": datetime(2025, 9, 11, 19, 0, 0, tzinfo=timezone.utc),  # 7pm UTC = 1pm CST
            "end_utc": datetime(2025, 9, 11, 21, 0, 0, tzinfo=timezone.utc),    # 9pm UTC = 3pm CST
        }
    ]
    
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📅 TEST {i}: {test_case['name']}")
        print(f"   UTC Range: {test_case['start_utc']} to {test_case['end_utc']}")
        
        payload = {
            "reportStart": test_case["start_utc"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reportEnd": test_case["end_utc"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
            "valueColumns": [
                {"column": "callCount", "aggregateFunction": None},
                {"column": "completedCalls", "aggregateFunction": None},
                {"column": "payoutAmount", "aggregateFunction": None},
                {"column": "conversionAmount", "aggregateFunction": None}
            ],
            "orderByColumns": [{"column": "callCount", "direction": "desc"}],
            "formatTimespans": True,
            "formatPercentages": True,
            "maxResultsPerGroup": 1000,
            "filters": [],
            "formatTimeZone": "America/New_York"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        metrics = parse_ringba_data(data)
                        
                        total_calls = sum(m.incoming for m in metrics)
                        total_completed = sum(m.completed for m in metrics)
                        total_payout = sum(m.payout for m in metrics)
                        
                        print(f"   Results: {len(metrics)} publishers, {total_calls} calls, {total_completed} completed, ${total_payout:.2f} payout")
                        
                        if total_calls == 87 and total_completed == 68 and abs(total_payout - 515) < 1:
                            print(f"   🎯 PERFECT MATCH! This timezone interpretation works!")
                            return test_case, metrics
                        else:
                            print(f"   ❌ No match")
                            
                    else:
                        print(f"   ❌ API Error: {response.status}")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None, []

async def main():
    """Main function to debug time range mismatch"""
    print("🚀 DEBUGGING TIME RANGE MISMATCH")
    print("=" * 80)
    
    # Test exact 1pm-3pm EST from dashboard
    is_match, metrics = await test_exact_1pm_3pm()
    
    if is_match:
        print(f"\n✅ 1pm-3pm EST works perfectly! Use this time range.")
    else:
        print(f"\n❌ 1pm-3pm EST doesn't work. Testing why 12pm-2pm worked...")
        await test_why_12pm_2pm_worked()
        
        print(f"\n🔍 Testing different timezone interpretations...")
        correct_timezone, correct_metrics = await test_timezone_offsets()
        
        if correct_timezone:
            print(f"\n✅ FOUND CORRECT TIMEZONE: {correct_timezone['name']}")
        else:
            print(f"\n❌ No timezone interpretation worked. Need to investigate further.")
    
    print(f"\n🏁 Debug complete!")

if __name__ == "__main__":
    asyncio.run(main())
