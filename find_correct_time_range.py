#!/usr/bin/env python3
"""Find the exact time range that matches the Ringba dashboard"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple
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

async def test_time_range(start_utc: datetime, end_utc: datetime, description: str) -> Tuple[bool, List[PublisherMetrics]]:
    """Test a specific time range"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
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
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    metrics = parse_ringba_data(data)
                    
                    total_calls = sum(m.incoming for m in metrics)
                    total_completed = sum(m.completed for m in metrics)
                    total_payout = sum(m.payout for m in metrics)
                    
                    # Check if this matches dashboard exactly
                    is_match = (total_calls == 87 and total_completed == 68 and abs(total_payout - 515) < 1)
                    
                    print(f"   {description}")
                    print(f"   Time: {start_utc} to {end_utc} UTC")
                    print(f"   Results: {len(metrics)} publishers, {total_calls} calls, {total_completed} completed, ${total_payout:.2f} payout")
                    
                    if is_match:
                        print(f"   🎯 PERFECT MATCH!")
                        return True, metrics
                    else:
                        print(f"   ❌ No match")
                        return False, metrics
                else:
                    print(f"   ❌ API Error: {response.status}")
                    return False, []
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False, []

async def find_correct_time_range():
    """Find the exact time range that matches the dashboard"""
    print("🔍 FINDING CORRECT TIME RANGE")
    print("=" * 60)
    print("Expected from Ringba Dashboard:")
    print("  Total Calls: 87")
    print("  Completed Calls: 68")
    print("  Total Payout: $515")
    print("=" * 60)
    
    # Test different time ranges around 1pm-3pm EST on 09/11/2025
    base_date = datetime(2025, 9, 11, tzinfo=timezone.utc)
    
    test_cases = []
    
    # Test different hours around 1pm-3pm EST (6pm-8pm UTC)
    for hour_offset in range(-2, 3):  # -2 to +2 hours
        start_hour = 18 + hour_offset  # 6pm UTC + offset
        end_hour = 20 + hour_offset    # 8pm UTC + offset
        
        start_utc = base_date.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        end_utc = base_date.replace(hour=end_hour, minute=0, second=0, microsecond=0)
        
        est_start = start_utc.astimezone(timezone(timedelta(hours=-5)))
        est_end = end_utc.astimezone(timezone(timedelta(hours=-5)))
        
        test_cases.append({
            "start_utc": start_utc,
            "end_utc": end_utc,
            "description": f"EST {est_start.hour}:00-{est_end.hour}:00 (UTC {start_utc.hour}:00-{end_utc.hour}:00)"
        })
    
    # Test different days
    for day_offset in range(-1, 2):  # -1 to +1 days
        test_date = base_date + timedelta(days=day_offset)
        start_utc = test_date.replace(hour=18, minute=0, second=0, microsecond=0)
        end_utc = test_date.replace(hour=20, minute=0, second=0, microsecond=0)
        
        est_start = start_utc.astimezone(timezone(timedelta(hours=-5)))
        est_end = end_utc.astimezone(timezone(timedelta(hours=-5)))
        
        test_cases.append({
            "start_utc": start_utc,
            "end_utc": end_utc,
            "description": f"{test_date.strftime('%Y-%m-%d')} EST {est_start.hour}:00-{est_end.hour}:00"
        })
    
    # Test current day
    now = datetime.now(timezone.utc)
    for hour_offset in range(-3, 4):  # -3 to +3 hours from now
        start_utc = now.replace(hour=(now.hour + hour_offset) % 24, minute=0, second=0, microsecond=0)
        end_utc = start_utc + timedelta(hours=2)
        
        est_start = start_utc.astimezone(timezone(timedelta(hours=-5)))
        est_end = end_utc.astimezone(timezone(timedelta(hours=-5)))
        
        test_cases.append({
            "start_utc": start_utc,
            "end_utc": end_utc,
            "description": f"Today EST {est_start.hour}:00-{est_end.hour}:00"
        })
    
    print(f"Testing {len(test_cases)} different time ranges...")
    print()
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"📅 TEST {i}:")
        is_match, metrics = await test_time_range(
            test_case["start_utc"], 
            test_case["end_utc"], 
            test_case["description"]
        )
        
        if is_match:
            print(f"\n🎯 FOUND CORRECT TIME RANGE!")
            print(f"   Use: {test_case['description']}")
            print(f"   UTC: {test_case['start_utc']} to {test_case['end_utc']}")
            return test_case, metrics
        
        print()
    
    print("❌ No time range matched the dashboard exactly.")
    return None, []

async def test_dashboard_filters():
    """Test if we need to add filters to match dashboard"""
    print(f"\n🔧 TESTING DASHBOARD FILTERS")
    print("=" * 60)
    
    # Test with different filters
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # Use the time range that gave us 88 calls (closest so far)
    start_utc = datetime(2025, 9, 11, 18, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2025, 9, 11, 20, 0, 0, tzinfo=timezone.utc)
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    filter_tests = [
        {
            "name": "No filters",
            "filters": []
        },
        {
            "name": "Exclude unknown publishers",
            "filters": [
                {
                    "column": "publisherName",
                    "operator": "NOT_EQUALS",
                    "value": ""
                }
            ]
        },
        {
            "name": "Exclude empty publisher names",
            "filters": [
                {
                    "column": "publisherName",
                    "operator": "NOT_EQUALS",
                    "value": "Unknown Publisher"
                }
            ]
        }
    ]
    
    for i, filter_test in enumerate(filter_tests, 1):
        print(f"📊 FILTER TEST {i}: {filter_test['name']}")
        print("-" * 40)
        
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
            "filters": filter_test["filters"],
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
                        
                        print(f"   Results: {len(metrics)} publishers")
                        print(f"   Total Calls: {total_calls}")
                        print(f"   Completed: {total_completed}")
                        print(f"   Total Payout: ${total_payout:.2f}")
                        
                        if total_calls == 87 and total_completed == 68 and abs(total_payout - 515) < 1:
                            print(f"   🎯 PERFECT MATCH!")
                            return filter_test, metrics
                        else:
                            print(f"   ❌ No match")
                            
                    else:
                        print(f"   ❌ API Error: {response.status}")
                        
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None, []

async def main():
    """Main function to find correct time range"""
    print("🚀 FINDING CORRECT TIME RANGE FOR DASHBOARD MATCH")
    print("=" * 80)
    
    # First, try to find the correct time range
    correct_time, correct_metrics = await find_correct_time_range()
    
    if correct_time:
        print(f"\n✅ FOUND CORRECT TIME RANGE!")
        print(f"   Use this configuration for accurate data.")
    else:
        print(f"\n❌ No correct time range found. Testing filters...")
        correct_filters, correct_metrics = await test_dashboard_filters()
        
        if correct_filters:
            print(f"\n✅ FOUND CORRECT FILTERS: {correct_filters['name']}")
        else:
            print(f"\n❌ No correct configuration found.")
            print(f"   The dashboard might be showing data from a different time period.")
            print(f"   Or there might be additional filters we're not aware of.")
    
    print(f"\n🏁 Search complete!")

if __name__ == "__main__":
    asyncio.run(main())
