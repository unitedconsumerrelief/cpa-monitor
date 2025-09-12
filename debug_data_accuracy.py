#!/usr/bin/env python3
"""Debug data accuracy by comparing API results with expected Ringba dashboard values"""

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

async def test_different_time_ranges():
    """Test different time ranges to find the correct one"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    # Test different time ranges
    test_cases = [
        {
            "name": "1pm-3pm EST (Current)",
            "start_utc": datetime(2025, 9, 11, 18, 0, 0, tzinfo=timezone.utc),  # 6pm UTC = 1pm EST
            "end_utc": datetime(2025, 9, 11, 20, 0, 0, tzinfo=timezone.utc),    # 8pm UTC = 3pm EST
        },
        {
            "name": "1pm-3pm EST (Alternative)",
            "start_utc": datetime(2025, 9, 11, 17, 0, 0, tzinfo=timezone.utc),  # 5pm UTC = 12pm EST
            "end_utc": datetime(2025, 9, 11, 19, 0, 0, tzinfo=timezone.utc),    # 7pm UTC = 2pm EST
        },
        {
            "name": "1pm-3pm EST (Another)",
            "start_utc": datetime(2025, 9, 11, 19, 0, 0, tzinfo=timezone.utc),  # 7pm UTC = 2pm EST
            "end_utc": datetime(2025, 9, 11, 21, 0, 0, tzinfo=timezone.utc),    # 9pm UTC = 4pm EST
        },
        {
            "name": "Today 1pm-3pm EST",
            "start_utc": datetime.now(timezone.utc).replace(hour=18, minute=0, second=0, microsecond=0),  # 6pm UTC = 1pm EST
            "end_utc": datetime.now(timezone.utc).replace(hour=20, minute=0, second=0, microsecond=0),    # 8pm UTC = 3pm EST
        }
    ]
    
    print("🔍 TESTING DIFFERENT TIME RANGES FOR DATA ACCURACY")
    print("=" * 80)
    print("Expected from Ringba Dashboard:")
    print("  Total Calls: 87")
    print("  Completed Calls: 68") 
    print("  Total Payout: $515")
    print("  Publishers: 10")
    print("=" * 80)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📅 TEST {i}: {test_case['name']}")
        print(f"   Time: {test_case['start_utc']} to {test_case['end_utc']} UTC")
        print("-" * 60)
        
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
            "generateRollups": True,
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
                        total_revenue = sum(m.revenue for m in metrics)
                        
                        print(f"   Results: {len(metrics)} publishers")
                        print(f"   Total Calls: {total_calls} (Expected: 87)")
                        print(f"   Completed: {total_completed} (Expected: 68)")
                        print(f"   Total Payout: ${total_payout:.2f} (Expected: $515)")
                        print(f"   Total Revenue: ${total_revenue:.2f}")
                        
                        # Check accuracy
                        calls_accuracy = "✅" if total_calls == 87 else "❌"
                        completed_accuracy = "✅" if total_completed == 68 else "❌"
                        payout_accuracy = "✅" if abs(total_payout - 515) < 1 else "❌"
                        
                        print(f"   Accuracy: Calls {calls_accuracy} | Completed {completed_accuracy} | Payout {payout_accuracy}")
                        
                        # Show top publishers
                        print(f"   Top Publishers:")
                        for j, metric in enumerate(metrics[:5]):
                            print(f"     {j+1}. {metric.publisher_name}: {metric.completed} completed, ${metric.payout:.2f} payout")
                        
                        if total_calls == 87 and total_completed == 68 and abs(total_payout - 515) < 1:
                            print(f"   🎯 PERFECT MATCH! This is the correct time range.")
                            return test_case, metrics
                            
                    else:
                        print(f"   ❌ API Error: {response.status}")
                        
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print(f"\n❌ No time range matched the expected dashboard values exactly.")
    return None, []

async def test_different_api_parameters():
    """Test different API parameters to see if that affects the data"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # Use the current time range
    start_utc = datetime(2025, 9, 11, 18, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2025, 9, 11, 20, 0, 0, tzinfo=timezone.utc)
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    # Test different parameter combinations
    test_cases = [
        {
            "name": "Current Parameters",
            "params": {
                "formatTimespans": True,
                "formatPercentages": True,
                "generateRollups": True,
                "formatTimeZone": "America/New_York"
            }
        },
        {
            "name": "No Formatting",
            "params": {
                "formatTimespans": False,
                "formatPercentages": False,
                "generateRollups": False,
                "formatTimeZone": "America/New_York"
            }
        },
        {
            "name": "UTC Timezone",
            "params": {
                "formatTimespans": True,
                "formatPercentages": True,
                "generateRollups": True,
                "formatTimeZone": "UTC"
            }
        }
    ]
    
    print(f"\n🔧 TESTING DIFFERENT API PARAMETERS")
    print("=" * 80)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📊 TEST {i}: {test_case['name']}")
        print("-" * 60)
        
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
            "maxResultsPerGroup": 1000,
            "filters": [],
            **test_case["params"]
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
                        
                        # Check if this matches dashboard
                        if total_calls == 87 and total_completed == 68 and abs(total_payout - 515) < 1:
                            print(f"   🎯 PERFECT MATCH! These parameters work.")
                            return test_case, metrics
                            
                    else:
                        print(f"   ❌ API Error: {response.status}")
                        
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None, []

async def main():
    """Main function to debug data accuracy"""
    print("🚀 STARTING DATA ACCURACY DEBUG SESSION")
    print("=" * 80)
    
    # Test different time ranges
    correct_time, correct_metrics = await test_different_time_ranges()
    
    if correct_time:
        print(f"\n✅ FOUND CORRECT TIME RANGE: {correct_time['name']}")
        print(f"   Use this time range for accurate data.")
    else:
        print(f"\n❌ No correct time range found. Testing different API parameters...")
        correct_params, correct_metrics = await test_different_api_parameters()
        
        if correct_params:
            print(f"\n✅ FOUND CORRECT PARAMETERS: {correct_params['name']}")
        else:
            print(f"\n❌ No correct parameters found. Need to investigate further.")
    
    print(f"\n🏁 Debug session complete!")

if __name__ == "__main__":
    asyncio.run(main())
