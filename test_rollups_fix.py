#!/usr/bin/env python3
"""Test if generateRollups is causing data duplication"""

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

async def test_rollups_parameter():
    """Test with and without generateRollups"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    # 1pm-3pm EST for 09/11/2025
    start_utc = datetime(2025, 9, 11, 18, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2025, 9, 11, 20, 0, 0, tzinfo=timezone.utc)
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    test_cases = [
        {
            "name": "WITH generateRollups: true",
            "generateRollups": True
        },
        {
            "name": "WITHOUT generateRollups (false)",
            "generateRollups": False
        },
        {
            "name": "WITHOUT generateRollups (omitted)",
            "generateRollups": None
        }
    ]
    
    print("🔍 TESTING generateRollups PARAMETER")
    print("=" * 60)
    print("Expected from Ringba Dashboard:")
    print("  Total Calls: 87")
    print("  Completed Calls: 68")
    print("  Total Payout: $515")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📊 TEST {i}: {test_case['name']}")
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
            "filters": [],
            "formatTimeZone": "America/New_York"
        }
        
        # Add generateRollups if specified
        if test_case["generateRollups"] is not None:
            payload["generateRollups"] = test_case["generateRollups"]
        
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
                        print(f"   Total Calls: {total_calls}")
                        print(f"   Completed: {total_completed}")
                        print(f"   Total Payout: ${total_payout:.2f}")
                        print(f"   Total Revenue: ${total_revenue:.2f}")
                        
                        # Check accuracy
                        calls_accuracy = "✅" if total_calls == 87 else "❌"
                        completed_accuracy = "✅" if total_completed == 68 else "❌"
                        payout_accuracy = "✅" if abs(total_payout - 515) < 1 else "❌"
                        
                        print(f"   Accuracy: Calls {calls_accuracy} | Completed {completed_accuracy} | Payout {payout_accuracy}")
                        
                        # Show detailed breakdown
                        print(f"   Publisher Breakdown:")
                        for j, metric in enumerate(metrics):
                            print(f"     {j+1:2d}. {metric.publisher_name}: {metric.completed} completed, ${metric.payout:.2f} payout")
                        
                        if total_calls == 87 and total_completed == 68 and abs(total_payout - 515) < 1:
                            print(f"   🎯 PERFECT MATCH! This configuration works.")
                            return test_case, metrics
                            
                    else:
                        print(f"   ❌ API Error: {response.status}")
                        error_text = await response.text()
                        print(f"   Error: {error_text}")
                        
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print(f"\n❌ No configuration matched the expected dashboard values exactly.")
    return None, []

async def test_different_aggregate_functions():
    """Test different aggregate functions"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    start_utc = datetime(2025, 9, 11, 18, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2025, 9, 11, 20, 0, 0, tzinfo=timezone.utc)
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    test_cases = [
        {
            "name": "No aggregate functions",
            "valueColumns": [
                {"column": "callCount"},
                {"column": "completedCalls"},
                {"column": "payoutAmount"},
                {"column": "conversionAmount"}
            ]
        },
        {
            "name": "SUM aggregate functions",
            "valueColumns": [
                {"column": "callCount", "aggregateFunction": "SUM"},
                {"column": "completedCalls", "aggregateFunction": "SUM"},
                {"column": "payoutAmount", "aggregateFunction": "SUM"},
                {"column": "conversionAmount", "aggregateFunction": "SUM"}
            ]
        }
    ]
    
    print(f"\n🔧 TESTING DIFFERENT AGGREGATE FUNCTIONS")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📊 TEST {i}: {test_case['name']}")
        print("-" * 40)
        
        payload = {
            "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
            "valueColumns": test_case["valueColumns"],
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
                        
                        print(f"   Results: {len(metrics)} publishers")
                        print(f"   Total Calls: {total_calls}")
                        print(f"   Completed: {total_completed}")
                        print(f"   Total Payout: ${total_payout:.2f}")
                        
                        # Check accuracy
                        if total_calls == 87 and total_completed == 68 and abs(total_payout - 515) < 1:
                            print(f"   🎯 PERFECT MATCH! This configuration works.")
                            return test_case, metrics
                            
                    else:
                        print(f"   ❌ API Error: {response.status}")
                        
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None, []

async def main():
    """Main function to test rollups fix"""
    print("🚀 TESTING ROLLUPS FIX")
    print("=" * 60)
    
    # Test rollups parameter
    correct_config, correct_metrics = await test_rollups_parameter()
    
    if correct_config:
        print(f"\n✅ FOUND CORRECT CONFIG: {correct_config['name']}")
    else:
        print(f"\n❌ Rollups test didn't work. Testing aggregate functions...")
        correct_config, correct_metrics = await test_different_aggregate_functions()
        
        if correct_config:
            print(f"\n✅ FOUND CORRECT CONFIG: {correct_config['name']}")
        else:
            print(f"\n❌ No correct configuration found.")
    
    print(f"\n🏁 Test complete!")

if __name__ == "__main__":
    asyncio.run(main())
