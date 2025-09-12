#!/usr/bin/env python3
"""Debug Ringba API to identify data accuracy issues"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RingbaAPIDebugger:
    def __init__(self):
        self.ringba_api_token = os.getenv("RINGBA_API_TOKEN")
        self.ringba_account_id = os.getenv("RINGBA_ACCOUNT_ID", "RA092c10a91f7c461098e354a1bbeda598")
        
        if not self.ringba_api_token:
            raise ValueError("RINGBA_API_TOKEN environment variable is required")

    async def test_api_call(self, start_time: datetime, end_time: datetime):
        """Test API call and return raw response for debugging"""
        url = f"https://api.ringba.com/v2/{self.ringba_account_id}/insights"
        
        headers = {
            "Authorization": f"Token {self.ringba_api_token}",
            "Content-Type": "application/json"
        }
        
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        payload = {
            "reportStart": start_str,
            "reportEnd": end_str,
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
        
        print(f"🔍 Testing Ringba API Call")
        print(f"📅 Time Range: {start_str} to {end_str}")
        print(f"🌐 URL: {url}")
        print(f"📋 Headers: {json.dumps(headers, indent=2)}")
        print(f"📦 Payload: {json.dumps(payload, indent=2)}")
        print("=" * 80)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    print(f"📊 Response Status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        print(f"✅ Success! Response structure:")
                        print(f"📋 Top-level keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                        
                        if "report" in data:
                            report = data["report"]
                            print(f"📊 Report keys: {list(report.keys()) if isinstance(report, dict) else 'Not a dict'}")
                            
                            if "records" in report:
                                records = report["records"]
                                print(f"📝 Number of records: {len(records) if isinstance(records, list) else 'Not a list'}")
                                
                                if records and len(records) > 0:
                                    print(f"\n🔍 First record structure:")
                                    first_record = records[0]
                                    print(f"📋 Record keys: {list(first_record.keys()) if isinstance(first_record, dict) else 'Not a dict'}")
                                    
                                    print(f"\n📊 Sample data from first record:")
                                    for key, value in first_record.items():
                                        print(f"  {key}: {value} (type: {type(value).__name__})")
                                    
                                    print(f"\n📈 All records summary:")
                                    for i, record in enumerate(records[:5]):  # Show first 5 records
                                        publisher = record.get("publisherName", "Unknown")
                                        calls = record.get("callCount", 0)
                                        completed = record.get("completedCalls", 0)
                                        payout = record.get("payoutAmount", 0)
                                        revenue = record.get("conversionAmount", 0)
                                        
                                        print(f"  {i+1}. {publisher}: {calls} calls, {completed} completed, ${payout:.2f} payout, ${revenue:.2f} revenue")
                                    
                                    if len(records) > 5:
                                        print(f"  ... and {len(records) - 5} more records")
                                
                                # Check for any data quality issues
                                print(f"\n🔍 Data Quality Analysis:")
                                publisher_names = [r.get("publisherName", "") for r in records if isinstance(r, dict)]
                                unique_publishers = set(publisher_names)
                                print(f"📊 Unique publishers: {len(unique_publishers)}")
                                print(f"📋 Publisher names: {sorted(unique_publishers)}")
                                
                                # Check for empty or problematic data
                                empty_publishers = [name for name in publisher_names if not name or name.strip() == ""]
                                if empty_publishers:
                                    print(f"⚠️  Empty publisher names: {len(empty_publishers)}")
                                
                                unknown_publishers = [name for name in publisher_names if name and "unknown" in name.lower()]
                                if unknown_publishers:
                                    print(f"⚠️  Unknown publishers: {unknown_publishers}")
                                
                                # Check numeric data consistency
                                total_calls = sum(r.get("callCount", 0) for r in records if isinstance(r, dict))
                                total_completed = sum(r.get("completedCalls", 0) for r in records if isinstance(r, dict))
                                total_payout = sum(r.get("payoutAmount", 0) for r in records if isinstance(r, dict))
                                total_revenue = sum(r.get("conversionAmount", 0) for r in records if isinstance(r, dict))
                                
                                print(f"📊 Totals across all publishers:")
                                print(f"  Total calls: {total_calls}")
                                print(f"  Total completed: {total_completed}")
                                print(f"  Total payout: ${total_payout:.2f}")
                                print(f"  Total revenue: ${total_revenue:.2f}")
                                
                                if total_completed > 0:
                                    avg_cpa = total_payout / total_completed
                                    print(f"  Average CPA: ${avg_cpa:.2f}")
                                
                            else:
                                print("❌ No 'records' found in report")
                        else:
                            print("❌ No 'report' found in response")
                            
                        return data
                    else:
                        error_text = await response.text()
                        print(f"❌ API Error {response.status}: {error_text}")
                        return None
                        
        except Exception as e:
            print(f"❌ Exception during API call: {e}")
            return None

    async def test_different_time_ranges(self):
        """Test API with different time ranges to see data consistency"""
        print("🕐 Testing different time ranges...")
        
        # Test last 2 hours
        now = datetime.now(timezone.utc)
        two_hours_ago = now - timedelta(hours=2)
        
        print(f"\n📅 Test 1: Last 2 hours ({two_hours_ago} to {now})")
        data1 = await self.test_api_call(two_hours_ago, now)
        
        # Test last 24 hours
        twenty_four_hours_ago = now - timedelta(hours=24)
        
        print(f"\n📅 Test 2: Last 24 hours ({twenty_four_hours_ago} to {now})")
        data2 = await self.test_api_call(twenty_four_hours_ago, now)
        
        # Test today EST
        est_now = now.astimezone(timezone(timedelta(hours=-5)))
        today_start_est = est_now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_est.astimezone(timezone.utc)
        
        print(f"\n📅 Test 3: Today EST ({today_start_utc} to {now})")
        data3 = await self.test_api_call(today_start_utc, now)
        
        return data1, data2, data3

async def main():
    """Main function to run API debugging"""
    try:
        debugger = RingbaAPIDebugger()
        
        print("🚀 Starting Ringba API Debug Session")
        print("=" * 80)
        
        # Test different time ranges
        data1, data2, data3 = await debugger.test_different_time_ranges()
        
        print("\n" + "=" * 80)
        print("🏁 Debug session complete!")
        
        if data1 or data2 or data3:
            print("✅ API calls successful - check the detailed output above for data accuracy issues")
        else:
            print("❌ All API calls failed - check your credentials and network connection")
            
    except Exception as e:
        print(f"❌ Failed to start debug session: {e}")

if __name__ == "__main__":
    asyncio.run(main())
