import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def test_11am_1pm():
    """Test fetching 11am-1pm EST data right now"""
    
    # API credentials
    api_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    account_id = "RA092c10a91f7c461098e354a1bbeda598"
    
    # 11am-1pm EST today (EST is UTC-5, so 11am EST = 4pm UTC, 1pm EST = 6pm UTC)
    today = datetime.now(timezone.utc).date()
    start_time = datetime.combine(today, datetime.min.time().replace(hour=16)).replace(tzinfo=timezone.utc)  # 11am EST
    end_time = datetime.combine(today, datetime.min.time().replace(hour=18)).replace(tzinfo=timezone.utc)    # 1pm EST
    
    url = f"https://api.ringba.com/v2/{account_id}/insights"
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "reportStart": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportEnd": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
        "valueColumns": [
            {"column": "callCount", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "convertedCalls", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None},
            {"column": "profitGross", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "generateRollups": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print(f"🔍 Testing 11am-1pm EST data...")
    print(f"⏰ Time range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"⏰ EST equivalent: 11:00 AM to 1:00 PM EST")
    print("-" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                print(f"📡 API Response Status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    print("✅ SUCCESS! Data fetched successfully!")
                    print()
                    
                    if "data" in data and "rows" in data["data"]:
                        rows = data["data"]["rows"]
                        print(f"📊 Found {len(rows)} publishers with data:")
                        print()
                        
                        total_calls = 0
                        total_completed = 0
                        total_revenue = 0
                        total_payout = 0
                        
                        for i, row in enumerate(rows, 1):
                            publisher = row.get("publisherName", "Unknown")
                            calls = row.get("callCount", 0)
                            completed = row.get("completedCalls", 0)
                            converted = row.get("convertedCalls", 0)
                            revenue = row.get("conversionAmount", 0)
                            payout = row.get("payoutAmount", 0)
                            profit = row.get("profitGross", 0)
                            
                            cpa = revenue / completed if completed > 0 else 0
                            
                            print(f"  {i:2d}. {publisher:<20} | Calls: {calls:3d} | Completed: {completed:3d} | Converted: {converted:2d} | Revenue: ${revenue:6.2f} | CPA: ${cpa:6.2f}")
                            
                            total_calls += calls
                            total_completed += completed
                            total_revenue += revenue
                            total_payout += payout
                        
                        print()
                        print("📈 TOTALS:")
                        print(f"  Total Calls: {total_calls}")
                        print(f"  Completed: {total_completed}")
                        print(f"  Revenue: ${total_revenue:.2f}")
                        print(f"  Payout: ${total_payout:.2f}")
                        print(f"  Overall CPA: ${total_revenue/total_completed:.2f}" if total_completed > 0 else "  Overall CPA: $0.00")
                        
                    else:
                        print("❌ No data found in response")
                        print(f"Response structure: {list(data.keys())}")
                        
                else:
                    error_text = await response.text()
                    print(f"❌ API Error {response.status}: {error_text}")
                    
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    asyncio.run(test_11am_1pm())
