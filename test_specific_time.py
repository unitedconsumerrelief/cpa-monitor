#!/usr/bin/env python3
"""
Test script to fetch data for a specific time range (11am-1pm EST)
"""
import asyncio
import os
from datetime import datetime, timezone, timedelta
from monitor import RingbaMonitor

async def test_specific_time():
    """Test fetching data for 11am-1pm EST"""
    
    # Set up environment variables (same as Render)
    os.environ["RINGBA_API_TOKEN"] = "09f0c9f035a894013c44904a9557433e3b41073ce0ee7dee632f6eb495bb9e26da9f78e078a9f83b0d85839dbe52314f14a5b26d69012061cQd26f9ac7Qc17ab00446bcceb5a3ab5417012471149066420910f98a12262.56662820bbf"
    os.environ["SLACK_WEBHOOK_URL"] = "https://hooks.slack.com/services/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"
    os.environ["RINGBA_ACCOUNT_ID"] = "RA092c10a91f7c461098e354a1bbeda598"
    
    try:
        monitor = RingbaMonitor()
        
        # Test with 11am-1pm EST today
        # Convert 11am EST to UTC (EST is UTC-5, so 11am EST = 4pm UTC)
        est_11am = datetime.now(timezone.utc).replace(hour=16, minute=0, second=0, microsecond=0)  # 11am EST = 4pm UTC
        est_1pm = datetime.now(timezone.utc).replace(hour=18, minute=0, second=0, microsecond=0)    # 1pm EST = 6pm UTC
        
        print(f"Testing data fetch for 11am-1pm EST today")
        print(f"UTC time range: {est_11am} to {est_1pm}")
        print(f"EST time range: {est_11am.astimezone(timezone(timedelta(hours=-5)))} to {est_1pm.astimezone(timezone(timedelta(hours=-5)))}")
        print("-" * 60)
        
        # Fetch data
        metrics = await monitor.fetch_ringba_data(est_11am, est_1pm)
        
        if metrics:
            print(f"✅ SUCCESS! Found {len(metrics)} publishers with data:")
            print()
            
            # Calculate totals
            totals = monitor.calculate_totals(metrics)
            
            print("📊 SUMMARY:")
            print(f"  Total Calls: {totals.incoming}")
            print(f"  Completed: {totals.completed}")
            print(f"  Converted: {totals.converted}")
            print(f"  Conversion Rate: {totals.conversion_percent:.2f}%")
            print(f"  Revenue: ${totals.revenue:.2f}")
            print(f"  Payout: ${totals.payout:.2f}")
            print(f"  Profit: ${totals.profit:.2f}")
            print(f"  CPA: ${totals.cpa:.2f}")
            print()
            
            print("📋 PUBLISHER BREAKDOWN:")
            for i, metric in enumerate(metrics, 1):
                print(f"  {i:2d}. {metric.publisher_name:<20} | Calls: {metric.incoming:3d} | Completed: {metric.completed:3d} | CPA: ${metric.cpa:6.2f} | Revenue: ${metric.revenue:6.2f}")
            
            print()
            print("✅ Data is available! The monitoring system should work correctly.")
            
        else:
            print("❌ No data found for 11am-1pm EST")
            print("This could mean:")
            print("  - No calls in that time period")
            print("  - API token issue")
            print("  - Time zone mismatch")
            print("  - Ringba API issue")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_specific_time())
