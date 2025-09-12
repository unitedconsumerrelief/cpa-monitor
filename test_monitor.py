#!/usr/bin/env python3
"""
Test script for Ringba monitoring system
"""
import asyncio
import os
from datetime import datetime, timezone, timedelta
from monitor import RingbaMonitor

async def test_monitor():
    """Test the monitoring system with sample data"""
    
    # Set up test environment variables with NEW API token
    os.environ["RINGBA_API_TOKEN"] = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    os.environ["SLACK_WEBHOOK_URL"] = "https://hooks.slack.com/services/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"
    
    try:
        monitor = RingbaMonitor()
        
        # Test with 12pm-2pm EST (last 2 hours from current time 2:04pm EST)
        # Current time is 2:04pm EST, so last 2 hours is 12pm-2pm EST
        est_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
        end_time_est = est_now
        start_time_est = est_now - timedelta(hours=2)
        
        # Convert to UTC for API call
        start_time = start_time_est.astimezone(timezone.utc)
        end_time = end_time_est.astimezone(timezone.utc)
        
        print(f"Testing monitoring for {start_time} to {end_time}")
        
        # Test data fetching with real API token
        print("Testing Ringba API call...")
        print(f"Requesting time range: {start_time} to {end_time}")
        print(f"EST equivalent: {start_time.astimezone(timezone(timedelta(hours=-5)))} to {end_time.astimezone(timezone(timedelta(hours=-5)))}")
        
        metrics = await monitor.fetch_ringba_data(start_time, end_time)
        
        if metrics:
            print(f"✅ SUCCESS! Fetched {len(metrics)} publisher metrics:")
            print()
            
            total_calls = 0
            total_completed = 0
            total_revenue = 0
            
            for i, metric in enumerate(metrics, 1):
                print(f"  {i:2d}. {metric.publisher_name:<20} | Calls: {metric.incoming:3d} | Completed: {metric.completed:3d} | Revenue: ${metric.revenue:6.2f} | CPA: ${metric.cpa:6.2f}")
                total_calls += metric.incoming
                total_completed += metric.completed
                total_revenue += metric.revenue
            
            print()
            print(f"📊 TOTALS: {total_calls} calls, {total_completed} completed, ${total_revenue:.2f} revenue")
            print(f"💰 Overall CPA: ${total_revenue/total_completed:.2f}" if total_completed > 0 else "💰 Overall CPA: $0.00")
        else:
            print("❌ No metrics fetched from Ringba")
        
        # Test Slack message formatting with sample data
        print("Testing Slack message formatting...")
        from monitor import PublisherMetrics
        
        # Create sample data
        sample_metrics = [
            PublisherMetrics(
                publisher_name="TDES023-YT",
                incoming=5,
                completed=4,
                converted=1,
                revenue=100.0,
                payout=80.0,
                profit=20.0,
                tcl_seconds=1155,  # 19:15
                acl_seconds=288    # 4:48
            ),
            PublisherMetrics(
                publisher_name="FITZ",
                incoming=4,
                completed=2,
                converted=0,
                revenue=0.0,
                payout=0.0,
                profit=0.0,
                tcl_seconds=166,   # 2:46
                acl_seconds=83     # 1:23
            ),
            PublisherMetrics(
                publisher_name="Koji Digital",
                incoming=4,
                completed=2,
                converted=0,
                revenue=0.0,
                payout=0.0,
                profit=0.0,
                tcl_seconds=316,   # 5:16
                acl_seconds=158    # 2:38
            )
        ]
        
        # Test Slack summary (won't actually send without real webhook)
        print("Testing Slack summary generation...")
        # Create sample daily metrics (same as 2-hour for testing)
        sample_daily_metrics = sample_metrics.copy()
        await monitor.send_slack_summary(sample_metrics, sample_daily_metrics, start_time, end_time, start_time)
        
        print("✅ All tests completed successfully!")
        print("\n📋 Sample CPA calculations:")
        for metric in sample_metrics:
            print(f"  {metric.publisher_name}: ${metric.cpa:.2f} CPA")
        
        # Test business hours logic
        print("\n🕐 Testing business hours scheduling:")
        from datetime import datetime, timezone, timedelta
        
        # Test current time
        now = datetime.now(timezone.utc)
        is_business = monitor.is_business_hours(now)
        est_time = now.astimezone(timezone(timedelta(hours=-5)))
        print(f"Current time: {est_time.strftime('%Y-%m-%d %H:%M:%S')} EST")
        print(f"Is business hours: {is_business}")
        
        # Test next business hour
        next_run = monitor.get_next_business_hour()
        next_est = next_run.astimezone(timezone(timedelta(hours=-5)))
        print(f"Next monitoring cycle: {next_est.strftime('%Y-%m-%d %H:%M:%S')} EST")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_monitor())
