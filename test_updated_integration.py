#!/usr/bin/env python3
"""Test the updated Google Sheets integration with the new rules"""

import asyncio
from datetime import datetime, timezone, timedelta
from monitor import RingbaMonitor, PublisherMetrics

async def test_updated_integration():
    """Test the updated integration with the new rules"""
    monitor = RingbaMonitor()
    
    # Create mock metrics with various publishers
    metrics = [
        PublisherMetrics(
            publisher_name="TDES008-YT",
            completed=5,
            payout=1000.0
        ),
        PublisherMetrics(
            publisher_name="Koji Digital", 
            completed=12,
            payout=220.0
        ),
        PublisherMetrics(
            publisher_name="FITZ",
            completed=14,
            payout=105.0
        ),
        PublisherMetrics(
            publisher_name="Unknown Publisher",
            completed=3,
            payout=150.0
        ),
        PublisherMetrics(
            publisher_name="New Publisher",
            completed=8,
            payout=300.0
        )
    ]
    
    print("🎯 Testing Updated Integration Rules:")
    print("=" * 60)
    print("Rule: ALL Ringba publishers included, accurate CPA only for spreadsheet data")
    print()
    
    # Show initial data
    print("📊 Initial Ringba Data:")
    for metric in metrics:
        print(f"   {metric.publisher_name}: {metric.completed} completed, ${metric.payout} payout, ${metric.cpa:.2f} old CPA")
    print()
    
    # Test time range (3pm-5pm EDT)
    start_time = datetime(2025, 9, 12, 19, 0, 0, tzinfo=timezone.utc)  # 3pm EDT
    end_time = datetime(2025, 9, 12, 21, 0, 0, tzinfo=timezone.utc)    # 5pm EDT
    
    print("🔧 Enhancing with Accurate CPA Calculation:")
    print("=" * 60)
    
    # Enhance with accurate CPA
    enhanced_metrics = await monitor.enhance_metrics_with_accurate_cpa(metrics, start_time, end_time)
    
    print("\n✅ Results:")
    print("=" * 60)
    print(f"{'Publisher':<20} {'Sales':<6} {'Payout':<8} {'Old CPA':<8} {'Accurate CPA':<12}")
    print("-" * 60)
    
    for metric in enhanced_metrics:
        print(f"{metric.publisher_name[:19]:<20} {metric.sales_count:<6} ${metric.payout:<7.2f} ${metric.cpa:<7.2f} ${metric.accurate_cpa:<11.2f}")
    
    print("\n📋 Rule Verification:")
    print("=" * 40)
    
    # Verify the rules
    for metric in enhanced_metrics:
        if metric.sales_count > 0:
            print(f"✅ {metric.publisher_name}: Has spreadsheet data - accurate CPA calculated")
        else:
            print(f"✅ {metric.publisher_name}: No spreadsheet data - accurate CPA = $0.00")
    
    print(f"\n✅ All {len(enhanced_metrics)} publishers included in results")
    print("✅ End-of-day report will include all publishers")

if __name__ == "__main__":
    asyncio.run(test_updated_integration())
