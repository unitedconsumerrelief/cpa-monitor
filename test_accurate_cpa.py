#!/usr/bin/env python3
"""Test script for accurate CPA calculation"""

import asyncio
from datetime import datetime, timezone, timedelta
from monitor import RingbaMonitor, PublisherMetrics

async def test_accurate_cpa():
    """Test the accurate CPA calculation"""
    monitor = RingbaMonitor()
    
    # Create mock metrics
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
        )
    ]
    
    # Calculate old CPA
    for metric in metrics:
        print(f"📊 {metric.publisher_name}:")
        print(f"   Completed: {metric.completed}")
        print(f"   Payout: ${metric.payout}")
        print(f"   Old CPA: ${metric.cpa:.2f}")
        print()
    
    # Test time range (3pm-5pm EDT)
    start_time = datetime(2025, 9, 12, 19, 0, 0, tzinfo=timezone.utc)  # 3pm EDT
    end_time = datetime(2025, 9, 12, 21, 0, 0, tzinfo=timezone.utc)    # 5pm EDT
    
    print("🎯 Testing Accurate CPA Calculation:")
    print("=" * 50)
    
    # Enhance with accurate CPA
    enhanced_metrics = await monitor.enhance_metrics_with_accurate_cpa(metrics, start_time, end_time)
    
    print("\n✅ Results:")
    print("=" * 50)
    for metric in enhanced_metrics:
        print(f"📊 {metric.publisher_name}:")
        print(f"   Sales Count: {metric.sales_count}")
        print(f"   Payout: ${metric.payout}")
        print(f"   Accurate CPA: ${metric.accurate_cpa:.2f}")
        print(f"   Old CPA: ${metric.cpa:.2f}")
        print(f"   Difference: ${abs(metric.accurate_cpa - metric.cpa):.2f}")
        print()

if __name__ == "__main__":
    asyncio.run(test_accurate_cpa())
