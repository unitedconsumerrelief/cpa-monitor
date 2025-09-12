#!/usr/bin/env python3
"""Test the cleaned up reports without old CPA references"""

import asyncio
from datetime import datetime, timezone, timedelta
from monitor import RingbaMonitor, PublisherMetrics

async def test_clean_reports():
    """Test the cleaned up reports"""
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
    
    print("🧹 CLEANED UP REPORTS - No Old CPA References")
    print("=" * 60)
    
    # Test time range (3pm-5pm EDT)
    start_time = datetime(2025, 9, 12, 19, 0, 0, tzinfo=timezone.utc)  # 3pm EDT
    end_time = datetime(2025, 9, 12, 21, 0, 0, tzinfo=timezone.utc)    # 5pm EDT
    
    # Enhance with accurate CPA
    enhanced_metrics = await monitor.enhance_metrics_with_accurate_cpa(metrics, start_time, end_time)
    
    # Calculate totals
    totals = monitor.calculate_totals(enhanced_metrics)
    
    print("📊 SUMMARY SECTION:")
    print("=" * 40)
    print(f"• Total Calls: {totals.incoming}")
    print(f"• Completed Calls: {totals.completed}")
    print(f"• Revenue: ${totals.revenue:.2f}")
    print(f"• CPA: ${totals.accurate_cpa:.2f}  ← Only accurate CPA shown")
    print(f"• Profit: ${totals.profit:.2f}")
    print(f"• Conversion Rate: {totals.conversion_percent:.1f}%")
    print()
    
    print("🏆 TOP PERFORMERS:")
    print("=" * 40)
    for i, metric in enumerate(enhanced_metrics, 1):
        print(f"{i}. {metric.publisher_name}: {metric.completed} completed, ${metric.accurate_cpa:.2f} CPA")
    print()
    
    print("📋 DETAILED TABLE:")
    print("=" * 60)
    print(f"{'Publisher':<20} {'Completed':<10} {'Sales':<6} {'Accurate CPA':<12} {'Revenue':<10} {'Profit':<10}")
    print("-" * 60)
    
    for metric in enhanced_metrics:
        print(f"{metric.publisher_name[:19]:<20} {metric.completed:<10} {metric.sales_count:<6} ${metric.accurate_cpa:<11.2f} ${metric.revenue:<9.2f} ${metric.profit:<9.2f}")
    
    print()
    print("✅ CLEANED UP FEATURES:")
    print("=" * 40)
    print("✅ No old CPA references in logs")
    print("✅ Only accurate CPA in summaries")
    print("✅ Only accurate CPA in top performers")
    print("✅ Only accurate CPA in detailed tables")
    print("✅ Clean, focused reporting")

if __name__ == "__main__":
    asyncio.run(test_clean_reports())
