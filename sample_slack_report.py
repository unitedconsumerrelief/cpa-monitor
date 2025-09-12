#!/usr/bin/env python3
"""Generate a sample Slack report to show the final format"""

import asyncio
from datetime import datetime, timezone, timedelta
from monitor import RingbaMonitor, PublisherMetrics

async def generate_sample_report():
    """Generate a sample Slack report"""
    monitor = RingbaMonitor()
    
    # Create realistic sample data
    metrics_2hour = [
        PublisherMetrics(
            publisher_name="TDES008-YT",
            incoming=8,
            completed=5,
            connected=4,
            paid=1,
            converted=1,
            revenue=0.0,
            payout=1000.0,
            profit=-1000.0,
            conversion_percent=12.5,
            tcl_seconds=1200,
            acl_seconds=300
        ),
        PublisherMetrics(
            publisher_name="Koji Digital",
            incoming=19,
            completed=12,
            connected=11,
            paid=4,
            converted=4,
            revenue=0.0,
            payout=220.0,
            profit=-220.0,
            conversion_percent=21.05,
            tcl_seconds=4487,
            acl_seconds=236
        ),
        PublisherMetrics(
            publisher_name="FITZ",
            incoming=14,
            completed=14,
            connected=14,
            paid=3,
            converted=3,
            revenue=0.0,
            payout=105.0,
            profit=-105.0,
            conversion_percent=21.43,
            tcl_seconds=3459,
            acl_seconds=247
        ),
        PublisherMetrics(
            publisher_name="TDES023-YT",
            incoming=16,
            completed=16,
            connected=12,
            paid=0,
            converted=0,
            revenue=0.0,
            payout=0.0,
            profit=0.0,
            conversion_percent=0.0,
            tcl_seconds=0,
            acl_seconds=0
        ),
        PublisherMetrics(
            publisher_name="LeadVIPs",
            incoming=3,
            completed=2,
            connected=2,
            paid=0,
            converted=0,
            revenue=0.0,
            payout=0.0,
            profit=0.0,
            conversion_percent=0.0,
            tcl_seconds=0,
            acl_seconds=0
        )
    ]
    
    # Create daily metrics (same publishers but with more data)
    metrics_daily = [
        PublisherMetrics(
            publisher_name="TDES008-YT",
            incoming=25,
            completed=18,
            connected=15,
            paid=2,
            converted=2,
            revenue=0.0,
            payout=2000.0,
            profit=-2000.0,
            conversion_percent=8.0,
            tcl_seconds=3600,
            acl_seconds=200
        ),
        PublisherMetrics(
            publisher_name="Koji Digital",
            incoming=45,
            completed=28,
            connected=25,
            paid=8,
            converted=8,
            revenue=0.0,
            payout=500.0,
            profit=-500.0,
            conversion_percent=17.78,
            tcl_seconds=8400,
            acl_seconds=300
        ),
        PublisherMetrics(
            publisher_name="FITZ",
            incoming=32,
            completed=28,
            connected=28,
            paid=6,
            converted=6,
            revenue=0.0,
            payout=210.0,
            profit=-210.0,
            conversion_percent=18.75,
            tcl_seconds=7200,
            acl_seconds=257
        ),
        PublisherMetrics(
            publisher_name="TDES023-YT",
            incoming=40,
            completed=32,
            connected=25,
            paid=0,
            converted=0,
            revenue=0.0,
            payout=0.0,
            profit=0.0,
            conversion_percent=0.0,
            tcl_seconds=0,
            acl_seconds=0
        ),
        PublisherMetrics(
            publisher_name="LeadVIPs",
            incoming=8,
            completed=6,
            connected=5,
            paid=0,
            converted=0,
            revenue=0.0,
            payout=0.0,
            profit=0.0,
            conversion_percent=0.0,
            tcl_seconds=0,
            acl_seconds=0
        )
    ]
    
    # Simulate time ranges
    start_time = datetime(2025, 9, 12, 19, 0, 0, tzinfo=timezone.utc)  # 3pm EDT
    end_time = datetime(2025, 9, 12, 21, 0, 0, tzinfo=timezone.utc)    # 5pm EDT
    daily_start_time = datetime(2025, 9, 12, 13, 0, 0, tzinfo=timezone.utc)  # 9am EDT
    
    print("📱 SAMPLE SLACK REPORT")
    print("=" * 80)
    print("🔄 RESEND: Ringba Performance Summary - 2025-09-12 15:00 - 2025-09-12 17:00 EDT")
    print()
    
    # Simulate the accurate CPA enhancement
    print("🔧 Enhancing with Accurate CPA from Google Sheets...")
    
    # Simulate sales data from spreadsheet
    sales_data_2hour = {
        "TDES008-YT": 1,  # Has 1 sale in 3pm-5pm window
        "Koji Digital": 0,  # No sales in this window
        "FITZ": 0,  # No sales in this window
        "TDES023-YT": 0,  # No sales in this window
        "LeadVIPs": 0  # No sales in this window
    }
    
    sales_data_daily = {
        "TDES008-YT": 2,  # Has 2 sales total for the day
        "Koji Digital": 1,  # Has 1 sale total for the day
        "FITZ": 0,  # No sales for the day
        "TDES023-YT": 0,  # No sales for the day
        "LeadVIPs": 0  # No sales for the day
    }
    
    # Apply sales data to metrics
    for metric in metrics_2hour:
        metric.sales_count = sales_data_2hour.get(metric.publisher_name, 0)
        if metric.sales_count > 0:
            metric.accurate_cpa = metric.payout / metric.sales_count
        else:
            metric.accurate_cpa = 0.0
    
    for metric in metrics_daily:
        metric.sales_count = sales_data_daily.get(metric.publisher_name, 0)
        if metric.sales_count > 0:
            metric.accurate_cpa = metric.payout / metric.sales_count
        else:
            metric.accurate_cpa = 0.0
    
    # Calculate totals
    totals_2hour = monitor.calculate_totals(metrics_2hour)
    totals_daily = monitor.calculate_totals(metrics_daily)
    
    print("✅ Accurate CPA calculated for all publishers")
    print()
    
    # Display the report sections
    print("📊 SUMMARY SECTION:")
    print("=" * 50)
    print(f"*Last 2 Hours (2025-09-12 15:00 - 2025-09-12 17:00 EDT)*")
    print(f"• *Completed Calls:* {totals_2hour.completed:,}")
    print(f"• *Revenue:* ${totals_2hour.revenue:,.2f}")
    print(f"• *CPA:* ${totals_2hour.accurate_cpa:.2f}")
    print(f"• *Profit:* ${totals_2hour.profit:,.2f}")
    print(f"• *Conversion Rate:* {totals_2hour.conversion_percent:.1f}%")
    print()
    print(f"*Daily Total (2025-09-12 09:00 - 2025-09-12 17:00 EDT)*")
    print(f"• *Completed Calls:* {totals_daily.completed:,}")
    print(f"• *Revenue:* ${totals_daily.revenue:,.2f}")
    print(f"• *CPA:* ${totals_daily.accurate_cpa:.2f}")
    print(f"• *Profit:* ${totals_daily.profit:,.2f}")
    print(f"• *Conversion Rate:* {totals_daily.conversion_percent:.1f}%")
    print()
    
    print("🏆 TOP PERFORMERS:")
    print("=" * 50)
    print("*🏆 Top 5 Publishers by Completed Calls (Last 2 Hours):*")
    for i, metric in enumerate(sorted(metrics_2hour, key=lambda x: x.completed, reverse=True)[:5], 1):
        print(f"{i}. *{metric.publisher_name}*: {metric.completed} completed, ${metric.accurate_cpa:.2f} CPA")
    print()
    
    print("📋 DETAILED TABLES:")
    print("=" * 50)
    print("*📋 ALL Publishers Performance (Last 2 Hours):*")
    print("```")
    print(f"{'Publisher':<20} {'Completed':<10} {'Sales':<6} {'Accurate CPA':<12} {'Revenue':<10} {'Profit':<10}")
    print("-" * 70)
    for metric in sorted(metrics_2hour, key=lambda x: x.completed, reverse=True):
        print(f"{metric.publisher_name[:19]:<20} {metric.completed:<10} {metric.sales_count:<6} ${metric.accurate_cpa:<11.2f} ${metric.revenue:<9.2f} ${metric.profit:<9.2f}")
    print("```")
    print()
    
    print("*📋 ALL Publishers Performance (Daily Accumulated):*")
    print("```")
    print(f"{'Publisher':<20} {'Completed':<10} {'Sales':<6} {'Accurate CPA':<12} {'Revenue':<10} {'Profit':<10}")
    print("-" * 70)
    for metric in sorted(metrics_daily, key=lambda x: x.completed, reverse=True):
        print(f"{metric.publisher_name[:19]:<20} {metric.completed:<10} {metric.sales_count:<6} ${metric.accurate_cpa:<11.2f} ${metric.revenue:<9.2f} ${metric.profit:<9.2f}")
    print("```")
    print()
    
    print("🔄 RESEND CONFIRMATION:")
    print("=" * 50)
    print("• This is a resend of the 3pm-5pm EDT report that failed earlier")
    print("• Webhook issue has been fixed")
    print("• All publisher data included (no missing records)")
    print("• CPA calculations are accurate based on actual sales data")
    print("• Timezone: Eastern Daylight Time (EDT) - UTC-4")
    print()
    
    print("✅ SAMPLE REPORT COMPLETE")
    print("=" * 50)
    print("This is exactly how your Slack reports will look!")
    print("• Clean, professional format")
    print("• Only accurate CPA (no old CPA clutter)")
    print("• All publishers included")
    print("• Real sales data from Google Sheets")
    print("• Ready for deployment! 🚀")

if __name__ == "__main__":
    asyncio.run(generate_sample_report())
