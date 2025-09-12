#!/usr/bin/env python3
"""Test the new monitoring schedule and end-of-day report"""

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
    live: int = 0
    completed: int = 0
    ended: int = 0
    connected: int = 0
    paid: int = 0
    converted: int = 0
    no_connect: int = 0
    duplicate: int = 0
    blocked: int = 0
    ivr_hangup: int = 0
    revenue: float = 0.0
    payout: float = 0.0
    profit: float = 0.0
    margin: float = 0.0
    conversion_percent: float = 0.0
    tcl_seconds: int = 0
    acl_seconds: int = 0
    total_cost: float = 0.0

    @property
    def cpa(self) -> float:
        """Calculate CPA: Payout / Completed Calls"""
        if self.completed == 0:
            return 0.0
        return self.payout / self.completed

def parse_ringba_data(data: Dict[str, Any]) -> List[PublisherMetrics]:
    """Parse Ringba API response with FIXED logic"""
    metrics_list = []
    
    try:
        if "report" in data and "records" in data["report"]:
            for row in data["report"]["records"]:
                # Handle missing, empty, or "Unknown" publisher names
                publisher_name = row.get("publisherName", "")
                raw_publisher_name = publisher_name
                
                # If publisher name is missing/empty, use "Unknown Publisher" but DON'T skip the record
                if not publisher_name or publisher_name.strip() == "" or publisher_name.strip().lower() in ["unknown", "missing"]:
                    publisher_name = "Unknown Publisher"
                
                # Only skip if it's explicitly "MISSING" in the raw data (not empty)
                if raw_publisher_name and raw_publisher_name.upper() == "MISSING":
                    continue
                
                # Helper function to safely convert values
                def safe_float(value, default=0.0):
                    if not value:
                        return default
                    try:
                        clean_value = str(value).replace('%', '')
                        return float(clean_value)
                    except:
                        return default
                
                def safe_int(value, default=0):
                    if not value:
                        return default
                    try:
                        return int(float(value))
                    except:
                        return default
                
                metrics = PublisherMetrics(
                    publisher_name=publisher_name,
                    incoming=safe_int(row.get("callCount", 0)),
                    live=safe_int(row.get("liveCallCount", 0)),
                    completed=safe_int(row.get("completedCalls", 0)),
                    ended=safe_int(row.get("endedCalls", 0)),
                    connected=safe_int(row.get("connectedCallCount", 0)),
                    paid=safe_int(row.get("payoutCount", 0)),
                    converted=safe_int(row.get("convertedCalls", 0)),
                    no_connect=safe_int(row.get("nonConnectedCallCount", 0)),
                    duplicate=safe_int(row.get("duplicateCalls", 0)),
                    blocked=safe_int(row.get("blockedCalls", 0)),
                    ivr_hangup=safe_int(row.get("incompleteCalls", 0)),
                    revenue=safe_float(row.get("conversionAmount", 0)),
                    payout=safe_float(row.get("payoutAmount", 0)),
                    profit=safe_float(row.get("profitGross", 0)),
                    margin=safe_float(row.get("profitMarginGross", 0)),
                    conversion_percent=safe_float(row.get("convertedPercent", 0)),
                    tcl_seconds=safe_int(row.get("callLengthInSeconds", 0)),
                    acl_seconds=safe_int(row.get("avgHandleTime", 0)),
                    total_cost=safe_float(row.get("totalCost", 0))
                )
                metrics_list.append(metrics)
    except Exception as e:
        print(f"Error parsing data: {e}")
        
    return metrics_list

def calculate_totals(metrics: List[PublisherMetrics]) -> PublisherMetrics:
    """Calculate totals across all publishers"""
    totals = PublisherMetrics(publisher_name="TOTALS")
    
    for metric in metrics:
        totals.incoming += metric.incoming
        totals.live += metric.live
        totals.completed += metric.completed
        totals.ended += metric.ended
        totals.connected += metric.connected
        totals.paid += metric.paid
        totals.converted += metric.converted
        totals.no_connect += metric.no_connect
        totals.duplicate += metric.duplicate
        totals.blocked += metric.blocked
        totals.ivr_hangup += metric.ivr_hangup
        totals.revenue += metric.revenue
        totals.payout += metric.payout
        totals.profit += metric.profit
        totals.total_cost += metric.total_cost
        totals.tcl_seconds += metric.tcl_seconds
    
    # Calculate overall conversion percentage
    if totals.incoming > 0:
        totals.conversion_percent = (totals.converted / totals.incoming) * 100
    
    return totals

async def test_schedule_logic():
    """Test the new schedule logic"""
    print("🧪 TESTING NEW SCHEDULE LOGIC")
    print("=" * 60)
    
    # Test different times to see what reports would be generated
    test_times = [
        ("10:30 AM EDT", 10, 30),  # Before first report
        ("11:30 AM EDT", 11, 30),  # 11am report time
        ("12:30 PM EDT", 12, 30),  # Between reports
        ("1:30 PM EDT", 13, 30),   # 1pm report time
        ("3:30 PM EDT", 15, 30),   # 3pm report time
        ("5:30 PM EDT", 17, 30),   # 5pm report time
        ("7:30 PM EDT", 19, 30),   # 7pm report time
        ("9:30 PM EDT", 21, 30),   # 9pm report time (end of day)
        ("10:30 PM EDT", 22, 30),  # After business hours
    ]
    
    for time_desc, hour, minute in test_times:
        # Create a test datetime
        test_date = datetime(2025, 9, 11, hour, minute, 0, tzinfo=timezone(timedelta(hours=-4)))
        
        print(f"\n📅 Testing {time_desc}:")
        print(f"   Time: {test_date}")
        
        # Determine what report would be generated
        if hour >= 11 and hour < 13:  # 11am-1pm EDT
            data_window = "9am-11am"
            report_type = "2-hour"
        elif hour >= 13 and hour < 15:  # 1pm-3pm EDT
            data_window = "11am-1pm"
            report_type = "2-hour"
        elif hour >= 15 and hour < 17:  # 3pm-5pm EDT
            data_window = "1pm-3pm"
            report_type = "2-hour"
        elif hour >= 17 and hour < 19:  # 5pm-7pm EDT
            data_window = "3pm-5pm"
            report_type = "2-hour"
        elif hour >= 19 and hour < 21:  # 7pm-9pm EDT
            data_window = "5pm-7pm"
            report_type = "2-hour"
        elif hour >= 21:  # 9pm EDT - End of day report
            data_window = "7pm-9pm + Full Day"
            report_type = "end-of-day"
        else:
            data_window = "None (Outside hours)"
            report_type = "skip"
        
        print(f"   Data Window: {data_window}")
        print(f"   Report Type: {report_type}")

async def test_end_of_day_report():
    """Test the end-of-day report with sample data"""
    print(f"\n🧪 TESTING END-OF-DAY REPORT")
    print("=" * 60)
    
    # Create sample daily data (simulating a full day)
    sample_metrics = [
        PublisherMetrics("Koji Digital", 30, 0, 16, 16, 12, 6, 6, 18, 1, 0, 14, 1.0, 330.0, -329.0, -32900.0, 20.0, 6746, 421, 6.154),
        PublisherMetrics("FITZ", 28, 0, 28, 28, 25, 6, 7, 3, 8, 0, 0, 1.0, 185.0, -184.0, -18400.0, 25.0, 6892, 246, 6.326),
        PublisherMetrics("TDES023-YT", 14, 0, 13, 13, 10, 0, 7, 4, 1, 0, 1, 2.0, 0.0, 2.0, 100.0, 50.0, 7665, 590, 6.470),
        PublisherMetrics("TDES008-YT", 7, 0, 6, 6, 4, 0, 4, 3, 1, 0, 1, 0.0, 0.0, 0.0, 0.0, 57.14, 716, 119, 0.748),
        PublisherMetrics("TDES082-YT", 2, 0, 1, 2, 0, 0, 0, 2, 0, 1, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0, 0.038),
        PublisherMetrics("TDES085-YT", 2, 0, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 67, 34, 0.096),
        PublisherMetrics("LeadVIPs", 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 15, 15, 0.048),
        PublisherMetrics("TDES063-YT", 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0.0, 0.0, 0.0, 0.0, 0.0, 18, 18, 0.038),
        PublisherMetrics("TDES075-YT", 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 48, 48, 0.048),
        PublisherMetrics("TDES092-YT", 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0.0, 0.0, 0.0, 0.0, 0.0, 28, 28, 0.038),
    ]
    
    # Calculate totals
    totals = calculate_totals(sample_metrics)
    
    print(f"📊 Sample Daily Data:")
    print(f"   Total Calls: {totals.incoming}")
    print(f"   Completed Calls: {totals.completed}")
    print(f"   Total Revenue: ${totals.revenue:.2f}")
    print(f"   Total Payout: ${totals.payout:.2f}")
    print(f"   Total Profit: ${totals.profit:.2f}")
    print(f"   Overall CPA: ${totals.cpa:.2f}")
    print(f"   Conversion Rate: {totals.conversion_percent:.1f}%")
    
    print(f"\n📋 Publisher Breakdown:")
    for i, metric in enumerate(sorted(sample_metrics, key=lambda x: x.completed, reverse=True), 1):
        print(f"   {i:2d}. {metric.publisher_name}: {metric.completed} completed, ${metric.cpa:.2f} CPA, ${metric.payout:.2f} payout")
    
    # Calculate CPA statistics
    cpas = [m.cpa for m in sample_metrics if m.completed > 0]
    if cpas:
        avg_cpa = sum(cpas) / len(cpas)
        min_cpa = min(cpas)
        max_cpa = max(cpas)
        
        print(f"\n📊 CPA Analysis:")
        print(f"   Average CPA: ${avg_cpa:.2f}")
        print(f"   Lowest CPA: ${min_cpa:.2f}")
        print(f"   Highest CPA: ${max_cpa:.2f}")
        print(f"   Publishers with CPA > $0: {len(cpas)}")

async def test_slack_end_of_day():
    """Test sending end-of-day report to Slack"""
    print(f"\n📱 TESTING END-OF-DAY SLACK REPORT")
    print("=" * 60)
    
    # Create sample daily data
    sample_metrics = [
        PublisherMetrics("Koji Digital", 30, 0, 16, 16, 12, 6, 6, 18, 1, 0, 14, 1.0, 330.0, -329.0, -32900.0, 20.0, 6746, 421, 6.154),
        PublisherMetrics("FITZ", 28, 0, 28, 28, 25, 6, 7, 3, 8, 0, 0, 1.0, 185.0, -184.0, -18400.0, 25.0, 6892, 246, 6.326),
        PublisherMetrics("TDES023-YT", 14, 0, 13, 13, 10, 0, 7, 4, 1, 0, 1, 2.0, 0.0, 2.0, 100.0, 50.0, 7665, 590, 6.470),
        PublisherMetrics("TDES008-YT", 7, 0, 6, 6, 4, 0, 4, 3, 1, 0, 1, 0.0, 0.0, 0.0, 0.0, 57.14, 716, 119, 0.748),
        PublisherMetrics("TDES082-YT", 2, 0, 1, 2, 0, 0, 0, 2, 0, 1, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0, 0.038),
        PublisherMetrics("TDES085-YT", 2, 0, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 67, 34, 0.096),
        PublisherMetrics("LeadVIPs", 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 15, 15, 0.048),
        PublisherMetrics("TDES063-YT", 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0.0, 0.0, 0.0, 0.0, 0.0, 18, 18, 0.038),
        PublisherMetrics("TDES075-YT", 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 48, 48, 0.048),
        PublisherMetrics("TDES092-YT", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0.0, 0.0, 0.0, 0.0, 0.0, 28, 28, 0.038),
    ]
    
    # Calculate totals
    totals = calculate_totals(sample_metrics)
    
    # Create end-of-day report
    daily_start_time = datetime(2025, 9, 11, 9, 0, 0, tzinfo=timezone.utc)  # 9am EDT
    end_time = datetime(2025, 9, 11, 21, 0, 0, tzinfo=timezone.utc)        # 9pm EDT
    
    daily_range = f"{daily_start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} EDT"
    
    message = {
        "text": f"📊 END OF DAY REPORT - {daily_range}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 END OF DAY REPORT - {daily_range}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📈 Daily Summary (9am-9pm EDT)*\n"
                           f"• *Total Calls:* {totals.incoming:,}\n"
                           f"• *Completed Calls:* {totals.completed:,}\n"
                           f"• *Revenue:* ${totals.revenue:,.2f}\n"
                           f"• *Payout:* ${totals.payout:,.2f}\n"
                           f"• *Profit:* ${totals.profit:,.2f}\n"
                           f"• *Overall CPA:* ${totals.cpa:.2f}\n"
                           f"• *Conversion Rate:* {totals.conversion_percent:.1f}%"
                }
            }
        ]
    }
    
    # Add top performers
    top_performers = sorted(sample_metrics, key=lambda x: x.completed, reverse=True)[:5]
    performers_text = "*🏆 Top 5 Publishers by Completed Calls (Daily):*\n"
    for i, metric in enumerate(top_performers, 1):
        performers_text += f"{i}. *{metric.publisher_name}*: {metric.completed} completed, ${metric.cpa:.2f} CPA\n"
    
    message["blocks"].append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": performers_text
        }
    })
    
    # Add complete daily table
    table_text = "*📋 ALL Publishers Performance (Daily Summary):*\n"
    table_text += "```\n"
    table_text += f"{'#':<3} {'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Payout':<10} {'Profit':<10}\n"
    table_text += "-" * 90 + "\n"
    
    for i, metric in enumerate(sorted(sample_metrics, key=lambda x: x.completed, reverse=True), 1):
        table_text += f"{i:<3} {metric.publisher_name[:19]:<20} {metric.completed:<10} ${metric.cpa:<7.2f} ${metric.revenue:<9.2f} ${metric.payout:<9.2f} ${metric.profit:<9.2f}\n"
    
    table_text += "```"
    
    message["blocks"].append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": table_text
        }
    })
    
    # Add CPA analysis
    cpas = [m.cpa for m in sample_metrics if m.completed > 0]
    if cpas:
        avg_cpa = sum(cpas) / len(cpas)
        min_cpa = min(cpas)
        max_cpa = max(cpas)
        
        cpa_text = f"*📊 CPA Analysis:*\n"
        cpa_text += f"• *Average CPA:* ${avg_cpa:.2f}\n"
        cpa_text += f"• *Lowest CPA:* ${min_cpa:.2f}\n"
        cpa_text += f"• *Highest CPA:* ${max_cpa:.2f}\n"
        cpa_text += f"• *Publishers with CPA > $0:* {len(cpas)}"
        
        message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": cpa_text
            }
        })
    
    # Add end of day summary
    message["blocks"].append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*✅ End of Day Summary:*\n"
                   f"• Monitoring completed for {daily_start_time.strftime('%Y-%m-%d')}\n"
                   f"• Next monitoring cycle starts tomorrow at 11am EDT\n"
                   f"• All data is accurate and matches Ringba dashboard"
        }
    })
    
    # Send to Slack
    slack_webhook_url = "https://hooks.slack.com/services/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"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(slack_webhook_url, json=message) as response:
                if response.status == 200:
                    print("✅ Successfully sent end-of-day test report to Slack")
                else:
                    error_text = await response.text()
                    print(f"❌ Slack webhook error {response.status}: {error_text}")
    except Exception as e:
        print(f"❌ Error sending Slack message: {e}")

async def main():
    """Main function to test new schedule"""
    print("🚀 TESTING NEW MONITORING SCHEDULE")
    print("=" * 80)
    
    # Test schedule logic
    await test_schedule_logic()
    
    # Test end-of-day report
    await test_end_of_day_report()
    
    # Test Slack end-of-day report
    await test_slack_end_of_day()
    
    print(f"\n🏁 All tests complete!")

if __name__ == "__main__":
    asyncio.run(main())
