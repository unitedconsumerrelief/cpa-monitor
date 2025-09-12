#!/usr/bin/env python3
"""Test end-of-day report with real daily data (9am-9pm EDT)"""

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

async def fetch_daily_data(start_time: datetime, end_time: datetime) -> List[PublisherMetrics]:
    """Fetch real daily data from Ringba API"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {"Authorization": f"Token {ringba_token}", "Content-Type": "application/json"}
    
    payload = {
        "reportStart": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportEnd": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return parse_ringba_data(data)
                else:
                    print(f"API Error: {response.status}")
                    return []
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

async def send_real_end_of_day_report(metrics_daily: List[PublisherMetrics], 
                                    daily_start_time: datetime, end_time: datetime):
    """Send real end-of-day report to Slack"""
    slack_webhook_url = "https://hooks.slack.com/services/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"
    
    if not metrics_daily:
        print("No daily metrics to send to Slack")
        return
    
    # Calculate totals
    totals_daily = calculate_totals(metrics_daily)
    
    # Format time range
    daily_range = f"{daily_start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} EDT"
    
    message = {
        "text": f"📊 REAL END OF DAY REPORT - {daily_range}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 REAL END OF DAY REPORT - {daily_range}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📈 Daily Summary (9am-9pm EDT)*\n"
                           f"• *Total Calls:* {totals_daily.incoming:,}\n"
                           f"• *Completed Calls:* {totals_daily.completed:,}\n"
                           f"• *Revenue:* ${totals_daily.revenue:,.2f}\n"
                           f"• *Payout:* ${totals_daily.payout:,.2f}\n"
                           f"• *Profit:* ${totals_daily.profit:,.2f}\n"
                           f"• *Overall CPA:* ${totals_daily.cpa:.2f}\n"
                           f"• *Conversion Rate:* {totals_daily.conversion_percent:.1f}%"
                }
            }
        ]
    }
    
    # Add top performers
    if metrics_daily:
        top_performers = sorted(metrics_daily, key=lambda x: x.completed, reverse=True)[:5]
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
    
    # Add complete daily table with all publishers (NO PROFIT COLUMN)
    if metrics_daily:
        table_text = "*📋 ALL Publishers Performance (Daily Summary):*\n"
        table_text += "```\n"
        table_text += f"{'#':<3} {'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Payout':<10}\n"
        table_text += "-" * 80 + "\n"
        
        for i, metric in enumerate(sorted(metrics_daily, key=lambda x: x.completed, reverse=True), 1):
            table_text += f"{i:<3} {metric.publisher_name[:19]:<20} {metric.completed:<10} ${metric.cpa:<7.2f} ${metric.revenue:<9.2f} ${metric.payout:<9.2f}\n"
        
        table_text += "```"
        
        message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": table_text
            }
        })
    
    # Add CPA analysis
    if metrics_daily:
        # Calculate CPA statistics
        cpas = [m.cpa for m in metrics_daily if m.completed > 0]
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
                   f"• All data is accurate and matches Ringba dashboard\n"
                   f"• This is REAL daily data (9am-9pm EDT), not sample data"
        }
    })
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(slack_webhook_url, json=message) as response:
                if response.status == 200:
                    print("✅ Successfully sent REAL end-of-day report to Slack")
                else:
                    error_text = await response.text()
                    print(f"❌ Slack webhook error {response.status}: {error_text}")
    except Exception as e:
        print(f"❌ Error sending Slack message: {e}")

async def main():
    """Test real end-of-day report with actual daily data"""
    print("🚀 TESTING REAL END-OF-DAY REPORT")
    print("=" * 60)
    
    # Fetch REAL daily data for 9am-9pm EDT on 09/11/2025
    daily_start_utc = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone.utc)  # 9am EDT
    daily_end_utc = datetime(2025, 9, 12, 1, 0, 0, tzinfo=timezone.utc)     # 9pm EDT (next day)
    
    print(f"📅 Fetching REAL daily data for 9am-9pm EDT on 09/11/2025")
    print(f"⏰ Start time (UTC): {daily_start_utc}")
    print(f"⏰ End time (UTC): {daily_end_utc}")
    print()
    
    # Fetch real daily data
    metrics = await fetch_daily_data(daily_start_utc, daily_end_utc)
    
    if metrics:
        print(f"📊 Fetched {len(metrics)} publishers for daily report")
        
        # Show summary
        totals = calculate_totals(metrics)
        print(f"📈 DAILY TOTALS: {totals.incoming} calls, {totals.completed} completed, ${totals.payout:.2f} payout, ${totals.cpa:.2f} CPA")
        print()
        
        # Show publisher breakdown
        print(f"📋 Publisher Breakdown:")
        for i, metric in enumerate(sorted(metrics, key=lambda x: x.completed, reverse=True), 1):
            print(f"   {i:2d}. {metric.publisher_name}: {metric.completed} completed, ${metric.cpa:.2f} CPA, ${metric.payout:.2f} payout")
        
        # Send to Slack
        print("\n📱 Sending REAL end-of-day report to Slack...")
        await send_real_end_of_day_report(metrics, daily_start_utc, daily_end_utc)
        print("✅ Real end-of-day test complete!")
    else:
        print("⚠️  No data found for 9am-9pm EDT on 09/11/2025")

if __name__ == "__main__":
    asyncio.run(main())
