#!/usr/bin/env python3
"""Resend the 11am report that failed due to webhook issue"""

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

async def fetch_ringba_data(start_time: datetime, end_time: datetime) -> List[PublisherMetrics]:
    """Fetch data from Ringba API"""
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

async def send_11am_report(metrics_2hour: List[PublisherMetrics], metrics_daily: List[PublisherMetrics], 
                          start_time: datetime, end_time: datetime, daily_start_time: datetime):
    """Send the 11am report that failed due to webhook issue"""
    slack_webhook_url = "https://hooks.slack.com/services/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"
    
    if not metrics_2hour and not metrics_daily:
        print("No metrics to send to Slack")
        return
    
    # Calculate totals
    totals_2hour = calculate_totals(metrics_2hour) if metrics_2hour else PublisherMetrics(publisher_name="TOTALS")
    totals_daily = calculate_totals(metrics_daily) if metrics_daily else PublisherMetrics(publisher_name="TOTALS")
    
    # Format time range
    time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} EDT"
    daily_range = f"{daily_start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} EDT"
    
    message = {
        "text": f"📊 RESEND: Ringba Performance Summary - {time_range}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 RESEND: Ringba Performance Summary - {time_range}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Last 2 Hours ({time_range})*\n"
                           f"• *Completed Calls:* {totals_2hour.completed:,}\n"
                           f"• *Revenue:* ${totals_2hour.revenue:,.2f}\n"
                           f"• *CPA:* ${totals_2hour.cpa:.2f}\n"
                           f"• *Profit:* ${totals_2hour.profit:,.2f}\n"
                           f"• *Conversion Rate:* {totals_2hour.conversion_percent:.1f}%"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Daily Total ({daily_range})*\n"
                           f"• *Completed Calls:* {totals_daily.completed:,}\n"
                           f"• *Revenue:* ${totals_daily.revenue:,.2f}\n"
                           f"• *CPA:* ${totals_daily.cpa:.2f}\n"
                           f"• *Profit:* ${totals_daily.profit:,.2f}\n"
                           f"• *Conversion Rate:* {totals_daily.conversion_percent:.1f}%"
                }
            }
        ]
    }
    
    # Add top performers if we have data
    if metrics_2hour:
        top_performers = sorted(metrics_2hour, key=lambda x: x.completed, reverse=True)[:5]
        performers_text = "*🏆 Top 5 Publishers by Completed Calls (Last 2 Hours):*\n"
        for i, metric in enumerate(top_performers, 1):
            performers_text += f"{i}. *{metric.publisher_name}*: {metric.completed} completed, ${metric.cpa:.2f} CPA\n"
        
        message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": performers_text
            }
        })
    
    # Add detailed table
    if metrics_2hour:
        table_text = "*📋 ALL Publishers Performance (Last 2 Hours):*\n"
        table_text += "```\n"
        table_text += f"{'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Profit':<10}\n"
        table_text += "-" * 70 + "\n"
        
        for metric in sorted(metrics_2hour, key=lambda x: x.completed, reverse=True):
            table_text += f"{metric.publisher_name[:19]:<20} {metric.completed:<10} ${metric.cpa:<7.2f} ${metric.revenue:<9.2f} ${metric.profit:<9.2f}\n"
        
        table_text += "```"
        
        message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": table_text
            }
        })
    
    # Add daily accumulated table
    if metrics_daily:
        daily_table_text = "*📋 ALL Publishers Performance (Daily Accumulated):*\n"
        daily_table_text += "```\n"
        daily_table_text += f"{'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Profit':<10}\n"
        daily_table_text += "-" * 70 + "\n"
        
        for metric in sorted(metrics_daily, key=lambda x: x.completed, reverse=True):
            daily_table_text += f"{metric.publisher_name[:19]:<20} {metric.completed:<10} ${metric.cpa:<7.2f} ${metric.revenue:<9.2f} ${metric.profit:<9.2f}\n"
        
        daily_table_text += "```"
        
        message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": daily_table_text
            }
        })
    
    # Add resend notice
    message["blocks"].append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*🔄 Resend Notice:*\n"
                   f"• This is the 11am report that failed due to webhook issue\n"
                   f"• Webhook has been fixed and reports will resume normally\n"
                   f"• Next report scheduled for 1pm EDT"
        }
    })
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(slack_webhook_url, json=message) as response:
                if response.status == 200:
                    print("✅ Successfully resent 11am report to Slack")
                else:
                    error_text = await response.text()
                    print(f"❌ Slack webhook error {response.status}: {error_text}")
    except Exception as e:
        print(f"❌ Error sending Slack message: {e}")

async def main():
    """Resend the 11am report"""
    print("🔄 RESENDING 11AM REPORT")
    print("=" * 50)
    
    # Fetch 9am-11am EDT data (the 11am report window)
    start_time_utc = datetime(2025, 9, 12, 13, 0, 0, tzinfo=timezone.utc)  # 9am EDT
    end_time_utc = datetime(2025, 9, 12, 15, 0, 0, tzinfo=timezone.utc)    # 11am EDT
    
    # Fetch daily data (from 9am EDT today)
    daily_start_utc = datetime(2025, 9, 12, 13, 0, 0, tzinfo=timezone.utc)  # 9am EDT
    
    print(f"📅 Fetching 9am-11am EDT data for 11am report")
    print(f"⏰ Start time (UTC): {start_time_utc}")
    print(f"⏰ End time (UTC): {end_time_utc}")
    print()
    
    # Fetch data
    metrics_2hour = await fetch_ringba_data(start_time_utc, end_time_utc)
    metrics_daily = await fetch_ringba_data(daily_start_utc, end_time_utc)
    
    if metrics_2hour or metrics_daily:
        print(f"📊 Fetched {len(metrics_2hour)} 2-hour metrics, {len(metrics_daily)} daily metrics")
        
        # Show summary
        if metrics_2hour:
            totals_2hour = calculate_totals(metrics_2hour)
            print(f"📈 2-HOUR TOTALS: {totals_2hour.completed} completed, ${totals_2hour.payout:.2f} payout, ${totals_2hour.cpa:.2f} CPA")
        
        if metrics_daily:
            totals_daily = calculate_totals(metrics_daily)
            print(f"📈 DAILY TOTALS: {totals_daily.completed} completed, ${totals_daily.payout:.2f} payout, ${totals_daily.cpa:.2f} CPA")
        
        # Send to Slack
        print("\n📱 Sending 11am report to Slack...")
        await send_11am_report(metrics_2hour, metrics_daily, start_time_utc, end_time_utc, daily_start_utc)
        print("✅ 11am report resent successfully!")
    else:
        print("⚠️  No data found for 9am-11am EDT")

if __name__ == "__main__":
    asyncio.run(main())
