#!/usr/bin/env python3
"""Full report test to match the monitoring system's complete format"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import List

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
        """Calculate CPA: Revenue / Completed Calls"""
        if self.completed == 0:
            return 0.0
        return self.revenue / self.completed

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
    """Fetch data from Ringba API with full metrics"""
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {
        "Authorization": f"Token {ringba_token}",
        "Content-Type": "application/json"
    }
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "reportStart": start_str,
        "reportEnd": end_str,
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
        "generateRollups": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print(f"🔗 Fetching full Ringba data from {start_time} to {end_time}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                print(f"📡 Response status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ API call successful")
                    
                    metrics_list = []
                    if "report" in data and "records" in data["report"]:
                        for record in data["report"]["records"]:
                            metrics = PublisherMetrics(
                                publisher_name=record.get("publisherName", ""),
                                incoming=int(float(record.get("callCount", 0))),
                                live=int(float(record.get("liveCallCount", 0))),
                                completed=int(float(record.get("completedCalls", 0))),
                                ended=int(float(record.get("endedCalls", 0))),
                                connected=int(float(record.get("connectedCallCount", 0))),
                                paid=int(float(record.get("payoutCount", 0))),
                                converted=int(float(record.get("convertedCalls", 0))),
                                no_connect=int(float(record.get("nonConnectedCallCount", 0))),
                                duplicate=int(float(record.get("duplicateCalls", 0))),
                                blocked=int(float(record.get("blockedCalls", 0))),
                                ivr_hangup=int(float(record.get("incompleteCalls", 0))),
                                revenue=float(record.get("conversionAmount", 0)),
                                payout=float(record.get("payoutAmount", 0)),
                                profit=float(record.get("profitGross", 0)),
                                margin=float(str(record.get("profitMarginGross", 0)).replace('%', '')) if record.get("profitMarginGross") else 0.0,
                                conversion_percent=float(str(record.get("convertedPercent", 0)).replace('%', '')) if record.get("convertedPercent") else 0.0,
                                tcl_seconds=int(float(record.get("callLengthInSeconds", 0))),
                                acl_seconds=int(float(record.get("avgHandleTime", 0))),
                                total_cost=float(record.get("totalCost", 0))
                            )
                            metrics_list.append(metrics)
                        
                        print(f"📊 Fetched {len(metrics_list)} publishers with full metrics")
                        return metrics_list
                    else:
                        print(f"⚠️  Unexpected data structure")
                        return []
                else:
                    error_text = await response.text()
                    print(f"❌ API error {response.status}: {error_text}")
                    return []
                    
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

async def send_full_slack_report(metrics_2hour: List[PublisherMetrics], metrics_daily: List[PublisherMetrics], 
                                start_time: datetime, end_time: datetime, daily_start_time: datetime):
    """Send complete Slack report matching the monitoring system format"""
    slack_webhook = "https://hooks.slack.com/services/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"
    
    if not metrics_2hour and not metrics_daily:
        print("⚠️  No metrics to send to Slack")
        return
    
    # Calculate totals
    totals_2hour = calculate_totals(metrics_2hour) if metrics_2hour else PublisherMetrics(publisher_name="TOTALS")
    totals_daily = calculate_totals(metrics_daily) if metrics_daily else PublisherMetrics(publisher_name="TOTALS")
    
    # Format time range
    time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} ET"
    daily_range = f"{daily_start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} ET"
    
    message = {
        "text": f"📊 COMPLETE TEST: Ringba Performance Summary - {time_range}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 COMPLETE TEST: Ringba Performance Summary - {time_range}"
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
    
    # Add top performers
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
    
    # Add detailed table with ALL publishers
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
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(slack_webhook, json=message) as response:
                if response.status == 200:
                    print("✅ Complete Slack report sent successfully!")
                else:
                    error_text = await response.text()
                    print(f"❌ Slack error {response.status}: {error_text}")
    except Exception as e:
        print(f"❌ Error sending Slack message: {e}")

async def main():
    """Test complete report with all publishers and daily totals"""
    print("🧪 Starting COMPLETE test: Fetch 1pm-3pm EST data with ALL publishers and daily totals")
    
    # Set up 1pm-3pm EST time range for 09/11/2025
    start_time_est = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))  # 1pm EST
    end_time_est = datetime(2025, 9, 11, 15, 0, 0, tzinfo=timezone(timedelta(hours=-5)))    # 3pm EST
    
    # Convert to UTC for API call
    start_time = start_time_est.astimezone(timezone.utc)
    end_time = end_time_est.astimezone(timezone.utc)
    
    # Calculate daily range (from 9am EST today)
    daily_start_est = datetime(2025, 9, 11, 9, 0, 0, tzinfo=timezone(timedelta(hours=-5)))  # 9am EST
    daily_start_utc = daily_start_est.astimezone(timezone.utc)
    
    print(f"🕐 Fetching 2-hour data: 1pm-3pm EST")
    print(f"🕐 Fetching daily data: 9am-3pm EST")
    
    # Fetch both 2-hour and daily data
    metrics_2hour = await fetch_ringba_data(start_time, end_time)
    metrics_daily = await fetch_ringba_data(daily_start_utc, end_time)
    
    if metrics_2hour or metrics_daily:
        print(f"📊 Fetched {len(metrics_2hour)} 2-hour metrics, {len(metrics_daily)} daily metrics")
        
        # Show summary
        if metrics_2hour:
            totals_2hour = calculate_totals(metrics_2hour)
            print(f"📈 2-HOUR TOTALS: {totals_2hour.completed} completed, ${totals_2hour.revenue:.2f} revenue, ${totals_2hour.cpa:.2f} CPA")
        
        if metrics_daily:
            totals_daily = calculate_totals(metrics_daily)
            print(f"📈 DAILY TOTALS: {totals_daily.completed} completed, ${totals_daily.revenue:.2f} revenue, ${totals_daily.cpa:.2f} CPA")
        
        # Send complete report to Slack
        await send_full_slack_report(metrics_2hour, metrics_daily, start_time, end_time, daily_start_utc)
    else:
        print("⚠️  No data found")

if __name__ == "__main__":
    asyncio.run(main())
