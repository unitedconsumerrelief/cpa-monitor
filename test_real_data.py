#!/usr/bin/env python3
"""Test script to fetch real 1pm-3pm EST data for 09/11/2025 and send Slack report"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import logging
from dataclasses import dataclass
# Using hardcoded credentials for testing

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

    def format_tcl(self) -> str:
        """Format TCL in HH:MM:SS"""
        hours = self.tcl_seconds // 3600
        minutes = (self.tcl_seconds % 3600) // 60
        seconds = self.tcl_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def format_acl(self) -> str:
        """Format ACL in HH:MM:SS"""
        hours = self.acl_seconds // 3600
        minutes = (self.acl_seconds % 3600) // 60
        seconds = self.acl_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

async def fetch_ringba_data(start_time: datetime, end_time: datetime) -> List[PublisherMetrics]:
    """Fetch data from Ringba API"""
    ringba_api_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account_id = "RA092c10a91f7c461098e354a1bbeda598"
    
    url = f"https://api.ringba.com/v2/{ringba_account_id}/insights"
    
    headers = {
        "Authorization": f"Token {ringba_api_token}",
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
    
    logger.info(f"🔗 API URL: {url}")
    logger.info(f"⏰ Fetching data from {start_time} to {end_time}")
    logger.info(f"📊 Payload: {json.dumps(payload, indent=2)}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                logger.info(f"📡 Response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ SUCCESS: API call successful")
                    logger.info(f"📊 Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    
                    # Parse the data
                    metrics_list = []
                    if "report" in data and "records" in data["report"]:
                        for row in data["report"]["records"]:
                            metrics = PublisherMetrics(
                                publisher_name=row.get("publisherName", ""),
                                incoming=row.get("callCount", 0),
                                live=row.get("liveCallCount", 0),
                                completed=row.get("completedCalls", 0),
                                ended=row.get("endedCalls", 0),
                                connected=row.get("connectedCallCount", 0),
                                paid=row.get("payoutCount", 0),
                                converted=row.get("convertedCalls", 0),
                                no_connect=row.get("nonConnectedCallCount", 0),
                                duplicate=row.get("duplicateCalls", 0),
                                blocked=row.get("blockedCalls", 0),
                                ivr_hangup=row.get("incompleteCalls", 0),
                                revenue=row.get("conversionAmount", 0.0),
                                payout=row.get("payoutAmount", 0.0),
                                profit=row.get("profitGross", 0.0),
                                margin=row.get("profitMarginGross", 0.0),
                                conversion_percent=row.get("convertedPercent", 0.0),
                                tcl_seconds=row.get("callLengthInSeconds", 0),
                                acl_seconds=row.get("avgHandleTime", 0),
                                total_cost=row.get("totalCost", 0.0)
                            )
                            metrics_list.append(metrics)
                        
                        logger.info(f"📈 Fetched {len(metrics_list)} publisher records")
                        return metrics_list
                    else:
                        logger.warning(f"⚠️  Unexpected data structure: {data}")
                        return []
                else:
                    error_text = await response.text()
                    logger.error(f"❌ API error {response.status}: {error_text}")
                    return []
                    
    except Exception as e:
        logger.error(f"❌ Error fetching Ringba data: {e}")
        return []

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

async def send_slack_summary(metrics: List[PublisherMetrics], start_time: datetime, end_time: datetime):
    """Send formatted summary to Slack"""
    slack_webhook_url = "https://hooks.slack.com/services/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"
    
    if not metrics:
        logger.warning("No metrics to send to Slack")
        return
    
    # Calculate totals
    totals = calculate_totals(metrics)
    
    # Format time range
    time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} ET"
    
    message = {
        "text": f"🧪 TEST REPORT: Ringba Performance Summary - {time_range}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🧪 TEST REPORT: Ringba Performance Summary - {time_range}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Test Data (1pm-3pm EST on 09/11/2025)*\n"
                           f"• *Completed Calls:* {totals.completed:,}\n"
                           f"• *Revenue:* ${totals.revenue:,.2f}\n"
                           f"• *CPA:* ${totals.cpa:.2f}\n"
                           f"• *Profit:* ${totals.profit:,.2f}\n"
                           f"• *Conversion Rate:* {totals.conversion_percent:.1f}%"
                }
            }
        ]
    }
    
    # Add top performers
    if metrics:
        top_performers = sorted(metrics, key=lambda x: x.completed, reverse=True)[:5]
        performers_text = "*🏆 Top 5 Publishers by Completed Calls:*\n"
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
    if metrics:
        table_text = "*📋 Detailed Performance (Top 10):*\n"
        table_text += "```\n"
        table_text += f"{'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Profit':<10}\n"
        table_text += "-" * 70 + "\n"
        
        for metric in sorted(metrics, key=lambda x: x.completed, reverse=True)[:10]:
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
            async with session.post(slack_webhook_url, json=message) as response:
                if response.status == 200:
                    logger.info("✅ Successfully sent Slack test report")
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Slack webhook error {response.status}: {error_text}")
    except Exception as e:
        logger.error(f"❌ Error sending Slack message: {e}")

async def main():
    """Test the 1pm-3pm EST data fetch and send Slack report"""
    logger.info("🧪 Starting test: Fetch 1pm-3pm EST data for 09/11/2025 and send Slack report")
    
    # Set up 1pm-3pm EST time range for 09/11/2025
    start_time_est = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))  # 1pm EST
    end_time_est = datetime(2025, 9, 11, 15, 0, 0, tzinfo=timezone(timedelta(hours=-5)))    # 3pm EST
    
    # Convert to UTC for API call
    start_time = start_time_est.astimezone(timezone.utc)
    end_time = end_time_est.astimezone(timezone.utc)
    
    logger.info(f"🕐 Fetching data for 1pm-3pm EST on 09/11/2025")
    logger.info(f"⏰ Start time (UTC): {start_time}")
    logger.info(f"⏰ End time (UTC): {end_time}")
    
    # Fetch data
    metrics = await fetch_ringba_data(start_time, end_time)
    
    if metrics:
        logger.info(f"📊 Fetched {len(metrics)} publishers")
        
        # Show summary
        totals = calculate_totals(metrics)
        logger.info(f"📈 TOTALS: {totals.completed} completed calls, ${totals.revenue:.2f} revenue, ${totals.cpa:.2f} CPA")
        
        # Send to Slack
        await send_slack_summary(metrics, start_time, end_time)
    else:
        logger.warning("⚠️  No data found for 1pm-3pm EST on 09/11/2025")

if __name__ == "__main__":
    asyncio.run(main())
