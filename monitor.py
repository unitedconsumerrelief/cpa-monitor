#!/usr/bin/env python3
"""Ringba CPA Monitoring System"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import logging
from dataclasses import dataclass

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

class RingbaMonitor:
    def __init__(self):
        self.ringba_api_token = os.getenv("RINGBA_API_TOKEN")
        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        self.ringba_account_id = os.getenv("RINGBA_ACCOUNT_ID", "RA092c10a91f7c461098e354a1bbeda598")

    async def fetch_ringba_data(self, start_time: datetime, end_time: datetime) -> List[PublisherMetrics]:
        """Fetch data from Ringba API"""
        url = f"https://api.ringba.com/v2/{self.ringba_account_id}/insights"
        
        headers = {
            "Authorization": f"Token {self.ringba_api_token}",
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
        
        logger.info(f"API URL: {url}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Payload: {json.dumps(payload, indent=2)}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    logger.info(f"Response status: {response.status}")
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                        return self._parse_ringba_data(data)
                    else:
                        error_text = await response.text()
                        logger.error(f"API error {response.status}: {error_text}")
                        return []
        except Exception as e:
            logger.error(f"Error fetching Ringba data: {e}")
            return []

    def _parse_ringba_data(self, data: Dict[str, Any]) -> List[PublisherMetrics]:
        """Parse Ringba API response into PublisherMetrics objects"""
        metrics_list = []
        
        try:
            # Extract data from the response structure
            if "data" in data and "rows" in data["data"]:
                for row in data["data"]["rows"]:
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
            else:
                logger.warning(f"Unexpected data structure: {data}")
                
        except Exception as e:
            logger.error(f"Error parsing Ringba data: {e}")
            
        return metrics_list

    def calculate_totals(self, metrics: List[PublisherMetrics]) -> PublisherMetrics:
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

    async def send_slack_summary(self, metrics_2hour: List[PublisherMetrics], metrics_daily: List[PublisherMetrics], 
                                start_time: datetime, end_time: datetime, daily_start_time: datetime):
        """Send formatted summary to Slack with both 2-hour and daily views"""
        if not metrics_2hour and not metrics_daily:
            logger.warning("No metrics to send to Slack")
            return
        
        # Calculate totals
        totals_2hour = self.calculate_totals(metrics_2hour) if metrics_2hour else PublisherMetrics(publisher_name="TOTALS")
        totals_daily = self.calculate_totals(metrics_daily) if metrics_daily else PublisherMetrics(publisher_name="TOTALS")
        
        # Format time range
        time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} ET"
        daily_range = f"{daily_start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} ET"
        
        message = {
            "text": f"📊 Ringba Performance Summary - {time_range}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"📊 Ringba Performance Summary - {time_range}"
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
            table_text = "*📋 Detailed Performance (Top 10 - Last 2 Hours):*\n"
            table_text += "```\n"
            table_text += f"{'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Profit':<10}\n"
            table_text += "-" * 70 + "\n"
            
            for metric in sorted(metrics_2hour, key=lambda x: x.completed, reverse=True)[:10]:
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
                async with session.post(self.slack_webhook_url, json=message) as response:
                    if response.status == 200:
                        logger.info("Successfully sent Slack summary")
                    else:
                        error_text = await response.text()
                        logger.error(f"Slack webhook error {response.status}: {error_text}")
        except Exception as e:
            logger.error(f"Error sending Slack message: {e}")

    async def run_monitoring_cycle(self):
        """Run one monitoring cycle"""
        try:
            # Get current EST time
            est_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
            
            # Calculate which 2-hour window we should report on
            # Reports run at: 11am, 1pm, 3pm, 5pm, 7pm, 9pm EST
            # Data windows are: 9-11am, 11am-1pm, 1-3pm, 3-5pm, 5-7pm, 7-9pm EST
            
            current_hour = est_now.hour
            
            # Determine which data window to fetch based on current time
            if current_hour >= 11 and current_hour < 13:  # 11am-1pm EST
                # Report on 9am-11am data
                start_time_est = est_now.replace(hour=9, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=11, minute=0, second=0, microsecond=0)
            elif current_hour >= 13 and current_hour < 15:  # 1pm-3pm EST
                # Report on 11am-1pm data
                start_time_est = est_now.replace(hour=11, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=13, minute=0, second=0, microsecond=0)
            elif current_hour >= 15 and current_hour < 17:  # 3pm-5pm EST
                # Report on 1pm-3pm data
                start_time_est = est_now.replace(hour=13, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=15, minute=0, second=0, microsecond=0)
            elif current_hour >= 17 and current_hour < 19:  # 5pm-7pm EST
                # Report on 3pm-5pm data
                start_time_est = est_now.replace(hour=15, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=17, minute=0, second=0, microsecond=0)
            elif current_hour >= 19 and current_hour < 21:  # 7pm-9pm EST
                # Report on 5pm-7pm data
                start_time_est = est_now.replace(hour=17, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=19, minute=0, second=0, microsecond=0)
            else:
                # Outside business hours or before 11am, use previous 2-hour window
                start_time_est = est_now.replace(hour=9, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=11, minute=0, second=0, microsecond=0)
            
            # Convert to UTC for API call
            start_time = start_time_est.astimezone(timezone.utc)
            end_time = end_time_est.astimezone(timezone.utc)
            
            # Calculate daily range (from 9am EST today)
            daily_start_est = est_now.replace(hour=9, minute=0, second=0, microsecond=0)
            daily_start_utc = daily_start_est.astimezone(timezone.utc)
            
            logger.info(f"Current EST time: {est_now}")
            logger.info(f"Fetching 2-hour data for {start_time} to {end_time}")
            logger.info(f"Fetching daily data for {daily_start_utc} to {end_time}")
            
            # Fetch both 2-hour and daily data
            metrics_2hour = await self.fetch_ringba_data(start_time, end_time)
            metrics_daily = await self.fetch_ringba_data(daily_start_utc, end_time)
            
            if metrics_2hour or metrics_daily:
                logger.info(f"Fetched {len(metrics_2hour)} 2-hour metrics, {len(metrics_daily)} daily metrics")
                
                # Send to Slack with both views
                await self.send_slack_summary(metrics_2hour, metrics_daily, start_time, end_time, daily_start_utc)
            else:
                logger.warning("No metrics fetched from Ringba")
                
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")

    def is_business_hours(self, dt: datetime) -> bool:
        """Check if current time is within business hours (9am-9pm EST, Mon-Sat)"""
        # Convert to EST (UTC-5)
        est = dt.astimezone(timezone(timedelta(hours=-5)))
        
        # Check if it's Monday-Saturday (0=Monday, 6=Sunday)
        weekday = est.weekday()
        if weekday >= 6:  # Sunday
            return False
        
        # Check if it's between 9am and 9pm EST
        hour = est.hour
        return 9 <= hour < 21

    def get_next_business_hour(self) -> datetime:
        """Get the next business hour start time"""
        now_utc = datetime.now(timezone.utc)
        est = now_utc.astimezone(timezone(timedelta(hours=-5)))
        
        # If it's currently business hours, next report is in 2 hours
        if self.is_business_hours(now_utc):
            next_time_est = est + timedelta(hours=2)
            # Make sure next time is still in business hours
            if 9 <= next_time_est.hour < 21 and next_time_est.weekday() < 6:
                return next_time_est.astimezone(timezone.utc)
        
        # Find next business day at 9am EST
        next_day = est.replace(hour=9, minute=0, second=0, microsecond=0)
        
        # If it's already past 9am today, start tomorrow
        if est.hour >= 9:
            next_day += timedelta(days=1)
        
        # Skip Sunday (weekday 6)
        while next_day.weekday() == 6:
            next_day += timedelta(days=1)
        
        # Convert back to UTC
        return next_day.astimezone(timezone.utc)

    async def start_monitoring(self):
        """Start the monitoring service (runs every 2 hours during business hours)"""
        logger.info("Starting Ringba monitoring service...")
        logger.info("Schedule: 9am-9pm EST, Monday-Saturday, every 2 hours")
        
        # Wait until first business hour (11am EST for first report)
        next_run = self.get_next_business_hour()
        logger.info(f"Next monitoring cycle scheduled for: {next_run.astimezone(timezone(timedelta(hours=-5)))} EST")
        
        while True:
            try:
                # Wait until next scheduled time
                now = datetime.now(timezone.utc)
                wait_seconds = (next_run - now).total_seconds()
                
                if wait_seconds > 0:
                    logger.info(f"Waiting {wait_seconds/3600:.1f} hours until next monitoring cycle...")
                    await asyncio.sleep(wait_seconds)
                
                # Run monitoring cycle
                if self.is_business_hours(datetime.now(timezone.utc)):
                    await self.run_monitoring_cycle()
                    
                    # Schedule next run in 2 hours
                    next_run = datetime.now(timezone.utc) + timedelta(hours=2)
                    
                    # If next run is outside business hours, schedule for next business day
                    if not self.is_business_hours(next_run):
                        next_run = self.get_next_business_hour()
                        logger.info(f"Next monitoring cycle scheduled for: {next_run.astimezone(timezone(timedelta(hours=-5)))} EST")
                    else:
                        logger.info(f"Next monitoring cycle in 2 hours: {next_run.astimezone(timezone(timedelta(hours=-5)))} EST")
                else:
                    logger.info("Outside business hours, skipping monitoring cycle")
                    next_run = self.get_next_business_hour()
                    logger.info(f"Next monitoring cycle scheduled for: {next_run.astimezone(timezone(timedelta(hours=-5)))} EST")
                
            except KeyboardInterrupt:
                logger.info("Monitoring service stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in monitoring service: {e}")
                await asyncio.sleep(5 * 60)  # Wait 5 minutes before retrying

async def main():
    """Main function to start monitoring"""
    try:
        monitor = RingbaMonitor()
        await monitor.start_monitoring()
    except Exception as e:
        logging.error(f"Failed to start monitoring service: {e}")

if __name__ == "__main__":
    asyncio.run(main())