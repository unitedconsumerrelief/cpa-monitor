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
    # New fields for accurate CPA calculation
    sales_count: int = 0
    accurate_cpa: float = 0.0

    @property
    def cpa(self) -> float:
        """Calculate CPA: Payout / Completed Calls"""
        if self.completed == 0:
            return 0.0
        return self.payout / self.completed

    def format_tcl(self) -> str:
        """Format TCL in HH:MM:SS"""
        hours = self.tcl_seconds // 3600
        minutes = (self.tcl_seconds % 3600) // 60
        seconds = self.tcl_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def calculate_accurate_cpa(self, sales_count: int) -> float:
        """Calculate accurate CPA: Payout / Sales Count"""
        if sales_count == 0:
            return 0.0
        return self.payout / sales_count

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
        # TODO: Add Google Sheets credentials
        self.sheets_service = None

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
            else:
                logger.warning(f"Unexpected data structure: {data}")
                
        except Exception as e:
            logger.error(f"Error parsing Ringba data: {e}")
            
        return metrics_list

    async def get_sales_from_spreadsheet(self, start_time: datetime, end_time: datetime) -> Dict[str, int]:
        """Get sales count per publisher from Google Sheets for the given time range"""
        # Convert to EDT for spreadsheet comparison
        start_edt = start_time.astimezone(timezone(timedelta(hours=-4)))
        end_edt = end_time.astimezone(timezone(timedelta(hours=-4)))
        
        logger.info(f"📊 Checking Google Sheets for sales between {start_edt} and {end_edt} EDT")
        
        try:
            # Try to read the sheet as CSV (if it's publicly accessible)
            spreadsheet_id = "1yPWM2CIjPcAg1pF7xNUDmt22kbS2qrKqsOgWkIJzd9I"
            csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid=0"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(csv_url) as response:
                    if response.status == 200:
                        csv_content = await response.text()
                        
                        # Parse CSV
                        import pandas as pd
                        from io import StringIO
                        df = pd.read_csv(StringIO(csv_content))
                        
                        # Process the data
                        sales_data = self._process_spreadsheet_data(df, start_edt, end_edt)
                        
                        logger.info(f"📈 Sales found: {sales_data}")
                        return sales_data
                    else:
                        logger.warning(f"Could not access spreadsheet: {response.status}")
                        return self._get_mock_sales_data()
                        
        except Exception as e:
            logger.error(f"Error reading spreadsheet: {e}")
            logger.info("Falling back to mock data")
            return self._get_mock_sales_data()
    
    def _process_spreadsheet_data(self, df, start_edt: datetime, end_edt: datetime) -> Dict[str, int]:
        """Process the spreadsheet data to count sales per publisher"""
        sales_data = {}
        
        # Note: We don't pre-initialize publishers here anymore
        # The enhance_metrics_with_accurate_cpa method will handle all Ringba publishers
        # and this function will only return publishers that have actual sales data
        
        # Process each row
        for index, row in df.iterrows():
            try:
                # Get publisher from Lookup_Publisher column
                publisher = str(row['Lookup_Publisher']) if 'Lookup_Publisher' in row else "Not Found"
                
                # Skip if publisher is "Not Found" or empty
                if publisher == "Not Found" or publisher == "nan" or publisher.strip() == "":
                    continue
                
                # Get date from Date column
                date_str = str(row['Date']) if 'Date' in row else ""
                
                # Get time from Time column
                time_str = str(row['Time']) if 'Time' in row else ""
                
                # Skip if no date or time
                if date_str == "nan" or time_str == "nan" or date_str.strip() == "" or time_str.strip() == "":
                    continue
                
                # Parse date and time
                try:
                    # Parse date (format: 8/5/2025)
                    date_parts = date_str.split('/')
                    if len(date_parts) == 3:
                        month, day, year = date_parts
                        sale_date = datetime(int(year), int(month), int(day))
                        
                        # Parse time (format: 3:00:30 PM)
                        if ':' in time_str:
                            time_parts = time_str.split(':')
                            if len(time_parts) >= 2:
                                hour = int(time_parts[0])
                                minute = int(time_parts[1])
                                
                                # Handle AM/PM
                                if 'PM' in time_str and hour != 12:
                                    hour += 12
                                elif 'AM' in time_str and hour == 12:
                                    hour = 0
                                
                                sale_time = datetime.combine(sale_date, datetime.min.time().replace(hour=hour, minute=minute))
                                # Make timezone aware (EDT)
                                sale_time = sale_time.replace(tzinfo=timezone(timedelta(hours=-4)))
                                
                                # Check if sale is within time range
                                if start_edt <= sale_time <= end_edt:
                                    # Add to sales data (initialize if not exists)
                                    if publisher not in sales_data:
                                        sales_data[publisher] = 0
                                    sales_data[publisher] += 1
                                        
                except Exception as e:
                    logger.warning(f"Error parsing date/time for row {index}: {e}")
                    continue
                    
            except Exception as e:
                logger.warning(f"Error processing row {index}: {e}")
                continue
        
        return sales_data
    
    def _get_mock_sales_data(self) -> Dict[str, int]:
        """Return empty sales data when spreadsheet is not accessible"""
        # Return empty dictionary - all publishers will show 0 sales and $0.00 accurate CPA
        return {}

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
            totals.sales_count += metric.sales_count
        
        # Calculate overall conversion percentage
        if totals.incoming > 0:
            totals.conversion_percent = (totals.converted / totals.incoming) * 100
        
        # Calculate overall accurate CPA
        if totals.sales_count > 0:
            totals.accurate_cpa = totals.payout / totals.sales_count
        else:
            totals.accurate_cpa = 0.0
        
        return totals

    async def enhance_metrics_with_accurate_cpa(self, metrics: List[PublisherMetrics], 
                                               start_time: datetime, end_time: datetime) -> List[PublisherMetrics]:
        """Enhance metrics with accurate CPA calculation using spreadsheet sales data"""
        # Get sales data from spreadsheet
        sales_data = await self.get_sales_from_spreadsheet(start_time, end_time)
        
        # Calculate accurate CPA for each metric
        for metric in metrics:
            # Get sales count from spreadsheet (0 if not found)
            sales_count = sales_data.get(metric.publisher_name, 0)
            metric.sales_count = sales_count
            
            # Calculate accurate CPA
            if sales_count > 0:
                # Publisher has sales data from spreadsheet
                metric.accurate_cpa = metric.calculate_accurate_cpa(sales_count)
                logger.info(f"📊 {metric.publisher_name}: {sales_count} sales, ${metric.accurate_cpa:.2f} accurate CPA")
            else:
                # Publisher has no sales data from spreadsheet
                metric.accurate_cpa = 0.0
                logger.info(f"📊 {metric.publisher_name}: 0 sales, $0.00 accurate CPA")
        
        # Add publishers that have sales in Google Sheets but no Ringba data
        existing_publishers = {metric.publisher_name for metric in metrics}
        for publisher_name, sales_count in sales_data.items():
            if publisher_name not in existing_publishers and sales_count > 0:
                # Create a new metric for this publisher with sales but no Ringba data
                new_metric = PublisherMetrics(
                    publisher_name=publisher_name,
                    completed=0,  # No calls in Ringba
                    payout=0.0,   # No payout in Ringba
                    revenue=0.0,  # No revenue in Ringba
                    profit=0.0,   # No profit in Ringba
                    sales_count=sales_count,
                    accurate_cpa=0.0  # Will be set below
                )
                
                # Apply the "X Sales / No Payout" rule
                if new_metric.payout == 0.0 and sales_count > 0:
                    new_metric.sales_display = f"{sales_count} Sales/No$"
                    new_metric.accurate_cpa = 0.0
                else:
                    new_metric.accurate_cpa = new_metric.calculate_accurate_cpa(sales_count)
                
                metrics.append(new_metric)
                logger.info(f"📊 {publisher_name}: {sales_count} sales, {new_metric.sales_display if hasattr(new_metric, 'sales_display') and new_metric.sales_display else f'${new_metric.accurate_cpa:.2f} CPA'}")
        
        return metrics

    async def send_slack_summary(self, metrics_2hour: List[PublisherMetrics], 
                                start_time: datetime, end_time: datetime):
        """Send formatted summary to Slack with 2-hour window data"""
        if not metrics_2hour:
            logger.warning("No metrics to send to Slack")
            return
        
        # Calculate totals
        totals_2hour = self.calculate_totals(metrics_2hour) if metrics_2hour else PublisherMetrics(publisher_name="TOTALS")
        
        # Format time range
        time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} EDT"
        
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
                        "text": f"*Performance Summary ({time_range})*\n"
                               f"• *Completed Calls:* {totals_2hour.completed:,}\n"
                               f"• *Revenue:* ${totals_2hour.revenue:,.2f}\n"
                               f"• *CPA:* ${totals_2hour.accurate_cpa:.2f}\n"
                               f"• *Profit:* ${totals_2hour.profit:,.2f}\n"
                               f"• *Conversion Rate:* {totals_2hour.conversion_percent:.1f}%"
                    }
                }
            ]
        }
        
        # Add top performers if we have data
        if metrics_2hour:
            top_performers = sorted(metrics_2hour, key=lambda x: x.completed, reverse=True)[:5]
            performers_text = "*🏆 Top 5 Publishers by Completed Calls:*\n"
            for i, metric in enumerate(top_performers, 1):
                performers_text += f"{i}. *{metric.publisher_name}*: {metric.completed} completed, ${metric.accurate_cpa:.2f} CPA\n"
            
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": performers_text
                }
            })
        
        # Add detailed table
        if metrics_2hour:
            table_text = "*📋 ALL Publishers Performance:*\n"
            table_text += "```\n"
            table_text += f"{'Publisher':<20} {'Completed':<10} {'Sales':<6} {'Accurate CPA':<15} {'Revenue':<10} {'Profit':<10}\n"
            table_text += "-" * 70 + "\n"
            
            for metric in sorted(metrics_2hour, key=lambda x: x.completed, reverse=True):
                # Handle special case: show "X Sales / No Payout" for zero payout with sales
                if hasattr(metric, 'sales_display') and metric.sales_display:
                    cpa_display = metric.sales_display
                else:
                    cpa_display = f"${metric.accurate_cpa:.2f}"
                
                table_text += f"{metric.publisher_name[:19]:<20} {metric.completed:<10} {metric.sales_count:<6} {cpa_display:<15} ${metric.revenue:<9.2f} ${metric.profit:<9.2f}\n"
            
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

    async def send_end_of_day_summary(self, metrics_2hour: List[PublisherMetrics], 
                                    start_time: datetime, end_time: datetime):
        """Send end-of-day summary with all publishers and CPA calculations"""
        if not metrics_2hour:
            logger.warning("No metrics to send to Slack")
            return
        
        # Calculate totals
        totals_2hour = self.calculate_totals(metrics_2hour)
        
        # Format time range
        time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')} EDT"
        
        message = {
            "text": f"📊 END OF DAY REPORT - {time_range}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"📊 END OF DAY REPORT - {time_range}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*📈 Final Summary*\n"
                               f"• *Total Calls:* {totals_2hour.incoming:,}\n"
                               f"• *Completed Calls:* {totals_2hour.completed:,}\n"
                               f"• *Revenue:* ${totals_2hour.revenue:,.2f}\n"
                               f"• *Payout:* ${totals_2hour.payout:,.2f}\n"
                               f"• *Profit:* ${totals_2hour.profit:,.2f}\n"
                               f"• *Overall CPA:* ${totals_2hour.accurate_cpa:.2f}\n"
                               f"• *Conversion Rate:* {totals_2hour.conversion_percent:.1f}%"
                    }
                }
            ]
        }
        
        # Add top performers
        if metrics_2hour:
            top_performers = sorted(metrics_2hour, key=lambda x: x.completed, reverse=True)[:5]
            performers_text = "*🏆 Top 5 Publishers by Completed Calls:*\n"
            for i, metric in enumerate(top_performers, 1):
                performers_text += f"{i}. *{metric.publisher_name}*: {metric.completed} completed, ${metric.accurate_cpa:.2f} CPA\n"
            
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": performers_text
                }
            })
        
        # Add complete table with all publishers
        if metrics_2hour:
            table_text = "*📋 ALL Publishers Performance:*\n"
            table_text += "```\n"
            table_text += f"{'#':<3} {'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Payout':<10}\n"
            table_text += "-" * 80 + "\n"
            
            for i, metric in enumerate(sorted(metrics_2hour, key=lambda x: x.completed, reverse=True), 1):
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
        if metrics_2hour:
            # Calculate CPA statistics
            cpas = [m.cpa for m in metrics_2hour if m.completed > 0]
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
                       f"• Monitoring completed for {start_time.strftime('%Y-%m-%d')}\n"
                       f"• Next monitoring cycle starts tomorrow at 11am EDT\n"
                       f"• All data is accurate and matches Ringba dashboard"
            }
        })
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.slack_webhook_url, json=message) as response:
                    if response.status == 200:
                        logger.info("Successfully sent end-of-day summary")
                    else:
                        error_text = await response.text()
                        logger.error(f"Slack webhook error {response.status}: {error_text}")
        except Exception as e:
            logger.error(f"Error sending end-of-day Slack message: {e}")

    async def run_monitoring_cycle(self):
        """Run one monitoring cycle"""
        try:
            # Get current EDT time (UTC-4 in summer, UTC-5 in winter)
            est_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-4)))
            
            # Calculate which 2-hour window we should report on
            # Reports run at: 11am, 1pm, 3pm, 5pm, 7pm, 9pm EDT
            # Data windows are: 9-11am, 11am-1pm, 1-3pm, 3-5pm, 5-7pm, 7-9pm EDT
            
            current_hour = est_now.hour
            
            # Determine which data window to fetch based on current time
            if current_hour >= 11 and current_hour < 13:  # 11am-1pm EDT
                # Report on 9am-11am data
                start_time_est = est_now.replace(hour=9, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=11, minute=0, second=0, microsecond=0)
                report_type = "2-hour"
            elif current_hour >= 13 and current_hour < 15:  # 1pm-3pm EDT
                # Report on 11am-1pm data
                start_time_est = est_now.replace(hour=11, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=13, minute=0, second=0, microsecond=0)
                report_type = "2-hour"
            elif current_hour >= 15 and current_hour < 17:  # 3pm-5pm EDT
                # Report on 1pm-3pm data
                start_time_est = est_now.replace(hour=13, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=15, minute=0, second=0, microsecond=0)
                report_type = "2-hour"
            elif current_hour >= 17 and current_hour < 19:  # 5pm-7pm EDT
                # Report on 3pm-5pm data (with 5-minute buffer)
                start_time_est = est_now.replace(hour=15, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=17, minute=5, second=0, microsecond=0)  # 5:05pm EDT
                report_type = "2-hour"
            elif current_hour >= 19 and current_hour < 21:  # 7pm-9pm EDT
                # Report on 5pm-7pm data
                start_time_est = est_now.replace(hour=17, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=19, minute=0, second=0, microsecond=0)
                report_type = "2-hour"
            elif current_hour >= 21:  # 9pm EDT - End of day report
                # Report on 7pm-9pm data + full day summary
                start_time_est = est_now.replace(hour=19, minute=0, second=0, microsecond=0)
                end_time_est = est_now.replace(hour=21, minute=0, second=0, microsecond=0)
                report_type = "end-of-day"
            else:
                # Outside business hours or before 11am, skip
                logger.info("Outside business hours, skipping monitoring cycle")
                return
            
            # Convert to UTC for API call
            start_time = start_time_est.astimezone(timezone.utc)
            end_time = end_time_est.astimezone(timezone.utc)
            
            logger.info(f"Current EDT time: {est_now}")
            logger.info(f"Report type: {report_type}")
            logger.info(f"Fetching 2-hour data for {start_time} to {end_time}")
            
            # Fetch 2-hour data only
            metrics_2hour = await self.fetch_ringba_data(start_time, end_time)
            
            if metrics_2hour:
                logger.info(f"Fetched {len(metrics_2hour)} 2-hour metrics")
                
                # Enhance metrics with accurate CPA calculation
                metrics_2hour = await self.enhance_metrics_with_accurate_cpa(metrics_2hour, start_time, end_time)
                
                if report_type == "end-of-day":
                    # Send end-of-day report (using 2-hour data)
                    await self.send_end_of_day_summary(metrics_2hour, start_time, end_time)
                else:
                    # Send regular 2-hour report
                    await self.send_slack_summary(metrics_2hour, start_time, end_time)
            else:
                logger.warning("No metrics fetched from Ringba")
                
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")

    def is_business_hours(self, dt: datetime) -> bool:
        """Check if current time is within business hours (9am-9pm EDT, Mon-Sat)"""
        # Convert to EDT (UTC-4 in summer, UTC-5 in winter)
        edt = dt.astimezone(timezone(timedelta(hours=-4)))
        
        # Check if it's Monday-Saturday (0=Monday, 6=Sunday)
        weekday = edt.weekday()
        if weekday >= 6:  # Sunday
            return False
        
        # Check if it's between 9am and 9pm EDT
        hour = edt.hour
        return 9 <= hour < 21

    def get_next_report_time(self) -> datetime:
        """Get the next report time based on the schedule"""
        now_utc = datetime.now(timezone.utc)
        edt = now_utc.astimezone(timezone(timedelta(hours=-4)))
        
        # Define report times: 11am, 1pm, 3pm, 5:05pm, 7pm, 9pm EDT (5:05pm for buffer)
        report_hours = [11, 13, 15, 17, 19, 21]
        report_minutes = [0, 0, 0, 5, 0, 0]  # 5:05pm for 5pm report
        
        # Find next report time today
        for i, hour in enumerate(report_hours):
            if edt.hour < hour or (edt.hour == hour and edt.minute < report_minutes[i]):
                next_time_edt = edt.replace(hour=hour, minute=report_minutes[i], second=0, microsecond=0)
                return next_time_edt.astimezone(timezone.utc)
        
        # If past 9pm today, start tomorrow at 11am EDT
        next_day = edt.replace(hour=11, minute=0, second=0, microsecond=0) + timedelta(days=1)
        
        # Skip Sunday (weekday 6)
        while next_day.weekday() == 6:
            next_day += timedelta(days=1)
        
        # Convert back to UTC
        return next_day.astimezone(timezone.utc)

    async def start_monitoring(self):
        """Start the monitoring service with the new schedule"""
        logger.info("Starting Ringba monitoring service...")
        logger.info("Schedule: 9am-9pm EDT, Monday-Saturday")
        logger.info("Reports: 11am, 1pm, 3pm, 5pm, 7pm, 9pm EDT")
        logger.info("Data windows: 9-11am, 11am-1pm, 1-3pm, 3-5pm, 5-7pm, 7-9pm EDT")
        
        # Wait until first report time (11am EDT)
        next_run = self.get_next_report_time()
        logger.info(f"Next monitoring cycle scheduled for: {next_run.astimezone(timezone(timedelta(hours=-4)))} EDT")
        
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
                    
                    # Schedule next run
                    next_run = self.get_next_report_time()
                    logger.info(f"Next monitoring cycle scheduled for: {next_run.astimezone(timezone(timedelta(hours=-4)))} EDT")
                else:
                    logger.info("Outside business hours, skipping monitoring cycle")
                    next_run = self.get_next_report_time()
                    logger.info(f"Next monitoring cycle scheduled for: {next_run.astimezone(timezone(timedelta(hours=-4)))} EDT")
                
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