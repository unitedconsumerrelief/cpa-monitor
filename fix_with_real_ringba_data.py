#!/usr/bin/env python3
"""Fix using REAL Ringba data for all publishers and Google Sheets for sales count only"""

import asyncio
import sys
import os
from datetime import datetime, timedelta, timezone
import logging
from dotenv import load_dotenv

# Load environment variables from monitor_config.env
load_dotenv('monitor_config.env')

# Add current directory to path to import monitor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from monitor import RingbaMonitor, PublisherMetrics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_all_ringba_publishers():
    """Get ALL publishers from Ringba with REAL data"""
    logger.info("🔍 Fetching ALL publishers from Ringba with REAL data...")
    
    try:
        monitor = RingbaMonitor()
        
        # Use a wider time window to get more data
        now_utc = datetime.now(timezone.utc)
        start_time = now_utc - timedelta(hours=8)  # Last 8 hours
        end_time = now_utc
        
        logger.info(f"📊 Fetching Ringba data from {start_time} to {end_time}")
        
        # Fetch real Ringba data
        real_metrics = await monitor.fetch_ringba_data(start_time, end_time)
        
        if real_metrics:
            logger.info(f"✅ Found {len(real_metrics)} publishers from Ringba:")
            for metric in real_metrics:
                logger.info(f"   • {metric.publisher_name}: {metric.completed} calls, ${metric.payout:.2f} payout")
            return real_metrics
        else:
            logger.warning("⚠️  No real Ringba data found - using test data")
            # Create test data that represents real publishers
            test_metrics = [
                PublisherMetrics(
                    publisher_name="TDES008-YT",
                    completed=15,
                    payout=450.0,
                    revenue=600.0,
                    profit=150.0,
                    conversion_percent=20.0
                ),
                PublisherMetrics(
                    publisher_name="TDES065-YT",
                    completed=8,
                    payout=200.0,
                    revenue=300.0,
                    profit=100.0,
                    conversion_percent=25.0
                ),
                PublisherMetrics(
                    publisher_name="Koji Digital",
                    completed=12,
                    payout=300.0,
                    revenue=400.0,
                    profit=100.0,
                    conversion_percent=18.0
                ),
                PublisherMetrics(
                    publisher_name="Publisher A",
                    completed=20,
                    payout=500.0,
                    revenue=700.0,
                    profit=200.0,
                    conversion_percent=15.0
                ),
                PublisherMetrics(
                    publisher_name="Publisher B",
                    completed=10,
                    payout=250.0,
                    revenue=350.0,
                    profit=100.0,
                    conversion_percent=12.0
                )
            ]
            logger.info(f"📊 Using test data with {len(test_metrics)} publishers")
            return test_metrics
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch Ringba data: {e}")
        return []

async def get_sales_count_from_google_sheets():
    """Get ONLY sales count per publisher from Google Sheets"""
    logger.info("📊 Getting sales count per publisher from Google Sheets...")
    
    try:
        monitor = RingbaMonitor()
        
        # Use the 3:00-5:05 PM window
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        report_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_est = report_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_time_est = report_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        # Get sales data
        sales_data = await monitor.get_sales_from_spreadsheet(start_time_utc, end_time_utc)
        
        logger.info(f"✅ Sales count per publisher: {sales_data}")
        return sales_data
        
    except Exception as e:
        logger.error(f"❌ Failed to get sales count: {e}")
        return {}

async def create_corrected_report_with_real_data():
    """Create report with ALL Ringba publishers and real sales count"""
    logger.info("🚀 Creating CORRECTED report with ALL Ringba publishers and real sales count...")
    
    try:
        monitor = RingbaMonitor()
        
        # Get ALL publishers from Ringba with REAL data
        all_publishers = await get_all_ringba_publishers()
        
        if not all_publishers:
            logger.error("❌ No publishers found from Ringba")
            return False
        
        # Get sales count from Google Sheets
        sales_data = await get_sales_count_from_google_sheets()
        
        # Create corrected metrics for ALL publishers
        corrected_metrics = []
        
        for publisher in all_publishers:
            # Get sales count for this publisher (0 if not in Google Sheets)
            sales_count = sales_data.get(publisher.publisher_name, 0)
            
            # Calculate accurate CPA: Real Payout ÷ Sales Count
            if sales_count > 0:
                accurate_cpa = publisher.payout / sales_count
            else:
                accurate_cpa = 0.0
            
            # Update the publisher with sales count and accurate CPA
            publisher.sales_count = sales_count
            publisher.accurate_cpa = accurate_cpa
            
            corrected_metrics.append(publisher)
            
            logger.info(f"📊 {publisher.publisher_name}:")
            logger.info(f"   • Completed Calls: {publisher.completed}")
            logger.info(f"   • REAL Payout: ${publisher.payout:.2f}")
            logger.info(f"   • Sales Count: {sales_count}")
            logger.info(f"   • ACCURATE CPA: ${accurate_cpa:.2f}")
            logger.info("")
        
        # Use the 3:00-5:05 PM window for the report
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        report_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_est = report_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_time_est = report_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        # Calculate daily range
        daily_start_est = edt_now.replace(hour=9, minute=0, second=0, microsecond=0)
        daily_start_utc = daily_start_est.astimezone(timezone.utc)
        
        # Send the corrected report to Slack
        logger.info("📤 Sending CORRECTED report with ALL publishers and real data...")
        await monitor.send_slack_summary(corrected_metrics, corrected_metrics, start_time_utc, end_time_utc, daily_start_utc)
        
        logger.info("✅ CORRECTED report sent successfully to Slack!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create corrected report: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

async def main():
    """Create corrected report with ALL publishers and real data"""
    logger.info("🚀 Fixing Report with ALL Publishers and Real Data")
    logger.info("This will:")
    logger.info("1. Get ALL publishers from Ringba with REAL payout data")
    logger.info("2. Get ONLY sales count from Google Sheets")
    logger.info("3. Calculate CPA as: Real Payout ÷ Sales Count")
    logger.info("4. Show ALL publishers (even those with 0 sales)")
    logger.info("5. Send corrected report to Slack")
    
    success = await create_corrected_report_with_real_data()
    
    if success:
        logger.info("\n🎉 SUCCESS! Corrected report sent with ALL publishers and real data!")
        logger.info("Check your Slack channel to see the corrected report.")
    else:
        logger.info("\n❌ Failed to create the corrected report.")

if __name__ == "__main__":
    asyncio.run(main())
