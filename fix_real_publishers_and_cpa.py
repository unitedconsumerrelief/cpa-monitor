#!/usr/bin/env python3
"""Fix to use ONLY real publishers and correct CPA calculation"""

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

async def get_real_publishers_from_ringba():
    """Get ONLY real publishers from Ringba API"""
    logger.info("🔍 Fetching REAL publishers from Ringba API...")
    
    try:
        monitor = RingbaMonitor()
        
        # Use a wider time window to get more data
        now_utc = datetime.now(timezone.utc)
        start_time = now_utc - timedelta(hours=6)  # Last 6 hours
        end_time = now_utc
        
        logger.info(f"📊 Fetching Ringba data from {start_time} to {end_time}")
        
        # Fetch real Ringba data
        real_metrics = await monitor.fetch_ringba_data(start_time, end_time)
        
        if real_metrics:
            logger.info(f"✅ Found {len(real_metrics)} REAL publishers from Ringba:")
            for metric in real_metrics:
                logger.info(f"   • {metric.publisher_name}: {metric.completed} calls, ${metric.payout:.2f} payout")
            return real_metrics
        else:
            logger.warning("⚠️  No real Ringba data found")
            return []
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch real Ringba data: {e}")
        return []

async def count_actual_sales_per_publisher():
    """Count actual sales rows per publisher from Google Sheets"""
    logger.info("📊 Counting ACTUAL sales per publisher from Google Sheets...")
    
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
        
        logger.info(f"📊 Counting sales in window: {start_time_est} to {end_time_est} EDT")
        
        # Get sales data
        sales_data = await monitor.get_sales_from_spreadsheet(start_time_utc, end_time_utc)
        
        logger.info(f"✅ Actual sales count per publisher: {sales_data}")
        return sales_data
        
    except Exception as e:
        logger.error(f"❌ Failed to count sales: {e}")
        return {}

async def create_corrected_report():
    """Create report with ONLY real publishers and correct CPA"""
    logger.info("🚀 Creating CORRECTED report with real publishers and accurate CPA...")
    
    try:
        monitor = RingbaMonitor()
        
        # Get real publishers from Ringba
        real_publishers = await get_real_publishers_from_ringba()
        
        if not real_publishers:
            logger.error("❌ No real publishers found from Ringba")
            return False
        
        # Get actual sales count per publisher
        sales_data = await count_actual_sales_per_publisher()
        
        # Create metrics for ONLY real publishers
        corrected_metrics = []
        
        for publisher in real_publishers:
            # Get actual sales count for this publisher
            actual_sales = sales_data.get(publisher.publisher_name, 0)
            
            # Calculate accurate CPA: Payout ÷ Actual Sales Count
            if actual_sales > 0:
                accurate_cpa = publisher.payout / actual_sales
            else:
                accurate_cpa = 0.0
            
            # Update the publisher with accurate CPA
            publisher.sales_count = actual_sales
            publisher.accurate_cpa = accurate_cpa
            
            corrected_metrics.append(publisher)
            
            logger.info(f"📊 {publisher.publisher_name}:")
            logger.info(f"   • Completed Calls: {publisher.completed}")
            logger.info(f"   • Payout: ${publisher.payout:.2f}")
            logger.info(f"   • Actual Sales: {actual_sales}")
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
        logger.info("📤 Sending CORRECTED report with REAL publishers and accurate CPA...")
        await monitor.send_slack_summary(corrected_metrics, corrected_metrics, start_time_utc, end_time_utc, daily_start_utc)
        
        logger.info("✅ CORRECTED report sent successfully to Slack!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create corrected report: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

async def main():
    """Create corrected report with real publishers and accurate CPA"""
    logger.info("🚀 Fixing Report with REAL Publishers and Accurate CPA")
    logger.info("This will:")
    logger.info("1. Use ONLY real publishers from Ringba API")
    logger.info("2. Count ACTUAL sales rows per publisher from Google Sheets")
    logger.info("3. Calculate CPA as: Payout ÷ Actual Sales Count")
    logger.info("4. Send corrected report to Slack")
    
    success = await create_corrected_report()
    
    if success:
        logger.info("\n🎉 SUCCESS! Corrected report sent with REAL publishers and accurate CPA!")
        logger.info("Check your Slack channel to see the corrected report.")
    else:
        logger.info("\n❌ Failed to create the corrected report.")

if __name__ == "__main__":
    asyncio.run(main())
