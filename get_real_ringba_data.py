#!/usr/bin/env python3
"""Get REAL data from Ringba with actual API token"""

import asyncio
import sys
import os
from datetime import datetime, timedelta, timezone
import logging
from dotenv import load_dotenv

# Load environment variables from monitor_config.env
load_dotenv('monitor_config.env')

# Set the real Ringba API token
os.environ['RINGBA_API_TOKEN'] = '09f0c9f035a894013c44904a9557433e3b41073ce0ee7dee632f6eb495bb9e26da9f78e078a9f83b0d85839dbe52314f14a5b36db9042061c9da36f9ac70c17ab00446bcca65aaeb54e170e13f7114e066439810f88a123b2a56db63830bbf330b7499e93cd176fc8e5d8b96b2b1263a7cf0cbe8'

# Add current directory to path to import monitor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from monitor import RingbaMonitor, PublisherMetrics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_all_real_ringba_publishers():
    """Get ALL real publishers from Ringba with REAL data"""
    logger.info("🔍 Fetching ALL REAL publishers from Ringba...")
    
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

async def get_sales_count_from_google_sheets():
    """Get sales count per publisher from Google Sheets"""
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

async def create_final_corrected_report():
    """Create final corrected report with ALL real publishers"""
    logger.info("🚀 Creating FINAL corrected report with ALL real publishers...")
    
    try:
        monitor = RingbaMonitor()
        
        # Get ALL real publishers from Ringba
        all_publishers = await get_all_real_ringba_publishers()
        
        if not all_publishers:
            logger.error("❌ No real publishers found from Ringba")
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
        logger.info("📤 Sending FINAL corrected report with ALL real publishers...")
        await monitor.send_slack_summary(corrected_metrics, corrected_metrics, start_time_utc, end_time_utc, daily_start_utc)
        
        logger.info("✅ FINAL corrected report sent successfully to Slack!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create final corrected report: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

async def main():
    """Create final corrected report with ALL real publishers"""
    logger.info("🚀 FINAL Report with ALL Real Publishers and Real Data")
    logger.info("This will:")
    logger.info("1. Get ALL real publishers from Ringba with REAL payout data")
    logger.info("2. Get ONLY sales count from Google Sheets")
    logger.info("3. Calculate CPA as: Real Payout ÷ Sales Count")
    logger.info("4. Show ALL publishers (even those with 0 sales)")
    logger.info("5. Send final corrected report to Slack")
    
    success = await create_final_corrected_report()
    
    if success:
        logger.info("\n🎉 SUCCESS! Final corrected report sent with ALL real publishers!")
        logger.info("Check your Slack channel to see the corrected report.")
    else:
        logger.info("\n❌ Failed to create the final corrected report.")

if __name__ == "__main__":
    asyncio.run(main())
