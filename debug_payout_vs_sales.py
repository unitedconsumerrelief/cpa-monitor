#!/usr/bin/env python3
"""Debug the payout vs sales mismatch"""

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

async def debug_payout_vs_sales():
    """Debug why publishers with sales have $0 payout"""
    logger.info("🔍 Debugging payout vs sales mismatch...")
    
    try:
        monitor = RingbaMonitor()
        
        # Get Ringba data for the 3:00-5:05 PM window (same as sales data)
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        report_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_est = report_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_time_est = report_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        logger.info(f"📊 Checking Ringba data for EXACT sales window: {start_time_est} to {end_time_est} EDT")
        
        # Get Ringba data for the EXACT same time window as sales
        ringba_metrics = await monitor.fetch_ringba_data(start_time_utc, end_time_utc)
        
        if ringba_metrics:
            logger.info(f"✅ Found {len(ringba_metrics)} publishers in Ringba for 3:00-5:05 PM window:")
            for metric in ringba_metrics:
                logger.info(f"   • {metric.publisher_name}: {metric.completed} calls, ${metric.payout:.2f} payout")
        else:
            logger.warning("⚠️  No Ringba data found for 3:00-5:05 PM window")
        
        # Get sales data for the same window
        sales_data = await monitor.get_sales_from_spreadsheet(start_time_utc, end_time_utc)
        logger.info(f"📊 Sales data for same window: {sales_data}")
        
        # Compare the data
        logger.info("\n" + "="*80)
        logger.info("📋 COMPARISON: Ringba vs Google Sheets")
        logger.info("="*80)
        
        for publisher_name, sales_count in sales_data.items():
            # Find this publisher in Ringba data
            ringba_publisher = None
            for metric in ringba_metrics:
                if metric.publisher_name == publisher_name:
                    ringba_publisher = metric
                    break
            
            if ringba_publisher:
                logger.info(f"📊 {publisher_name}:")
                logger.info(f"   • Ringba: {ringba_publisher.completed} calls, ${ringba_publisher.payout:.2f} payout")
                logger.info(f"   • Google Sheets: {sales_count} sales")
                logger.info(f"   • CPA: ${ringba_publisher.payout:.2f} ÷ {sales_count} = ${ringba_publisher.payout/sales_count:.2f}")
                logger.info("")
            else:
                logger.warning(f"⚠️  {publisher_name}: Found in Google Sheets but NOT in Ringba for this time window")
        
        # Check if there's a time delay issue
        logger.info("\n🕐 Checking if there's a time delay issue...")
        
        # Get Ringba data for a wider window (last 4 hours)
        wider_start = now_utc - timedelta(hours=4)
        wider_ringba = await monitor.fetch_ringba_data(wider_start, now_utc)
        
        if wider_ringba:
            logger.info(f"📊 Ringba data for last 4 hours ({len(wider_ringba)} publishers):")
            for metric in wider_ringba:
                if metric.publisher_name in sales_data:
                    logger.info(f"   • {metric.publisher_name}: {metric.completed} calls, ${metric.payout:.2f} payout")
        
        return ringba_metrics, sales_data
        
    except Exception as e:
        logger.error(f"❌ Debug failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return [], {}

async def main():
    """Debug the payout vs sales issue"""
    logger.info("🚀 Debugging Payout vs Sales Mismatch")
    logger.info("This will help us understand why publishers with sales have $0 payout")
    
    ringba_data, sales_data = await debug_payout_vs_sales()
    
    logger.info("\n" + "="*80)
    logger.info("📋 DEBUG SUMMARY:")
    logger.info("="*80)
    
    if not ringba_data:
        logger.info("❌ No Ringba data found for the 3:00-5:05 PM window")
        logger.info("This could mean:")
        logger.info("1. No calls were made during this exact time window")
        logger.info("2. There's a time delay in Ringba data")
        logger.info("3. The time window is too narrow")
    else:
        logger.info(f"✅ Found {len(ringba_data)} publishers in Ringba for the time window")
        logger.info("The issue might be:")
        logger.info("1. Payout data is delayed in Ringba")
        logger.info("2. Sales data is from a different time period")
        logger.info("3. There's a mismatch between when sales are recorded vs when payouts are calculated")

if __name__ == "__main__":
    asyncio.run(main())
