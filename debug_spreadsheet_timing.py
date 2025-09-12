#!/usr/bin/env python3
"""Debug the spreadsheet timing issue"""

import asyncio
import sys
import os
from datetime import datetime, timedelta, timezone
import logging

# Add current directory to path to import monitor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from monitor import RingbaMonitor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def debug_spreadsheet_timing():
    """Debug why we're missing 1 sale in the spreadsheet parsing"""
    logger.info("🔍 Debugging spreadsheet timing issue...")
    
    try:
        monitor = RingbaMonitor()
        
        # Test with the exact 3:00-5:05 PM window
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        
        # Set to 5:05 PM EDT for testing
        test_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        
        # Calculate the exact 3pm-5pm data window
        start_time_est = test_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)  # 3:00 PM EDT
        end_time_est = test_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)    # 5:05 PM EDT
        
        # Convert to UTC for API calls
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        logger.info(f"🕐 Testing time window:")
        logger.info(f"   • Start: {start_time_est} EDT")
        logger.info(f"   • End: {end_time_est} EDT")
        logger.info(f"   • UTC Start: {start_time_utc} UTC")
        logger.info(f"   • UTC End: {end_time_utc} UTC")
        
        # Test the Google Sheets integration
        logger.info("📊 Fetching sales data from Google Sheets...")
        sales_data = await monitor.get_sales_from_spreadsheet(start_time_utc, end_time_utc)
        
        logger.info(f"📈 Sales found: {sales_data}")
        logger.info(f"📊 Total sales: {sum(sales_data.values())}")
        
        # Expected sales based on your spreadsheet:
        expected_sales = {
            'TDES008-YT': 3,  # 3:00:30 PM, 4:20:27 PM, 4:55:29 PM
            'TDES065-YT': 1,  # 4:10:28 PM
            'Koji Digital': 1  # 4:50:25 PM
        }
        
        logger.info(f"📋 Expected sales: {expected_sales}")
        logger.info(f"📊 Expected total: {sum(expected_sales.values())}")
        
        # Compare results
        logger.info("\n🔍 COMPARISON:")
        for publisher in expected_sales:
            expected = expected_sales[publisher]
            actual = sales_data.get(publisher, 0)
            status = "✅" if expected == actual else "❌"
            logger.info(f"   {status} {publisher}: Expected {expected}, Got {actual}")
        
        # Check if we're missing any publishers
        missing_publishers = set(expected_sales.keys()) - set(sales_data.keys())
        extra_publishers = set(sales_data.keys()) - set(expected_sales.keys())
        
        if missing_publishers:
            logger.warning(f"⚠️  Missing publishers: {missing_publishers}")
        if extra_publishers:
            logger.info(f"ℹ️  Extra publishers: {extra_publishers}")
        
        # Test with a wider time window to see if it's a timing issue
        logger.info("\n🕐 Testing with wider time window (2:00-6:00 PM EDT)...")
        wider_start = test_time_edt.replace(hour=14, minute=0, second=0, microsecond=0)
        wider_end = test_time_edt.replace(hour=18, minute=0, second=0, microsecond=0)
        wider_start_utc = wider_start.astimezone(timezone.utc)
        wider_end_utc = wider_end.astimezone(timezone.utc)
        
        wider_sales = await monitor.get_sales_from_spreadsheet(wider_start_utc, wider_end_utc)
        logger.info(f"📈 Wider window sales: {wider_sales}")
        logger.info(f"📊 Wider window total: {sum(wider_sales.values())}")
        
        return sales_data, expected_sales
        
    except Exception as e:
        logger.error(f"❌ Debug failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}, {}

async def main():
    """Run the debug test"""
    logger.info("🚀 Starting Spreadsheet Timing Debug...")
    
    actual_sales, expected_sales = await debug_spreadsheet_timing()
    
    if actual_sales == expected_sales:
        logger.info("✅ Perfect match! No timing issues found.")
    else:
        logger.info("⚠️  Discrepancy found. This needs investigation.")
        logger.info("This could be due to:")
        logger.info("1. Timezone conversion issues")
        logger.info("2. Date/time parsing issues in the spreadsheet")
        logger.info("3. Time window boundaries")
        logger.info("4. Data format differences")

if __name__ == "__main__":
    asyncio.run(main())
