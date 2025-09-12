#!/usr/bin/env python3
"""Fix zero payout publishers to show sales count instead of $0.00"""

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

async def create_corrected_report_with_sales_display():
    """Create report with sales count displayed for zero payout publishers"""
    logger.info("🚀 Creating corrected report with sales count for zero payout publishers...")
    
    try:
        monitor = RingbaMonitor()
        
        # Get ALL real publishers from Ringba
        now_utc = datetime.now(timezone.utc)
        start_time = now_utc - timedelta(hours=8)  # Last 8 hours
        end_time = now_utc
        
        all_publishers = await monitor.fetch_ringba_data(start_time, end_time)
        
        if not all_publishers:
            logger.error("❌ No real publishers found from Ringba")
            return False
        
        # Get sales count from Google Sheets for 3:00-5:05 PM window
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        report_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_est = report_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_time_est = report_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        sales_data = await monitor.get_sales_from_spreadsheet(start_time_utc, end_time_utc)
        logger.info(f"📊 Sales data: {sales_data}")
        
        # Create corrected metrics for ALL publishers
        corrected_metrics = []
        
        for publisher in all_publishers:
            # Get sales count for this publisher (0 if not in Google Sheets)
            sales_count = sales_data.get(publisher.publisher_name, 0)
            
            # NEW RULE: If no payout in Ringba but has sales in spreadsheet
            if publisher.payout == 0.0 and sales_count > 0:
                # Show sales count instead of $0.00
                accurate_cpa = sales_count  # Display sales count as the value
                logger.info(f"📊 {publisher.publisher_name}: {sales_count} Sales / No Payout")
            else:
                # Normal CPA calculation: Payout ÷ Sales Count
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
            logger.info(f"   • Payout: ${publisher.payout:.2f}")
            logger.info(f"   • Sales Count: {sales_count}")
            if publisher.payout == 0.0 and sales_count > 0:
                logger.info(f"   • DISPLAY: {sales_count} Sales / No Payout")
            else:
                logger.info(f"   • ACCURATE CPA: ${accurate_cpa:.2f}")
            logger.info("")
        
        # Send the corrected report to Slack
        logger.info("📤 Sending corrected report with sales count for zero payout publishers...")
        await monitor.send_slack_summary(corrected_metrics, corrected_metrics, start_time_utc, end_time_utc, start_time_utc)
        
        logger.info("✅ Corrected report sent successfully to Slack!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create corrected report: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

async def main():
    """Create corrected report with new rule for zero payout publishers"""
    logger.info("🚀 Fixing Report with New Rule for Zero Payout Publishers")
    logger.info("NEW RULE: If no payout in Ringba but has sales in spreadsheet:")
    logger.info("  → Display sales count instead of $0.00")
    logger.info("  → Example: '3 Sales / No Payout'")
    
    success = await create_corrected_report_with_sales_display()
    
    if success:
        logger.info("\n🎉 SUCCESS! Corrected report sent with new rule!")
        logger.info("Publishers with zero payout but sales will now show sales count.")
        logger.info("Check your Slack channel to see the corrected report.")
    else:
        logger.info("\n❌ Failed to create the corrected report.")

if __name__ == "__main__":
    asyncio.run(main())
