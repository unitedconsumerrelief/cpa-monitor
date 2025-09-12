#!/usr/bin/env python3
"""Fix to display 'X Sales / No Payout' as text instead of numbers"""

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

async def create_corrected_report_with_text_display():
    """Create report with 'X Sales / No Payout' as text display"""
    logger.info("🚀 Creating corrected report with 'X Sales / No Payout' text display...")
    
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
                # Store the text display in a custom field - shorter text for better alignment
                publisher.sales_display = f"{sales_count} Sales/No$"
                publisher.accurate_cpa = 0.0  # Keep as 0 for calculations
                logger.info(f"📊 {publisher.publisher_name}: {publisher.sales_display}")
            else:
                # Normal CPA calculation: Payout ÷ Sales Count
                if sales_count > 0:
                    publisher.accurate_cpa = publisher.payout / sales_count
                else:
                    publisher.accurate_cpa = 0.0
                publisher.sales_display = None
                logger.info(f"📊 {publisher.publisher_name}: ${publisher.accurate_cpa:.2f} CPA")
            
            # Update the publisher with sales count
            publisher.sales_count = sales_count
            
            corrected_metrics.append(publisher)
        
        # Send the corrected report to Slack
        logger.info("📤 Sending corrected report with 'X Sales / No Payout' text display...")
        await monitor.send_slack_summary(corrected_metrics, corrected_metrics, start_time_utc, end_time_utc, start_time_utc)
        
        logger.info("✅ Corrected report sent successfully to Slack!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create corrected report: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

async def main():
    """Create corrected report with proper text display"""
    logger.info("🚀 Fixing Report with Proper Text Display")
    logger.info("This will show 'X Sales / No Payout' as text instead of numbers")
    
    success = await create_corrected_report_with_text_display()
    
    if success:
        logger.info("\n🎉 SUCCESS! Corrected report sent with proper text display!")
        logger.info("Publishers with zero payout but sales will now show 'X Sales / No Payout' as text.")
        logger.info("Check your Slack channel to see the corrected report.")
    else:
        logger.info("\n❌ Failed to create the corrected report.")

if __name__ == "__main__":
    asyncio.run(main())
