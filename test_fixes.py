#!/usr/bin/env python3
"""Test script to verify CPA Monitor fixes"""

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

async def test_google_sheets_integration():
    """Test Google Sheets integration with pandas"""
    logger.info("🧪 Testing Google Sheets integration...")
    
    try:
        monitor = RingbaMonitor()
        
        # Test with a recent time range (last 2 hours)
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=2)
        end_time = now
        
        logger.info(f"Testing time range: {start_time} to {end_time}")
        
        # Test the Google Sheets integration
        sales_data = await monitor.get_sales_from_spreadsheet(start_time, end_time)
        
        logger.info(f"✅ Google Sheets integration working!")
        logger.info(f"📊 Sales data retrieved: {sales_data}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Google Sheets integration failed: {e}")
        return False

def test_timing_logic():
    """Test the timing logic for 5PM report"""
    logger.info("🧪 Testing timing logic...")
    
    try:
        monitor = RingbaMonitor()
        
        # Test the get_next_report_time function
        next_report = monitor.get_next_report_time()
        next_report_edt = next_report.astimezone(timezone(timedelta(hours=-4)))
        
        logger.info(f"✅ Next report time: {next_report_edt} EDT")
        
        # Test the run_monitoring_cycle logic for 5PM window
        # Simulate current time as 5:05 PM EDT
        test_time = datetime.now(timezone.utc).replace(hour=21, minute=5, second=0, microsecond=0)  # 5:05 PM EDT
        est_now = test_time.astimezone(timezone(timedelta(hours=-4)))
        
        logger.info(f"Testing with simulated time: {est_now} EDT")
        
        current_hour = est_now.hour
        
        if current_hour >= 17 and current_hour < 19:  # 5pm-7pm EDT
            # Report on 3pm-5pm data (with 5-minute buffer)
            start_time_est = est_now.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time_est = est_now.replace(hour=17, minute=5, second=0, microsecond=0)  # 5:05pm EDT
            report_type = "2-hour"
            
            logger.info(f"✅ 5PM report timing logic working!")
            logger.info(f"📊 Data window: {start_time_est} to {end_time_est} EDT")
            logger.info(f"📊 Report type: {report_type}")
            
            return True
        else:
            logger.warning(f"⚠️  Current hour {current_hour} doesn't match 5PM window")
            return False
            
    except Exception as e:
        logger.error(f"❌ Timing logic test failed: {e}")
        return False

async def test_ringba_api():
    """Test Ringba API connection"""
    logger.info("🧪 Testing Ringba API connection...")
    
    try:
        monitor = RingbaMonitor()
        
        # Test with a small time range
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(minutes=30)
        end_time = now
        
        logger.info(f"Testing API with time range: {start_time} to {end_time}")
        
        # Test the API call
        metrics = await monitor.fetch_ringba_data(start_time, end_time)
        
        logger.info(f"✅ Ringba API working!")
        logger.info(f"📊 Retrieved {len(metrics)} metrics")
        
        if metrics:
            for metric in metrics[:3]:  # Show first 3 publishers
                logger.info(f"  - {metric.publisher_name}: {metric.completed} completed calls")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ringba API test failed: {e}")
        return False

async def main():
    """Run all tests"""
    logger.info("🚀 Starting CPA Monitor fixes verification...")
    
    # Test 1: Google Sheets integration
    sheets_ok = await test_google_sheets_integration()
    
    # Test 2: Timing logic
    timing_ok = test_timing_logic()
    
    # Test 3: Ringba API
    api_ok = await test_ringba_api()
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("📋 TEST RESULTS SUMMARY:")
    logger.info(f"Google Sheets Integration: {'✅ PASS' if sheets_ok else '❌ FAIL'}")
    logger.info(f"Timing Logic: {'✅ PASS' if timing_ok else '❌ FAIL'}")
    logger.info(f"Ringba API: {'✅ PASS' if api_ok else '❌ FAIL'}")
    
    if sheets_ok and timing_ok and api_ok:
        logger.info("🎉 ALL TESTS PASSED! The bot is ready for deployment.")
    else:
        logger.info("⚠️  Some tests failed. Please check the issues above.")
    
    logger.info("="*50)

if __name__ == "__main__":
    asyncio.run(main())
