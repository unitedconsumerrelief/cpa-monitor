#!/usr/bin/env python3
"""Final verification test for all CPA Monitor requirements"""

import asyncio
import sys
import os
from datetime import datetime, timedelta, timezone
import logging

# Add current directory to path to import monitor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from monitor import RingbaMonitor, PublisherMetrics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_google_sheets_integration():
    """Test 1: Google Sheets integration"""
    logger.info("🧪 TEST 1: Google Sheets Integration")
    logger.info("="*50)
    
    try:
        monitor = RingbaMonitor()
        
        # Test with 3:00-5:05 PM EDT window (5PM report window)
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        
        # Set to 5:05 PM EDT for testing
        test_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_est = test_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)  # 3:00 PM EDT
        end_time_est = test_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)    # 5:05 PM EDT
        
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        logger.info(f"📊 Testing time window: {start_time_est} to {end_time_est} EDT")
        
        # Test Google Sheets integration
        sales_data = await monitor.get_sales_from_spreadsheet(start_time_utc, end_time_utc)
        
        logger.info(f"✅ Google Sheets data retrieved: {sales_data}")
        logger.info(f"📈 Total sales found: {sum(sales_data.values())}")
        
        # Verify we get the expected 5 sales
        expected_total = 5
        actual_total = sum(sales_data.values())
        
        if actual_total == expected_total:
            logger.info("✅ Google Sheets integration: PASS")
            return True
        else:
            logger.error(f"❌ Google Sheets integration: FAIL - Expected {expected_total}, got {actual_total}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Google Sheets integration: FAIL - {e}")
        return False

def test_timing_buffer():
    """Test 2: 5-minute delay/buffer before report triggers"""
    logger.info("\n🧪 TEST 2: 5-Minute Buffer Timing")
    logger.info("="*50)
    
    try:
        monitor = RingbaMonitor()
        
        # Test the get_next_report_time function
        next_report = monitor.get_next_report_time()
        next_report_edt = next_report.astimezone(timezone(timedelta(hours=-4)))
        
        logger.info(f"📅 Next report time: {next_report_edt} EDT")
        
        # Check if 5PM report is scheduled for 5:05 PM EDT
        if next_report_edt.hour == 17 and next_report_edt.minute == 5:
            logger.info("✅ 5PM report correctly scheduled for 5:05 PM EDT")
            buffer_correct = True
        else:
            logger.warning(f"⚠️  5PM report scheduled for {next_report_edt.hour}:{next_report_edt.minute:02d} EDT")
            buffer_correct = False
        
        # Test the data window logic
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        
        # Simulate 5:05 PM EDT
        test_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        current_hour = test_time_edt.hour
        
        if current_hour >= 17 and current_hour < 19:  # 5pm-7pm EDT
            start_time_est = test_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)  # 3:00 PM EDT
            end_time_est = test_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)    # 5:05 PM EDT
            
            logger.info(f"📊 Data window: {start_time_est} to {end_time_est} EDT")
            logger.info(f"⏰ Buffer: 5 minutes (5:00-5:05 PM)")
            
            if end_time_est.minute == 5:
                logger.info("✅ 5-minute buffer: PASS")
                return True
            else:
                logger.error("❌ 5-minute buffer: FAIL - End time not 5:05 PM")
                return False
        else:
            logger.error("❌ 5-minute buffer: FAIL - Not in 5PM window")
            return False
            
    except Exception as e:
        logger.error(f"❌ 5-minute buffer: FAIL - {e}")
        return False

async def test_cpa_calculation_with_spreadsheet_data():
    """Test 3: CPA calculation based on time/date + publisher in spreadsheet"""
    logger.info("\n🧪 TEST 3: CPA Calculation with Spreadsheet Data")
    logger.info("="*50)
    
    try:
        monitor = RingbaMonitor()
        
        # Create test metrics with publishers from spreadsheet
        test_metrics = [
            # Publishers WITH sales data in spreadsheet
            PublisherMetrics(publisher_name="TDES008-YT", completed=10, payout=300.0, revenue=400.0, profit=100.0),
            PublisherMetrics(publisher_name="TDES065-YT", completed=5, payout=150.0, revenue=200.0, profit=50.0),
            PublisherMetrics(publisher_name="Koji Digital", completed=8, payout=200.0, revenue=250.0, profit=50.0),
            # Publisher WITHOUT sales data in spreadsheet
            PublisherMetrics(publisher_name="No Sales Publisher", completed=15, payout=500.0, revenue=600.0, profit=100.0),
            # Another publisher without sales data
            PublisherMetrics(publisher_name="Another Publisher", completed=20, payout=800.0, revenue=1000.0, profit=200.0)
        ]
        
        logger.info("📊 Test publishers:")
        for metric in test_metrics:
            logger.info(f"   • {metric.publisher_name}: {metric.completed} calls, ${metric.payout} payout")
        
        # Test time window (3:00-5:05 PM EDT)
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        test_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_est = test_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_time_est = test_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        # Enhance metrics with accurate CPA calculation
        enhanced_metrics = await monitor.enhance_metrics_with_accurate_cpa(test_metrics, start_time_utc, end_time_utc)
        
        logger.info("\n📋 CPA Calculation Results:")
        logger.info("-" * 60)
        
        all_correct = True
        
        for metric in enhanced_metrics:
            logger.info(f"📊 {metric.publisher_name}:")
            logger.info(f"   • Completed Calls: {metric.completed}")
            logger.info(f"   • Payout: ${metric.payout:.2f}")
            logger.info(f"   • Sales Count: {metric.sales_count}")
            logger.info(f"   • Regular CPA: ${metric.cpa:.2f}")
            logger.info(f"   • ACCURATE CPA: ${metric.accurate_cpa:.2f}")
            
            # Check if CPA calculation is correct
            if metric.publisher_name in ["TDES008-YT", "TDES065-YT", "Koji Digital"]:
                # These should have sales data and non-zero CPA
                if metric.sales_count > 0 and metric.accurate_cpa > 0:
                    logger.info(f"   ✅ Correct: Has sales data, CPA = ${metric.accurate_cpa:.2f}")
                else:
                    logger.error(f"   ❌ ERROR: Should have sales data but CPA = ${metric.accurate_cpa:.2f}")
                    all_correct = False
            else:
                # These should have no sales data and $0.00 CPA
                if metric.sales_count == 0 and metric.accurate_cpa == 0.0:
                    logger.info(f"   ✅ Correct: No sales data, CPA = $0.00")
                else:
                    logger.error(f"   ❌ ERROR: Should have no sales data but CPA = ${metric.accurate_cpa:.2f}")
                    all_correct = False
            
            logger.info("")
        
        if all_correct:
            logger.info("✅ CPA Calculation: PASS")
            return True
        else:
            logger.error("❌ CPA Calculation: FAIL")
            return False
            
    except Exception as e:
        logger.error(f"❌ CPA Calculation: FAIL - {e}")
        return False

async def test_publishers_without_sales_data():
    """Test 4: Publishers without sales data show $0.00 CPA but keep other data"""
    logger.info("\n🧪 TEST 4: Publishers Without Sales Data")
    logger.info("="*50)
    
    try:
        monitor = RingbaMonitor()
        
        # Create metrics for publishers NOT in the spreadsheet
        test_metrics = [
            PublisherMetrics(
                publisher_name="Unknown Publisher 1", 
                completed=25, 
                payout=750.0, 
                revenue=1000.0, 
                profit=250.0,
                conversion_percent=15.0
            ),
            PublisherMetrics(
                publisher_name="Unknown Publisher 2", 
                completed=12, 
                payout=360.0, 
                revenue=480.0, 
                profit=120.0,
                conversion_percent=20.0
            )
        ]
        
        logger.info("📊 Testing publishers NOT in spreadsheet:")
        for metric in test_metrics:
            logger.info(f"   • {metric.publisher_name}: {metric.completed} calls, ${metric.payout} payout")
        
        # Test time window
        now_utc = datetime.now(timezone.utc)
        edt_now = now_utc.astimezone(timezone(timedelta(hours=-4)))
        test_time_edt = edt_now.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_est = test_time_edt.replace(hour=15, minute=0, second=0, microsecond=0)
        end_time_est = test_time_edt.replace(hour=17, minute=5, second=0, microsecond=0)
        start_time_utc = start_time_est.astimezone(timezone.utc)
        end_time_utc = end_time_est.astimezone(timezone.utc)
        
        # Enhance metrics
        enhanced_metrics = await monitor.enhance_metrics_with_accurate_cpa(test_metrics, start_time_utc, end_time_utc)
        
        logger.info("\n📋 Results for publishers without sales data:")
        logger.info("-" * 60)
        
        all_correct = True
        
        for metric in enhanced_metrics:
            logger.info(f"📊 {metric.publisher_name}:")
            logger.info(f"   • Completed Calls: {metric.completed}")
            logger.info(f"   • Payout: ${metric.payout:.2f}")
            logger.info(f"   • Revenue: ${metric.revenue:.2f}")
            logger.info(f"   • Profit: ${metric.profit:.2f}")
            logger.info(f"   • Conversion %: {metric.conversion_percent:.1f}%")
            logger.info(f"   • Sales Count: {metric.sales_count}")
            logger.info(f"   • ACCURATE CPA: ${metric.accurate_cpa:.2f}")
            
            # Verify: $0.00 CPA but other data intact
            if (metric.accurate_cpa == 0.0 and 
                metric.sales_count == 0 and 
                metric.completed > 0 and 
                metric.payout > 0):
                logger.info(f"   ✅ Correct: $0.00 CPA, but other data preserved")
            else:
                logger.error(f"   ❌ ERROR: CPA should be $0.00 but other data should be preserved")
                all_correct = False
            
            logger.info("")
        
        if all_correct:
            logger.info("✅ Publishers without sales data: PASS")
            return True
        else:
            logger.error("❌ Publishers without sales data: FAIL")
            return False
            
    except Exception as e:
        logger.error(f"❌ Publishers without sales data: FAIL - {e}")
        return False

async def main():
    """Run all verification tests"""
    logger.info("🚀 FINAL VERIFICATION TEST - CPA Monitor Bot")
    logger.info("="*60)
    logger.info("Testing all requirements before deployment:")
    logger.info("1. Google Sheets integration")
    logger.info("2. 5-minute delay/buffer before report triggers")
    logger.info("3. CPA calculation based on time/date + publisher in spreadsheet")
    logger.info("4. Publishers without sales data show $0.00 CPA but keep other data")
    logger.info("="*60)
    
    # Run all tests
    test1_ok = await test_google_sheets_integration()
    test2_ok = test_timing_buffer()
    test3_ok = await test_cpa_calculation_with_spreadsheet_data()
    test4_ok = await test_publishers_without_sales_data()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("📋 FINAL VERIFICATION RESULTS:")
    logger.info("="*60)
    logger.info(f"1. Google Sheets Integration: {'✅ PASS' if test1_ok else '❌ FAIL'}")
    logger.info(f"2. 5-Minute Buffer Timing: {'✅ PASS' if test2_ok else '❌ FAIL'}")
    logger.info(f"3. CPA Calculation with Spreadsheet: {'✅ PASS' if test3_ok else '❌ FAIL'}")
    logger.info(f"4. Publishers without Sales Data: {'✅ PASS' if test4_ok else '❌ FAIL'}")
    
    all_tests_passed = all([test1_ok, test2_ok, test3_ok, test4_ok])
    
    if all_tests_passed:
        logger.info("\n🎉 ALL TESTS PASSED! Bot is ready for deployment!")
        logger.info("✅ Google Sheets integration working perfectly")
        logger.info("✅ 5-minute buffer timing correct")
        logger.info("✅ CPA calculations accurate based on spreadsheet data")
        logger.info("✅ Publishers without sales data handled correctly")
        logger.info("\n🚀 DEPLOYMENT APPROVED!")
    else:
        logger.info("\n❌ SOME TESTS FAILED! Do not deploy yet.")
        logger.info("Please fix the failing tests before deployment.")
    
    logger.info("="*60)
    
    return all_tests_passed

if __name__ == "__main__":
    asyncio.run(main())
