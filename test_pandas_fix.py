#!/usr/bin/env python3
"""Test pandas fix specifically"""

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

async def test_pandas_import():
    """Test that pandas can be imported in the monitor module"""
    logger.info("🧪 Testing pandas import in monitor module...")
    
    try:
        # Test direct pandas import
        import pandas as pd
        logger.info(f"✅ Direct pandas import successful: version {pd.__version__}")
        
        # Test pandas import within the monitor context
        monitor = RingbaMonitor()
        
        # Test the Google Sheets function that uses pandas
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=2)
        end_time = now
        
        logger.info("📊 Testing Google Sheets integration with pandas...")
        sales_data = await monitor.get_sales_from_spreadsheet(start_time, end_time)
        
        logger.info(f"✅ Google Sheets integration successful!")
        logger.info(f"📈 Sales data: {sales_data}")
        
        return True
        
    except ImportError as e:
        logger.error(f"❌ Pandas import failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Google Sheets integration failed: {e}")
        return False

async def main():
    """Test pandas fix"""
    logger.info("🚀 Testing Pandas Fix...")
    
    success = await test_pandas_import()
    
    if success:
        logger.info("✅ PANDAS FIX CONFIRMED - No more 'No module named pandas' error!")
    else:
        logger.info("❌ PANDAS ISSUE STILL EXISTS - Need to fix before deployment")

if __name__ == "__main__":
    asyncio.run(main())
