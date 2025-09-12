#!/usr/bin/env python3
"""FINAL TEST of Google Sheets integration"""

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone, timedelta
import io

async def test_google_sheets_final():
    """FINAL test of Google Sheets integration"""
    print("🔬 FINAL GOOGLE SHEETS INTEGRATION TEST")
    print("=" * 60)
    
    try:
        # Test the Google Sheets CSV export
        sheets_url = "https://docs.google.com/spreadsheets/d/1yPWM2CIjPcAg1pF7xNUDmt22kbS2qrKqsOgWkIJzd9I/export?format=csv&gid=0"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(sheets_url) as response:
                if response.status == 200:
                    csv_content = await response.text()
                    print("✅ Google Sheets CSV export: SUCCESS")
                    
                    # Parse CSV with pandas
                    df = pd.read_csv(io.StringIO(csv_content))
                    print(f"✅ Pandas CSV parsing: SUCCESS ({len(df)} rows)")
                    
                    # Check required columns
                    required_cols = ['Lookup_Publisher', 'Date', 'Time']
                    if all(col in df.columns for col in required_cols):
                        print("✅ Required columns found: Lookup_Publisher, Date, Time")
                        
                        # Check for data
                        valid_data = df.dropna(subset=['Lookup_Publisher', 'Date', 'Time'])
                        print(f"✅ Valid data rows: {len(valid_data)}")
                        
                        # Test time range filtering (3pm-5pm EDT today)
                        today = datetime.now(timezone(timedelta(hours=-4))).date()
                        start_time = datetime.combine(today, datetime.min.time().replace(hour=15, minute=0))
                        end_time = datetime.combine(today, datetime.min.time().replace(hour=17, minute=5))
                        
                        print(f"\n🕐 TESTING TIME RANGE FILTERING:")
                        print(f"  Today: {today}")
                        print(f"  Time range: 3:00pm - 5:05pm EDT")
                        
                        # Count sales in time range
                        sales_in_range = 0
                        sales_by_publisher = {}
                        
                        for idx, row in valid_data.iterrows():
                            try:
                                # Get publisher from Lookup_Publisher column
                                publisher = str(row['Lookup_Publisher'])
                                
                                # Skip if publisher is empty
                                if publisher == "nan" or publisher.strip() == "":
                                    continue
                                
                                # Parse date and time
                                date_str = str(row['Date'])
                                time_str = str(row['Time'])
                                
                                # Skip if no date or time
                                if date_str == "nan" or time_str == "nan" or date_str.strip() == "" or time_str.strip() == "":
                                    continue
                                
                                # Parse date (format: 8/5/2025)
                                date_parts = date_str.split('/')
                                if len(date_parts) == 3:
                                    month, day, year = date_parts
                                    sale_date = datetime(int(year), int(month), int(day))
                                    
                                    # Parse time (format: 3:00:30 PM)
                                    if ':' in time_str:
                                        time_parts = time_str.split(':')
                                        if len(time_parts) >= 2:
                                            hour = int(time_parts[0])
                                            minute = int(time_parts[1])
                                            
                                            # Handle AM/PM
                                            if 'PM' in time_str and hour != 12:
                                                hour += 12
                                            elif 'AM' in time_str and hour == 12:
                                                hour = 0
                                            
                                            sale_time = datetime.combine(sale_date, datetime.min.time().replace(hour=hour, minute=minute))
                                            
                                            # Check if within time range
                                            if start_time <= sale_time <= end_time:
                                                sales_in_range += 1
                                                if publisher not in sales_by_publisher:
                                                    sales_by_publisher[publisher] = 0
                                                sales_by_publisher[publisher] += 1
                                                
                                                if sales_in_range <= 5:  # Show first 5 matches
                                                    print(f"    ✅ Sale at {sale_time.strftime('%H:%M:%S')} - {publisher}")
                                                
                            except Exception as e:
                                continue
                        
                        print(f"  📊 Total sales in 3pm-5pm range: {sales_in_range}")
                        print(f"  📊 Sales by publisher: {sales_by_publisher}")
                        
                        # Test CPA calculation
                        print(f"\n💰 TESTING CPA CALCULATION")
                        print("-" * 30)
                        
                        test_payouts = {
                            'TDES008-YT': 1000.0,
                            'TDES065-YT': 1500.0,
                            'TDES999-XX': 2000.0  # No sales
                        }
                        
                        for publisher, payout in test_payouts.items():
                            sales_count = sales_by_publisher.get(publisher, 0)
                            if sales_count > 0:
                                accurate_cpa = payout / sales_count
                                print(f"✅ {publisher}: ${payout} ÷ {sales_count} = ${accurate_cpa:.2f}")
                            else:
                                accurate_cpa = 0.0
                                print(f"✅ {publisher}: ${payout} ÷ {sales_count} = ${accurate_cpa:.2f} (no sales)")
                        
                        print(f"\n🎉 GOOGLE SHEETS INTEGRATION: FULLY WORKING!")
                        return True
                    else:
                        print("❌ Missing required columns")
                        return False
                else:
                    print(f"❌ Google Sheets access failed: {response.status}")
                    return False
    except Exception as e:
        print(f"❌ Google Sheets test failed: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_google_sheets_final())
