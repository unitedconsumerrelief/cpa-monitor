#!/usr/bin/env python3
"""Test updated Google Sheets integration"""

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone, timedelta
import io

async def test_updated_sheets():
    """Test the updated Google Sheets integration"""
    print("🔍 TESTING UPDATED GOOGLE SHEETS INTEGRATION")
    print("=" * 60)
    
    try:
        # Test the Google Sheets CSV export
        sheets_url = "https://docs.google.com/spreadsheets/d/1yPWM2CIjPcAg1pF7xNUDmt22kbS2qrKqsOgWkIJzd9I/export?format=csv&gid=0"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(sheets_url) as response:
                if response.status == 200:
                    csv_content = await response.text()
                    print("✅ Google Sheets CSV export: SUCCESS")
                    
                    # Parse CSV
                    df = pd.read_csv(io.StringIO(csv_content))
                    print(f"✅ CSV parsing: SUCCESS ({len(df)} rows)")
                    
                    # Check required columns
                    required_cols = ['Lookup_Publisher', 'Date', 'Time']
                    if all(col in df.columns for col in required_cols):
                        print("✅ Required columns found: Lookup_Publisher, Date, Time")
                        
                        # Check for data
                        valid_data = df.dropna(subset=['Lookup_Publisher', 'Date', 'Time'])
                        print(f"✅ Valid data rows: {len(valid_data)}")
                        
                        # Show sample data
                        if len(valid_data) > 0:
                            print("\n📊 SAMPLE DATA:")
                            sample = valid_data.head(5)
                            for idx, row in sample.iterrows():
                                publisher = str(row['Lookup_Publisher'])
                                date = str(row['Date'])
                                time = str(row['Time'])
                                print(f"  Row {idx}: Publisher='{publisher}', Date='{date}', Time='{time}'")
                        
                        # Test time range filtering (3pm-5pm EDT today)
                        today = datetime.now(timezone(timedelta(hours=-4))).date()
                        start_time = datetime.combine(today, datetime.min.time().replace(hour=15, minute=0))
                        end_time = datetime.combine(today, datetime.min.time().replace(hour=17, minute=5))
                        
                        print(f"\n🕐 TESTING TIME RANGE FILTERING:")
                        print(f"  Today: {today}")
                        print(f"  Time range: 3:00pm - 5:05pm EDT")
                        
                        # Count sales in time range
                        sales_in_range = 0
                        for idx, row in valid_data.iterrows():
                            try:
                                # Parse date and time
                                date_str = str(row['Date'])
                                time_str = str(row['Time'])
                                
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
                                                if sales_in_range <= 3:  # Show first 3 matches
                                                    print(f"    ✅ Sale at {sale_time.strftime('%H:%M:%S')} - {row['Lookup_Publisher']}")
                            except Exception as e:
                                continue
                        
                        print(f"  📊 Total sales in 3pm-5pm range: {sales_in_range}")
                        
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
    asyncio.run(test_updated_sheets())
