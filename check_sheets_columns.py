#!/usr/bin/env python3
"""Check Google Sheets column structure"""

import asyncio
import aiohttp
import pandas as pd
import io

async def check_columns():
    """Check what columns are in the Google Sheets"""
    print("🔍 CHECKING GOOGLE SHEETS COLUMNS")
    print("=" * 50)
    
    try:
        sheets_url = "https://docs.google.com/spreadsheets/d/1yPWM2CIjPcAg1pF7xNUDmt22kbS2qrKqsOgWkIJzd9I/export?format=csv&gid=0"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(sheets_url) as response:
                if response.status == 200:
                    csv_content = await response.text()
                    df = pd.read_csv(io.StringIO(csv_content))
                    
                    print(f"✅ Total rows: {len(df)}")
                    print(f"✅ Total columns: {len(df.columns)}")
                    print("\n📊 ALL COLUMNS:")
                    for i, col in enumerate(df.columns):
                        print(f"  {i+1:2d}. '{col}'")
                    
                    print("\n🔍 LOOKING FOR PUBLISHER/DATE/TIME COLUMNS:")
                    for i, col in enumerate(df.columns):
                        col_lower = col.lower()
                        if any(keyword in col_lower for keyword in ['publisher', 'pub', 'name']):
                            print(f"  📍 Publisher candidate: Column {i+1} = '{col}'")
                        if any(keyword in col_lower for keyword in ['date', 'time', 'timestamp']):
                            print(f"  📍 Date/Time candidate: Column {i+1} = '{col}'")
                    
                    # Check if Q, R, S exist
                    if 'Q' in df.columns:
                        print(f"\n✅ Column Q exists: '{df['Q'].iloc[0] if len(df) > 0 else 'Empty'}'")
                    if 'R' in df.columns:
                        print(f"✅ Column R exists: '{df['R'].iloc[0] if len(df) > 0 else 'Empty'}'")
                    if 'S' in df.columns:
                        print(f"✅ Column S exists: '{df['S'].iloc[0] if len(df) > 0 else 'Empty'}'")
                    
                    return True
                else:
                    print(f"❌ Failed to access Google Sheets: {response.status}")
                    return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(check_columns())
