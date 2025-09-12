#!/usr/bin/env python3
"""Calculate accurate CPA by combining Ringba payout data with spreadsheet sales data"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import pandas as pd
from io import StringIO

class AccurateCPACalculator:
    def __init__(self):
        self.ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
        self.ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
        self.sheets_service = None
        
    async def setup_sheets_service(self):
        """Setup Google Sheets service"""
        # This would need to be implemented with your Google Sheets credentials
        # For now, we'll simulate the data
        pass
    
    async def get_sales_from_spreadsheet(self, start_time: datetime, end_time: datetime) -> Dict[str, int]:
        """Get sales count per publisher from spreadsheet for the given time range"""
        # Convert to EDT for spreadsheet comparison
        start_edt = start_time.astimezone(timezone(timedelta(hours=-4)))
        end_edt = end_time.astimezone(timezone(timedelta(hours=-4)))
        
        print(f"📊 Checking spreadsheet for sales between {start_edt} and {end_edt} EDT")
        
        # TODO: Replace with actual Google Sheets API call
        # For now, simulate the data based on your example
        sales_data = {
            "TDES008-YT": 1,  # Based on your example at 3:00:30 PM
            "Koji Digital": 0,
            "FITZ": 0,
            "TDES023-YT": 0,
            # Add other publishers as needed
        }
        
        print(f"📈 Sales found: {sales_data}")
        return sales_data
    
    async def get_ringba_payout_data(self, start_time: datetime, end_time: datetime) -> Dict[str, float]:
        """Get payout data from Ringba API for the given time range"""
        url = f"https://api.ringba.com/v2/{self.ringba_account}/insights"
        headers = {"Authorization": f"Token {self.ringba_token}", "Content-Type": "application/json"}
        
        payload = {
            "reportStart": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reportEnd": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
            "valueColumns": [
                {"column": "payoutAmount", "aggregateFunction": None},
                {"column": "completedCalls", "aggregateFunction": None},
            ],
            "orderByColumns": [{"column": "payoutAmount", "direction": "desc"}],
            "formatTimespans": True,
            "formatPercentages": True,
            "maxResultsPerGroup": 1000,
            "filters": [],
            "formatTimeZone": "America/New_York"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        payout_data = {}
                        if "report" in data and "records" in data["report"]:
                            for row in data["report"]["records"]:
                                publisher = row.get("publisherName", "Unknown")
                                payout = float(row.get("payoutAmount", 0))
                                completed = int(row.get("completedCalls", 0))
                                
                                if payout > 0:  # Only include publishers with payouts
                                    payout_data[publisher] = {
                                        "payout": payout,
                                        "completed": completed
                                    }
                        
                        print(f"💰 Ringba payouts: {payout_data}")
                        return payout_data
                    else:
                        print(f"❌ Ringba API error: {response.status}")
                        return {}
        except Exception as e:
            print(f"❌ Error fetching Ringba data: {e}")
            return {}
    
    def calculate_accurate_cpa(self, sales_data: Dict[str, int], payout_data: Dict[str, Dict]) -> Dict[str, Dict]:
        """Calculate accurate CPA using sales count and payout data"""
        accurate_cpa = {}
        
        print("\n🎯 CALCULATING ACCURATE CPA:")
        print("=" * 60)
        print(f"{'Publisher':<20} {'Sales':<8} {'Payout':<10} {'CPA':<10} {'Old CPA':<10}")
        print("-" * 60)
        
        for publisher, sales_count in sales_data.items():
            if publisher in payout_data:
                payout = payout_data[publisher]["payout"]
                completed = payout_data[publisher]["completed"]
                
                # Calculate accurate CPA
                if sales_count > 0:
                    accurate_cpa_value = payout / sales_count
                else:
                    accurate_cpa_value = 0.0
                
                # Old CPA calculation (payout / completed calls)
                old_cpa_value = payout / completed if completed > 0 else 0.0
                
                accurate_cpa[publisher] = {
                    "sales_count": sales_count,
                    "payout": payout,
                    "completed_calls": completed,
                    "accurate_cpa": accurate_cpa_value,
                    "old_cpa": old_cpa_value
                }
                
                print(f"{publisher[:19]:<20} {sales_count:<8} ${payout:<9.2f} ${accurate_cpa_value:<9.2f} ${old_cpa_value:<9.2f}")
            else:
                print(f"{publisher[:19]:<20} {sales_count:<8} {'N/A':<10} {'N/A':<10} {'N/A':<10}")
        
        return accurate_cpa
    
    async def run_accurate_cpa_calculation(self, start_time: datetime, end_time: datetime):
        """Run the complete accurate CPA calculation process"""
        print(f"🚀 CALCULATING ACCURATE CPA FOR {start_time} to {end_time}")
        print("=" * 80)
        
        # Get sales data from spreadsheet
        sales_data = await self.get_sales_from_spreadsheet(start_time, end_time)
        
        # Get payout data from Ringba
        payout_data = await self.get_ringba_payout_data(start_time, end_time)
        
        # Calculate accurate CPA
        accurate_cpa = self.calculate_accurate_cpa(sales_data, payout_data)
        
        # Summary
        print("\n📊 SUMMARY:")
        print("=" * 40)
        total_sales = sum(sales_data.values())
        total_payout = sum(data["payout"] for data in payout_data.values())
        
        print(f"Total Sales: {total_sales}")
        print(f"Total Payout: ${total_payout:.2f}")
        if total_sales > 0:
            print(f"Overall CPA: ${total_payout / total_sales:.2f}")
        
        return accurate_cpa

async def main():
    """Test the accurate CPA calculation"""
    calculator = AccurateCPACalculator()
    
    # Test with 3pm-5pm EDT range (based on your example)
    start_time = datetime(2025, 9, 12, 19, 0, 0, tzinfo=timezone.utc)  # 3pm EDT
    end_time = datetime(2025, 9, 12, 21, 0, 0, tzinfo=timezone.utc)    # 5pm EDT
    
    await calculator.run_accurate_cpa_calculation(start_time, end_time)

if __name__ == "__main__":
    asyncio.run(main())
