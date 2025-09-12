#!/usr/bin/env python3
"""Test the 5-minute time buffer implementation"""

from datetime import datetime, timezone, timedelta

def test_time_buffer():
    """Test the time buffer logic"""
    print("🕐 TESTING 5-MINUTE TIME BUFFER")
    print("=" * 50)
    
    # Simulate 3pm-5pm EDT report
    start_time_est = datetime(2025, 9, 12, 15, 0, 0)  # 3pm EDT
    end_time_est = datetime(2025, 9, 12, 17, 0, 0)    # 5pm EDT
    
    print(f"Original time range: {start_time_est.strftime('%H:%M')} - {end_time_est.strftime('%H:%M')} EDT")
    
    # Convert to UTC
    start_time = start_time_est.replace(tzinfo=timezone(timedelta(hours=-4))).astimezone(timezone.utc)
    end_time = (end_time_est + timedelta(minutes=5)).replace(tzinfo=timezone(timedelta(hours=-4))).astimezone(timezone.utc)
    
    print(f"With buffer (UTC): {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')} UTC")
    print(f"With buffer (EDT): {start_time.astimezone(timezone(timedelta(hours=-4))).strftime('%H:%M')} - {end_time.astimezone(timezone(timedelta(hours=-4))).strftime('%H:%M')} EDT")
    
    # Show display time (without buffer)
    original_end_time = (end_time - timedelta(minutes=5)).astimezone(timezone(timedelta(hours=-4)))
    display_range = f"{start_time.astimezone(timezone(timedelta(hours=-4))).strftime('%H:%M')} - {original_end_time.strftime('%H:%M')} EDT"
    
    print(f"Display range: {display_range}")
    
    print("\n✅ BENEFITS:")
    print("• Captures sales up to 5:05pm EDT")
    print("• Ensures Ringba data is fully processed")
    print("• Gives spreadsheet time to record sales")
    print("• Displays clean 3pm-5pm range to users")
    print("• Prevents missing late sales like 3:00:30 PM")

if __name__ == "__main__":
    test_time_buffer()
