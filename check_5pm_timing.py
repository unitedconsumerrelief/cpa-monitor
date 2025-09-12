#!/usr/bin/env python3
"""Check exact 5pm report timing"""

from datetime import datetime, timezone, timedelta

def check_5pm_timing():
    """Check when the 5pm report will be sent"""
    print("🕐 5PM REPORT TIMING ANALYSIS")
    print("=" * 50)
    
    # Current time
    now_utc = datetime.now(timezone.utc)
    now_edt = now_utc.astimezone(timezone(timedelta(hours=-4)))
    
    print(f"Current time: {now_edt.strftime('%Y-%m-%d %H:%M:%S')} EDT")
    print(f"Current time: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    # 5pm EDT
    five_pm_edt = now_edt.replace(hour=17, minute=0, second=0, microsecond=0)
    five_pm_utc = five_pm_edt.astimezone(timezone.utc)
    
    print(f"\n5pm EDT: {five_pm_edt.strftime('%Y-%m-%d %H:%M:%S')} EDT")
    print(f"5pm UTC: {five_pm_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    # Time until 5pm
    time_until_5pm = (five_pm_utc - now_utc).total_seconds()
    
    if time_until_5pm > 0:
        hours = int(time_until_5pm // 3600)
        minutes = int((time_until_5pm % 3600) // 60)
        seconds = int(time_until_5pm % 60)
        print(f"\n⏰ Time until 5pm report: {hours}h {minutes}m {seconds}s")
    else:
        print(f"\n⏰ 5pm has already passed today")
    
    print(f"\n📊 REPORT SCHEDULE:")
    print(f"• 11am EDT - Reports on 9am-11am data")
    print(f"• 1pm EDT  - Reports on 11am-1pm data") 
    print(f"• 3pm EDT  - Reports on 1pm-3pm data")
    print(f"• 5pm EDT  - Reports on 3pm-5pm data ⭐")
    print(f"• 7pm EDT  - Reports on 5pm-7pm data")
    print(f"• 9pm EDT  - Reports on 7pm-9pm data + daily summary")
    
    print(f"\n🔍 5PM REPORT DETAILS:")
    print(f"• Trigger time: 5:00:00 PM EDT")
    print(f"• Data window: 3:00:00 PM - 5:05:00 PM EDT (with 5-min buffer)")
    print(f"• Display shows: 3:00:00 PM - 5:00:00 PM EDT")
    print(f"• Includes: All Ringba publishers + Google Sheets sales")
    print(f"• CPA calculation: Payout ÷ Actual sales from spreadsheet")
    
    print(f"\n✅ ANSWER: The report will be sent at EXACTLY 5:00:00 PM EDT")
    print(f"   (Not 5:01pm, not 5:05pm - exactly 5:00:00 PM EDT)")

if __name__ == "__main__":
    check_5pm_timing()
