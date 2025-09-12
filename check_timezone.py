#!/usr/bin/env python3
"""Check time zone conversion"""

from datetime import datetime, timezone, timedelta

# Our current conversion
start_est = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))  # 1pm EST
end_est = datetime(2025, 9, 11, 15, 0, 0, tzinfo=timezone(timedelta(hours=-5)))    # 3pm EST

start_utc = start_est.astimezone(timezone.utc)
end_utc = end_est.astimezone(timezone.utc)

print("EST times:")
print(f"  Start: {start_est}")
print(f"  End: {end_est}")
print("UTC times:")
print(f"  Start: {start_utc}")
print(f"  End: {end_utc}")
print("UTC strings:")
print(f"  Start: {start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}")
print(f"  End: {end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}")

# Check if this matches what Ringba expects
print("\nExpected for 1pm-3pm EST on 09/11/2025:")
print("  Should be 6pm-8pm UTC (EST is UTC-5)")
print(f"  Our conversion: {start_utc.hour}pm-{end_utc.hour}pm UTC")
