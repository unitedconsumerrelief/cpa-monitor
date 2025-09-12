#!/usr/bin/env python3
"""Test timezone calculations"""

from datetime import datetime, timedelta, timezone

# Test current time conversion
utc_now = datetime.now(timezone.utc)
print(f"UTC now: {utc_now}")

# Convert to EST
est_now = utc_now.astimezone(timezone(timedelta(hours=-5)))
print(f"EST now: {est_now}")

# Calculate 2-hour range in EST
end_time_est = est_now
start_time_est = est_now - timedelta(hours=2)

print(f"EST range: {start_time_est} to {end_time_est}")

# Convert back to UTC
end_time_utc = end_time_est.astimezone(timezone.utc)
start_time_utc = start_time_est.astimezone(timezone.utc)

print(f"UTC range: {start_time_utc} to {end_time_utc}")

# Test specific time: 11 AM - 1 PM EST
est_11am = est_now.replace(hour=11, minute=0, second=0, microsecond=0)
est_1pm = est_now.replace(hour=13, minute=0, second=0, microsecond=0)

print(f"EST 11am-1pm: {est_11am} to {est_1pm}")

utc_11am = est_11am.astimezone(timezone.utc)
utc_1pm = est_1pm.astimezone(timezone.utc)

print(f"UTC 11am-1pm: {utc_11am} to {utc_1pm}")
