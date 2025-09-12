#!/usr/bin/env python3
"""Test 1pm-3pm EST data with corrected CPA calculation (Payout / Completed Calls)"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

def safe_float(value, default=0.0):
    """Safely convert value to float, handling percentages"""
    if not value:
        return default
    try:
        clean_value = str(value).replace('%', '')
        return float(clean_value)
    except:
        return default

async def test_corrected_cpa():
    """Test 1pm-3pm EST data with correct CPA calculation"""
    
    # API credentials
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    slack_webhook = "https://hooks.slack.com/services/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"
    
    # Set up 1pm-3pm EST for 09/11/2025
    start_est = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))  # 1pm EST
    end_est = datetime(2025, 9, 11, 15, 0, 0, tzinfo=timezone(timedelta(hours=-5)))    # 3pm EST
    
    start_utc = start_est.astimezone(timezone.utc)
    end_utc = end_est.astimezone(timezone.utc)
    
    print(f"🕐 Testing 1pm-3pm EST data with CORRECTED CPA calculation")
    print(f"⏰ Start (UTC): {start_utc}")
    print(f"⏰ End (UTC): {end_utc}")
    print(f"📊 CPA Formula: Payout ÷ Completed Calls")
    
    # Ringba API call with full metrics
    url = f"https://api.ringba.com/v2/{ringba_account}/insights"
    headers = {
        "Authorization": f"Token {ringba_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "reportStart": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportEnd": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "groupByColumns": [{"column": "publisherName", "displayName": "Publisher"}],
        "valueColumns": [
            {"column": "callCount", "aggregateFunction": None},
            {"column": "liveCallCount", "aggregateFunction": None},
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "endedCalls", "aggregateFunction": None},
            {"column": "connectedCallCount", "aggregateFunction": None},
            {"column": "payoutCount", "aggregateFunction": None},
            {"column": "convertedCalls", "aggregateFunction": None},
            {"column": "nonConnectedCallCount", "aggregateFunction": None},
            {"column": "duplicateCalls", "aggregateFunction": None},
            {"column": "blockedCalls", "aggregateFunction": None},
            {"column": "incompleteCalls", "aggregateFunction": None},
            {"column": "earningsPerCallGross", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None},
            {"column": "profitGross", "aggregateFunction": None},
            {"column": "profitMarginGross", "aggregateFunction": None},
            {"column": "convertedPercent", "aggregateFunction": None},
            {"column": "callLengthInSeconds", "aggregateFunction": None},
            {"column": "avgHandleTime", "aggregateFunction": None},
            {"column": "totalCost", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "generateRollups": True,
        "maxResultsPerGroup": 1000,
        "filters": [],
        "formatTimeZone": "America/New_York"
    }
    
    print(f"🔗 Calling Ringba API...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                print(f"📡 Response status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Ringba API success!")
                    
                    if "report" in data and "records" in data["report"]:
                        records = data["report"]["records"]
                        print(f"📊 Found {len(records)} publishers")
                        
                        if records:
                            # Process all publishers with CORRECTED CPA calculation
                            publishers = []
                            for record in records:
                                publisher = {
                                    "name": record.get("publisherName", "Unknown"),
                                    "calls": int(safe_float(record.get("callCount", 0))),
                                    "completed": int(safe_float(record.get("completedCalls", 0))),
                                    "revenue": safe_float(record.get("conversionAmount", 0)),
                                    "payout": safe_float(record.get("payoutAmount", 0)),
                                    "profit": safe_float(record.get("profitGross", 0)),
                                    "conversion_rate": safe_float(record.get("convertedPercent", 0)),
                                    "connected": int(safe_float(record.get("connectedCallCount", 0))),
                                    "live": int(safe_float(record.get("liveCallCount", 0))),
                                    "ended": int(safe_float(record.get("endedCalls", 0))),
                                    "paid": int(safe_float(record.get("payoutCount", 0))),
                                    "converted": int(safe_float(record.get("convertedCalls", 0))),
                                    "no_connect": int(safe_float(record.get("nonConnectedCallCount", 0))),
                                    "duplicate": int(safe_float(record.get("duplicateCalls", 0))),
                                    "blocked": int(safe_float(record.get("blockedCalls", 0))),
                                    "ivr_hangup": int(safe_float(record.get("incompleteCalls", 0))),
                                    "tcl_seconds": int(safe_float(record.get("callLengthInSeconds", 0))),
                                    "acl_seconds": int(safe_float(record.get("avgHandleTime", 0))),
                                    "total_cost": safe_float(record.get("totalCost", 0))
                                }
                                # CORRECTED CPA calculation: Payout / Completed Calls
                                publisher["cpa"] = publisher["payout"] / publisher["completed"] if publisher["completed"] > 0 else 0.0
                                publishers.append(publisher)
                            
                            # Calculate totals
                            total_calls = sum(p["calls"] for p in publishers)
                            total_completed = sum(p["completed"] for p in publishers)
                            total_revenue = sum(p["revenue"] for p in publishers)
                            total_payout = sum(p["payout"] for p in publishers)
                            total_profit = sum(p["profit"] for p in publishers)
                            total_cpa = total_payout / total_completed if total_completed > 0 else 0
                            
                            print(f"\n📈 CORRECTED TOTALS (1pm-3pm EST):")
                            print(f"  • Total Calls: {total_calls:,}")
                            print(f"  • Completed Calls: {total_completed:,}")
                            print(f"  • Revenue: ${total_revenue:.2f}")
                            print(f"  • Payout: ${total_payout:.2f}")
                            print(f"  • Profit: ${total_profit:.2f}")
                            print(f"  • CPA (Payout/Completed): ${total_cpa:.2f}")
                            
                            # Show ALL publishers with CORRECTED CPA
                            print(f"\n📋 ALL PUBLISHERS with CORRECTED CPA:")
                            for i, p in enumerate(publishers, 1):
                                print(f"  {i:2d}. {p['name']:<20} | {p['completed']:3d} completed | ${p['cpa']:6.2f} CPA | ${p['payout']:8.2f} payout | ${p['revenue']:8.2f} revenue")
                            
                            # Send corrected report to Slack
                            slack_message = {
                                "text": f"🔧 CORRECTED CPA TEST: Ringba 1pm-3pm EST Data (09/11/2025)",
                                "blocks": [
                                    {
                                        "type": "header",
                                        "text": {
                                            "type": "plain_text",
                                            "text": "🔧 CORRECTED CPA TEST: Ringba 1pm-3pm EST Data (09/11/2025)"
                                        }
                                    },
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": f"*📊 Corrected Results (1pm-3pm EST on 09/11/2025):*\n• *Total Calls:* {total_calls:,}\n• *Completed Calls:* {total_completed:,}\n• *Revenue:* ${total_revenue:.2f}\n• *Payout:* ${total_payout:.2f}\n• *Profit:* ${total_profit:.2f}\n• *CPA (Payout/Completed):* ${total_cpa:.2f}"
                                        }
                                    }
                                ]
                            }
                            
                            # Add ALL publishers table with CORRECTED CPA
                            table_text = "*📋 ALL Publishers with CORRECTED CPA (Payout/Completed):*\n"
                            table_text += "```\n"
                            table_text += f"{'#':<3} {'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Payout':<10} {'Revenue':<10}\n"
                            table_text += "-" * 80 + "\n"
                            
                            for i, p in enumerate(publishers, 1):
                                table_text += f"{i:<3} {p['name'][:19]:<20} {p['completed']:<10} ${p['cpa']:<7.2f} ${p['payout']:<9.2f} ${p['revenue']:<9.2f}\n"
                            
                            table_text += "```"
                            
                            slack_message["blocks"].append({
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": table_text
                                }
                            })
                            
                            print(f"\n📤 Sending corrected report to Slack...")
                            
                            async with session.post(slack_webhook, json=slack_message) as response:
                                if response.status == 200:
                                    print(f"✅ Corrected Slack report sent successfully!")
                                    print(f"🎉 TEST COMPLETE: All {len(publishers)} publishers with CORRECTED CPA!")
                                else:
                                    error_text = await response.text()
                                    print(f"❌ Slack error {response.status}: {error_text}")
                        else:
                            print("⚠️  No data found for 1pm-3pm EST")
                    else:
                        print(f"⚠️  Unexpected data structure")
                else:
                    error_text = await response.text()
                    print(f"❌ Ringba API error {response.status}: {error_text}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_corrected_cpa())
