#!/usr/bin/env python3
"""Final test with proper percentage handling"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

def safe_float(value, default=0.0):
    """Safely convert value to float, handling percentages"""
    if not value:
        return default
    try:
        # Remove % and convert to float
        clean_value = str(value).replace('%', '')
        return float(clean_value)
    except:
        return default

async def test_complete_report():
    """Test complete report with all publishers"""
    
    # API credentials
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    slack_webhook = "https://hooks.slack.com/services/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"
    
    # Set up 1pm-3pm EST for 09/11/2025
    start_est = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))  # 1pm EST
    end_est = datetime(2025, 9, 11, 15, 0, 0, tzinfo=timezone(timedelta(hours=-5)))    # 3pm EST
    
    start_utc = start_est.astimezone(timezone.utc)
    end_utc = end_est.astimezone(timezone.utc)
    
    print(f"🕐 Testing COMPLETE 1pm-3pm EST data for 09/11/2025")
    print(f"⏰ Start (UTC): {start_utc}")
    print(f"⏰ End (UTC): {end_utc}")
    
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
    
    print(f"🔗 Calling Ringba API with full metrics...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                print(f"📡 Response status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Ringba API success!")
                    
                    if "report" in data and "records" in data["report"]:
                        records = data["report"]["records"]
                        print(f"📊 Found {len(records)} publishers with complete data")
                        
                        if records:
                            # Process all publishers
                            publishers = []
                            for record in records:
                                # Apply the same fix as monitor.py
                                raw_publisher_name = record.get("publisherName", "")
                                print(f"DEBUG: Raw publisherName = '{raw_publisher_name}'")
                                
                                publisher_name = raw_publisher_name
                                if not publisher_name or publisher_name.strip() == "" or publisher_name.strip().lower() == "unknown":
                                    publisher_name = "Unknown Publisher"
                                    print(f"DEBUG: Converted to '{publisher_name}'")
                                else:
                                    print(f"DEBUG: Kept as '{publisher_name}'")
                                
                                # Skip "Unknown Publisher" and "MISSING" to match Ringba interface display
                                if publisher_name == "Unknown Publisher" or record.get("publisherName", "").upper() == "MISSING":
                                    print(f"DEBUG: Skipping {record.get('publisherName', '')} -> {publisher_name}")
                                    continue
                                
                                publisher = {
                                    "name": publisher_name,
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
                                # Calculate CPA (Payout / Completed Calls)
                                publisher["cpa"] = publisher["payout"] / publisher["completed"] if publisher["completed"] > 0 else 0.0
                                publishers.append(publisher)
                            
                            # Calculate totals
                            total_calls = sum(p["calls"] for p in publishers)
                            total_completed = sum(p["completed"] for p in publishers)
                            total_revenue = sum(p["revenue"] for p in publishers)
                            total_payout = sum(p["payout"] for p in publishers)
                            total_profit = sum(p["profit"] for p in publishers)
                            total_cpa = total_payout / total_completed if total_completed > 0 else 0
                            
                            print(f"\n📈 COMPLETE TOTALS:")
                            print(f"  • Total Calls: {total_calls:,}")
                            print(f"  • Completed Calls: {total_completed:,}")
                            print(f"  • Revenue: ${total_revenue:.2f}")
                            print(f"  • Payout: ${total_payout:.2f}")
                            print(f"  • Profit: ${total_profit:.2f}")
                            print(f"  • CPA: ${total_cpa:.2f}")
                            
                            # Show ALL publishers
                            print(f"\n📋 ALL PUBLISHERS ({len(publishers)} total):")
                            for i, p in enumerate(publishers, 1):
                                print(f"  {i:2d}. {p['name']:<20} | {p['completed']:3d} completed | ${p['cpa']:6.2f} CPA | ${p['revenue']:8.2f} revenue | ${p['payout']:8.2f} payout")
                            
                            # Check against Ringba expected publishers
                            ringba_publishers = ['Koji Digital', 'FITZ', 'TDES008-YT', 'TDES023-YT', 'TDES082-YT', 'TDES085-YT', 'TDES089-YT', 'TDES092-YT', 'LeadVIPs', 'TDES093-YT']
                            our_publishers = [p['name'] for p in publishers]
                            missing = [p for p in ringba_publishers if p not in our_publishers]
                            extra = [p for p in our_publishers if p not in ringba_publishers]
                            
                            print(f"\n🔍 PUBLISHER COMPARISON:")
                            print(f"  Ringba expects: {len(ringba_publishers)} publishers")
                            print(f"  We have: {len(our_publishers)} publishers")
                            if missing:
                                print(f"  Missing: {missing}")
                            if extra:
                                print(f"  Extra: {extra}")
                            
                            # Send complete Slack report
                            slack_message = {
                                "text": f"📊 COMPLETE TEST: Ringba 1pm-3pm EST Data (09/11/2025) - ALL PUBLISHERS",
                                "blocks": [
                                    {
                                        "type": "header",
                                        "text": {
                                            "type": "plain_text",
                                            "text": "📊 COMPLETE TEST: Ringba 1pm-3pm EST Data (09/11/2025) - ALL PUBLISHERS"
                                        }
                                    },
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": f"*📊 Complete Results (1pm-3pm EST on 09/11/2025):*\n• *Total Calls:* {total_calls:,}\n• *Completed Calls:* {total_completed:,}\n• *Revenue:* ${total_revenue:.2f}\n• *Payout:* ${total_payout:.2f}\n• *Profit:* ${total_profit:.2f}\n• *CPA:* ${total_cpa:.2f}"
                                        }
                                    }
                                ]
                            }
                            
                            # Add ALL publishers table
                            table_text = "*📋 ALL Publishers Performance:*\n"
                            table_text += "```\n"
                            table_text += f"{'#':<3} {'Publisher':<20} {'Completed':<10} {'CPA':<8} {'Revenue':<10} {'Payout':<10}\n"
                            table_text += "-" * 80 + "\n"
                            
                            for i, p in enumerate(publishers, 1):
                                table_text += f"{i:<3} {p['name'][:19]:<20} {p['completed']:<10} ${p['cpa']:<7.2f} ${p['revenue']:<9.2f} ${p['payout']:<9.2f}\n"
                            
                            table_text += "```"
                            
                            slack_message["blocks"].append({
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": table_text
                                }
                            })
                            
                            print(f"\n📤 Sending complete report to Slack...")
                            
                            async with session.post(slack_webhook, json=slack_message) as slack_response:
                                if slack_response.status == 200:
                                    print(f"✅ Complete Slack report sent successfully!")
                                    print(f"🎉 TEST COMPLETE: All {len(publishers)} publishers reported!")
                                else:
                                    error_text = await slack_response.text()
                                    print(f"❌ Slack error {slack_response.status}: {error_text}")
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
    asyncio.run(test_complete_report())
