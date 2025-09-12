#!/usr/bin/env python3
"""Fixed test to fetch 1pm-3pm EST data and send to Slack"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def test_ringba_and_slack():
    """Test Ringba API and Slack webhook with correct data parsing"""
    
    # API credentials
    ringba_token = "09f0c9f035a894013c44904a9557433e3b41073cc7965927f5455f151a6eebe897ab6b0fd300aad3675e0195fe2ca22b00bf5c730a9c964bcf3ff764feaa2fbf08fd0e60b33407709f7419d0639fe8974ea8c28bcd4596af498a9ddd4cc33a37ce68cb2284c632b5559ea865f36342771de39372"
    ringba_account = "RA092c10a91f7c461098e354a1bbeda598"
    slack_webhook = "https://hooks.slack.com/services/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"
    
    # Set up 1pm-3pm EST for 09/11/2025
    start_est = datetime(2025, 9, 11, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))  # 1pm EST
    end_est = datetime(2025, 9, 11, 15, 0, 0, tzinfo=timezone(timedelta(hours=-5)))    # 3pm EST
    
    start_utc = start_est.astimezone(timezone.utc)
    end_utc = end_est.astimezone(timezone.utc)
    
    print(f"🕐 Testing 1pm-3pm EST data for 09/11/2025")
    print(f"⏰ Start (UTC): {start_utc}")
    print(f"⏰ End (UTC): {end_utc}")
    
    # Ringba API call
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
            {"column": "completedCalls", "aggregateFunction": None},
            {"column": "conversionAmount", "aggregateFunction": None},
            {"column": "payoutAmount", "aggregateFunction": None}
        ],
        "orderByColumns": [{"column": "callCount", "direction": "desc"}],
        "formatTimespans": True,
        "formatPercentages": True,
        "generateRollups": True,
        "maxResultsPerGroup": 100,
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
                    
                    # Parse the correct data structure
                    if "report" in data and "records" in data["report"]:
                        records = data["report"]["records"]
                        print(f"📊 Found {len(records)} publishers")
                        
                        if records:
                            # Calculate totals
                            total_calls = sum(float(record.get("callCount", 0)) for record in records)
                            total_completed = sum(float(record.get("completedCalls", 0)) for record in records)
                            total_revenue = sum(float(record.get("conversionAmount", 0)) for record in records)
                            total_payout = sum(float(record.get("payoutAmount", 0)) for record in records)
                            total_cpa = total_revenue / total_completed if total_completed > 0 else 0
                            
                            print(f"📈 TOTALS:")
                            print(f"  • Total Calls: {int(total_calls)}")
                            print(f"  • Completed Calls: {int(total_completed)}")
                            print(f"  • Revenue: ${total_revenue:.2f}")
                            print(f"  • Payout: ${total_payout:.2f}")
                            print(f"  • CPA: ${total_cpa:.2f}")
                            
                            # Show top publishers
                            print(f"\n🏆 Top Publishers:")
                            for i, record in enumerate(records[:5], 1):
                                publisher = record.get("publisherName", "Unknown")
                                calls = int(float(record.get("callCount", 0)))
                                completed = int(float(record.get("completedCalls", 0)))
                                revenue = float(record.get("conversionAmount", 0))
                                payout = float(record.get("payoutAmount", 0))
                                cpa = revenue / completed if completed > 0 else 0
                                print(f"  {i}. {publisher}: {completed} completed, ${cpa:.2f} CPA, ${payout:.2f} payout")
                            
                            # Send to Slack
                            slack_message = {
                                "text": f"🧪 TEST REPORT: Ringba 1pm-3pm EST Data (09/11/2025)",
                                "blocks": [
                                    {
                                        "type": "header",
                                        "text": {
                                            "type": "plain_text",
                                            "text": "🧪 TEST REPORT: Ringba 1pm-3pm EST Data (09/11/2025)"
                                        }
                                    },
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": f"*📊 Test Results (1pm-3pm EST on 09/11/2025):*\n• *Total Calls:* {int(total_calls):,}\n• *Completed Calls:* {int(total_completed):,}\n• *Revenue:* ${total_revenue:.2f}\n• *Payout:* ${total_payout:.2f}\n• *CPA:* ${total_cpa:.2f}"
                                        }
                                    }
                                ]
                            }
                            
                            # Add top 5 publishers
                            top_5 = records[:5]
                            performers_text = "*🏆 Top 5 Publishers:*\n"
                            for i, record in enumerate(top_5, 1):
                                publisher = record.get("publisherName", "Unknown")
                                completed = int(float(record.get("completedCalls", 0)))
                                revenue = float(record.get("conversionAmount", 0))
                                payout = float(record.get("payoutAmount", 0))
                                cpa = revenue / completed if completed > 0 else 0
                                performers_text += f"{i}. *{publisher}*: {completed} completed, ${cpa:.2f} CPA, ${payout:.2f} payout\n"
                            
                            slack_message["blocks"].append({
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": performers_text
                                }
                            })
                            
                            print(f"\n📤 Sending to Slack...")
                            
                            async with session.post(slack_webhook, json=slack_message) as slack_response:
                                if slack_response.status == 200:
                                    print(f"✅ Slack message sent successfully!")
                                    print(f"🎉 TEST COMPLETE: Successfully fetched 1pm-3pm data and sent Slack report!")
                                else:
                                    error_text = await slack_response.text()
                                    print(f"❌ Slack error {slack_response.status}: {error_text}")
                        else:
                            print("⚠️  No data found for 1pm-3pm EST")
                    else:
                        print(f"⚠️  Unexpected data structure: {data}")
                else:
                    error_text = await response.text()
                    print(f"❌ Ringba API error {response.status}: {error_text}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_ringba_and_slack())
