#!/usr/bin/env python3
"""Simple test to fetch 1pm-3pm EST data and send to Slack"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def test_ringba_and_slack():
    """Test Ringba API and Slack webhook"""
    
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
                    
                    if "data" in data and "rows" in data["data"]:
                        rows = data["data"]["rows"]
                        print(f"📊 Found {len(rows)} publishers")
                        
                        if rows:
                            # Calculate totals
                            total_calls = sum(row.get("callCount", 0) for row in rows)
                            total_completed = sum(row.get("completedCalls", 0) for row in rows)
                            total_revenue = sum(row.get("conversionAmount", 0.0) for row in rows)
                            total_cpa = total_revenue / total_completed if total_completed > 0 else 0
                            
                            print(f"📈 TOTALS: {total_completed} completed, ${total_revenue:.2f} revenue, ${total_cpa:.2f} CPA")
                            
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
                                            "text": f"*Test Results:*\n• *Completed Calls:* {total_completed:,}\n• *Revenue:* ${total_revenue:,.2f}\n• *CPA:* ${total_cpa:.2f}\n• *Total Calls:* {total_calls:,}"
                                        }
                                    }
                                ]
                            }
                            
                            # Add top 5 publishers
                            top_5 = sorted(rows, key=lambda x: x.get("completedCalls", 0), reverse=True)[:5]
                            performers_text = "*🏆 Top 5 Publishers:*\n"
                            for i, row in enumerate(top_5, 1):
                                publisher = row.get("publisherName", "Unknown")
                                completed = row.get("completedCalls", 0)
                                revenue = row.get("conversionAmount", 0.0)
                                cpa = revenue / completed if completed > 0 else 0
                                performers_text += f"{i}. *{publisher}*: {completed} completed, ${cpa:.2f} CPA\n"
                            
                            slack_message["blocks"].append({
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": performers_text
                                }
                            })
                            
                            print(f"📤 Sending to Slack...")
                            
                            async with session.post(slack_webhook, json=slack_message) as slack_response:
                                if slack_response.status == 200:
                                    print(f"✅ Slack message sent successfully!")
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
