#!/usr/bin/env python3
"""
Test the live webhook endpoint
"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone

# Live webhook endpoint
WEBHOOK_URL = "https://master-cpa-data.onrender.com/ringba-webhook"

# Test data matching your missing calls
TEST_WEBHOOK_DATA = {
    "call_id": "test_missing_call_001",
    "callStartUtc": "2025-09-12T10:40:58Z",
    "did": "+13808880865",
    "callerId": "+13802260897",  # This is the missing caller ID
    "durationSec": 45,
    "disposition": "completed",
    "campaignName": "SPANISH DEBT | 1.0 STANDARD",
    "campaignId": "test_campaign_123",
    "target": "LeadVIPs - Main",
    "publisherId": "test_publisher_456",
    "publisherName": "TDES089-YT",
    "payout": 25.00,
    "revenue": 50.00
}

async def test_live_webhook():
    """Test the live webhook endpoint"""
    print("🧪 Testing live webhook endpoint...")
    print(f"📡 URL: {WEBHOOK_URL}")
    print(f"📄 Data: {json.dumps(TEST_WEBHOOK_DATA, indent=2)}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(WEBHOOK_URL, json=TEST_WEBHOOK_DATA) as response:
                print(f"📡 Response Status: {response.status}")
                response_text = await response.text()
                print(f"📄 Response: {response_text}")
                
                if response.status == 200:
                    print("✅ Webhook test successful!")
                    print("🔍 Check your Google Sheets to see if the test call was added")
                else:
                    print("❌ Webhook test failed!")
                    
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_live_webhook())
