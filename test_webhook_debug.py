#!/usr/bin/env python3
"""
Test script to debug webhook data collection issues
"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone

# Test webhook endpoint
WEBHOOK_URL = "http://localhost:8000/ringba-webhook"
DEBUG_URL = "http://localhost:8000/debug/stats"

# Sample webhook data that should be processed
SAMPLE_WEBHOOK_DATA = {
    "call_id": "test_call_12345",
    "callStartUtc": "2024-01-15T10:30:00Z",
    "did": "+18337271754",
    "callerId": "+19564541202",  # This is the missing caller ID from your example
    "durationSec": 45,
    "disposition": "completed",
    "campaignName": "Test Campaign",
    "campaignId": "test_campaign_123",
    "target": "Test Target",
    "publisherId": "test_publisher_456",
    "publisherName": "Test Publisher",
    "payout": 25.00,
    "revenue": 50.00
}

async def test_webhook():
    """Test the webhook endpoint with sample data"""
    print("🧪 Testing webhook endpoint...")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Test webhook endpoint
            async with session.post(WEBHOOK_URL, json=SAMPLE_WEBHOOK_DATA) as response:
                print(f"📡 Webhook Response Status: {response.status}")
                response_text = await response.text()
                print(f"📄 Webhook Response: {response_text}")
                
                if response.status == 200:
                    print("✅ Webhook test successful!")
                else:
                    print("❌ Webhook test failed!")
            
            # Check debug stats
            print("\n📊 Checking debug stats...")
            async with session.get(DEBUG_URL) as response:
                if response.status == 200:
                    stats = await response.json()
                    print(f"📈 Current Stats: {json.dumps(stats, indent=2)}")
                else:
                    print(f"❌ Failed to get debug stats: {response.status}")
                    
    except Exception as e:
        print(f"❌ Test failed: {e}")

async def test_multiple_calls():
    """Test with multiple different call IDs to check duplicate filtering"""
    print("\n🔄 Testing multiple calls...")
    
    test_calls = [
        {**SAMPLE_WEBHOOK_DATA, "call_id": "test_call_001", "callerId": "+19564541202"},
        {**SAMPLE_WEBHOOK_DATA, "call_id": "test_call_002", "callerId": "+19564541203"},
        {**SAMPLE_WEBHOOK_DATA, "call_id": "test_call_003", "callerId": "+19564541204"},
        {**SAMPLE_WEBHOOK_DATA, "call_id": "test_call_001", "callerId": "+19564541202"},  # Duplicate
    ]
    
    try:
        async with aiohttp.ClientSession() as session:
            for i, call_data in enumerate(test_calls, 1):
                print(f"\n📞 Test Call {i}: {call_data['call_id']} - {call_data['callerId']}")
                
                async with session.post(WEBHOOK_URL, json=call_data) as response:
                    response_text = await response.text()
                    print(f"   Response: {response_text}")
                
                # Small delay between calls
                await asyncio.sleep(0.5)
            
            # Check final stats
            print("\n📊 Final Stats:")
            async with session.get(DEBUG_URL) as response:
                if response.status == 200:
                    stats = await response.json()
                    print(f"📈 Final Stats: {json.dumps(stats, indent=2)}")
                    
    except Exception as e:
        print(f"❌ Multiple calls test failed: {e}")

async def main():
    """Main test function"""
    print("🚀 Starting Webhook Debug Tests")
    print("=" * 50)
    
    # Test single webhook
    await test_webhook()
    
    # Test multiple calls
    await test_multiple_calls()
    
    print("\n✅ Tests completed!")
    print("\nTo check logs, look for the emoji-prefixed log messages:")
    print("📥 WEBHOOK RECEIVED")
    print("📞 PROCESSING CALL")
    print("📝 Added to queue")
    print("📊 WRITING TO SHEETS")

if __name__ == "__main__":
    asyncio.run(main())
