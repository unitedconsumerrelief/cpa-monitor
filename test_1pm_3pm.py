#!/usr/bin/env python3
"""Test script to fetch Ringba data from 1pm-3pm EST"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import logging
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PublisherMetrics:
    publisher_name: str
    incoming: int = 0
    live: int = 0
    completed: int = 0
    ended: int = 0
    connected: int = 0
    paid: int = 0
    converted: int = 0
    no_connect: int = 0
    duplicate: int = 0
    blocked: int = 0
    ivr_hangup: int = 0
    revenue: float = 0.0
    payout: float = 0.0
    profit: float = 0.0
    margin: float = 0.0
    conversion_percent: float = 0.0
    tcl_seconds: int = 0
    acl_seconds: int = 0
    total_cost: float = 0.0

    @property
    def cpa(self) -> float:
        """Calculate CPA: Revenue / Completed Calls"""
        if self.completed == 0:
            return 0.0
        return self.revenue / self.completed

async def fetch_ringba_data(start_time: datetime, end_time: datetime) -> List[PublisherMetrics]:
    """Fetch data from Ringba API"""
    ringba_api_token = os.getenv("RINGBA_API_TOKEN")
    ringba_account_id = os.getenv("RINGBA_ACCOUNT_ID", "RA092c10a91f7c461098e354a1bbeda598")
    
    url = f"https://api.ringba.com/v2/{ringba_account_id}/insights"
    
    headers = {
        "Authorization": f"Token {ringba_api_token}",
        "Content-Type": "application/json"
    }
    
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "reportStart": start_str,
        "reportEnd": end_str,
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
    
    logger.info(f"Testing API call for 1pm-3pm EST data")
    logger.info(f"Start time (UTC): {start_time}")
    logger.info(f"End time (UTC): {end_time}")
    logger.info(f"API URL: {url}")
    logger.info(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                logger.info(f"Response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    
                    # Parse the data
                    metrics_list = []
                    if "data" in data and "rows" in data["data"]:
                        for row in data["data"]["rows"]:
                            metrics = PublisherMetrics(
                                publisher_name=row.get("publisherName", ""),
                                incoming=row.get("callCount", 0),
                                live=row.get("liveCallCount", 0),
                                completed=row.get("completedCalls", 0),
                                ended=row.get("endedCalls", 0),
                                connected=row.get("connectedCallCount", 0),
                                paid=row.get("payoutCount", 0),
                                converted=row.get("convertedCalls", 0),
                                no_connect=row.get("nonConnectedCallCount", 0),
                                duplicate=row.get("duplicateCalls", 0),
                                blocked=row.get("blockedCalls", 0),
                                ivr_hangup=row.get("incompleteCalls", 0),
                                revenue=row.get("conversionAmount", 0.0),
                                payout=row.get("payoutAmount", 0.0),
                                profit=row.get("profitGross", 0.0),
                                margin=row.get("profitMarginGross", 0.0),
                                conversion_percent=row.get("convertedPercent", 0.0),
                                tcl_seconds=row.get("callLengthInSeconds", 0),
                                acl_seconds=row.get("avgHandleTime", 0),
                                total_cost=row.get("totalCost", 0.0)
                            )
                            metrics_list.append(metrics)
                        
                        logger.info(f"✅ SUCCESS: Fetched {len(metrics_list)} publishers")
                        
                        # Show top 5 publishers
                        top_publishers = sorted(metrics_list, key=lambda x: x.completed, reverse=True)[:5]
                        for i, metric in enumerate(top_publishers, 1):
                            logger.info(f"{i}. {metric.publisher_name}: {metric.completed} completed, ${metric.cpa:.2f} CPA")
                        
                        # Calculate totals
                        total_completed = sum(m.completed for m in metrics_list)
                        total_revenue = sum(m.revenue for m in metrics_list)
                        total_cpa = total_revenue / total_completed if total_completed > 0 else 0
                        
                        logger.info(f"📊 TOTALS: {total_completed} completed calls, ${total_revenue:.2f} revenue, ${total_cpa:.2f} CPA")
                        
                    else:
                        logger.warning(f"Unexpected data structure: {data}")
                        logger.info(f"Full response: {json.dumps(data, indent=2)}")
                        
                else:
                    error_text = await response.text()
                    logger.error(f"API error {response.status}: {error_text}")
                    
    except Exception as e:
        logger.error(f"Error fetching Ringba data: {e}")

async def main():
    """Test the 1pm-3pm EST data fetch"""
    # Get current EST time
    est_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    logger.info(f"Current EST time: {est_now}")
    
    # Set up 1pm-3pm EST time range
    start_time_est = est_now.replace(hour=13, minute=0, second=0, microsecond=0)  # 1pm EST
    end_time_est = est_now.replace(hour=15, minute=0, second=0, microsecond=0)    # 3pm EST
    
    # Convert to UTC for API call
    start_time = start_time_est.astimezone(timezone.utc)
    end_time = end_time_est.astimezone(timezone.utc)
    
    logger.info(f"Fetching data for 1pm-3pm EST ({start_time} to {end_time} UTC)")
    
    await fetch_ringba_data(start_time, end_time)

if __name__ == "__main__":
    asyncio.run(main())
