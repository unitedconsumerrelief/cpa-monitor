#!/usr/bin/env python3
"""
Deployment script for Ringba monitoring system
"""
import os
import sys
import subprocess
import asyncio
from monitor import RingbaMonitor

def check_environment():
    """Check if required environment variables are set"""
    required_vars = ["RINGBA_API_TOKEN", "SLACK_WEBHOOK_URL"]
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables before running the monitor.")
        return False
    
    print("✅ All required environment variables are set")
    return True

def install_dependencies():
    """Install required dependencies"""
    print("📦 Installing dependencies...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True, text=True)
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

async def test_monitoring():
    """Test the monitoring system"""
    print("🧪 Testing monitoring system...")
    try:
        monitor = RingbaMonitor()
        
        # Test with a small time window
        from datetime import datetime, timezone, timedelta
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=30)  # Test with 30 minutes
        
        print(f"Testing with time range: {start_time} to {end_time}")
        
        # This will make a real API call - it might fail if token is invalid
        metrics = await monitor.fetch_ringba_data(start_time, end_time)
        
        if metrics:
            print(f"✅ Successfully fetched {len(metrics)} publisher metrics")
            print("Sample metrics:")
            for metric in metrics[:3]:  # Show first 3
                print(f"  - {metric.publisher_name}: {metric.completed} completed, ${metric.cpa:.2f} CPA")
        else:
            print("⚠️  No metrics fetched (this might be normal if no data in time range)")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def main():
    """Main deployment function"""
    print("🚀 Ringba Monitor Deployment")
    print("=" * 40)
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        sys.exit(1)
    
    # Test monitoring
    print("\nTesting monitoring system...")
    test_result = asyncio.run(test_monitoring())
    
    if test_result:
        print("\n✅ Deployment successful!")
        print("\nTo start monitoring, run:")
        print("   python monitor.py")
        print("\nTo run in background:")
        print("   nohup python monitor.py > monitor.log 2>&1 &")
    else:
        print("\n⚠️  Deployment completed with warnings")
        print("Monitor may still work - check your API token and try running:")
        print("   python monitor.py")

if __name__ == "__main__":
    main()
