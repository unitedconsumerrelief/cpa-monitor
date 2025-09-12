#!/usr/bin/env python3
"""
Startup script for Ringba monitoring system
Loads environment variables from .env file if it exists
"""
import os
import sys
from pathlib import Path

def load_env_file():
    """Load environment variables from .env file if it exists"""
    env_file = Path(".env")
    if env_file.exists():
        print("📄 Loading environment variables from .env file...")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key] = value
        print("✅ Environment variables loaded")
    else:
        print("⚠️  No .env file found, using system environment variables")

def main():
    """Main startup function"""
    print("🚀 Starting Ringba Monitor...")
    print("=" * 40)
    
    # Load environment variables
    load_env_file()
    
    # Check required variables
    required_vars = ["RINGBA_API_TOKEN", "SLACK_WEBHOOK_URL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables or create a .env file with them.")
        print("See monitor_config.env for an example.")
        sys.exit(1)
    
    # Import and run the monitor
    try:
        from monitor import main as monitor_main
        import asyncio
        asyncio.run(monitor_main())
    except KeyboardInterrupt:
        print("\n👋 Monitor stopped by user")
    except Exception as e:
        print(f"❌ Error starting monitor: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
