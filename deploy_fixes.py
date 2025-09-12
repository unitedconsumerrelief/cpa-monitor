#!/usr/bin/env python3
"""Deploy fixes for pandas and timing"""

import subprocess
import sys

def run_command(cmd):
    """Run a command and return the result"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(f"Command: {cmd}")
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print(f"Output: {result.stdout}")
        if result.stderr:
            print(f"Error: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        print(f"Exception: {e}")
        return False

def main():
    print("🚀 DEPLOYING FIXES")
    print("=" * 40)
    
    # Add files
    print("1. Adding files...")
    if not run_command("git add requirements.txt monitor.py"):
        print("❌ Failed to add files")
        return False
    
    # Commit
    print("2. Committing changes...")
    if not run_command('git commit -m "Fix pandas dependency and 5pm timing - trigger at 5:05pm EDT"'):
        print("❌ Failed to commit")
        return False
    
    # Push
    print("3. Pushing to repository...")
    if not run_command("git push cpa-monitor master"):
        print("❌ Failed to push")
        return False
    
    print("✅ All fixes deployed successfully!")
    return True

if __name__ == "__main__":
    main()
