#!/usr/bin/env python3
"""Test if monitor.py filtering actually works"""

from monitor import RingbaMonitor

def test_filtering():
    monitor = RingbaMonitor()
    
    # Test data with MISSING publisher
    test_data = {
        'report': {
            'records': [
                {'publisherName': 'Koji Digital', 'callCount': 29, 'completedCalls': 21, 'payoutAmount': 495},
                {'publisherName': 'FITZ', 'callCount': 25, 'completedCalls': 25, 'payoutAmount': 185},
                {'publisherName': 'MISSING', 'callCount': 88, 'completedCalls': 76, 'payoutAmount': 680}
            ]
        }
    }
    
    print("TESTING MONITOR.PY FILTERING")
    print("Input: 3 records (including MISSING)")
    
    publishers = monitor._parse_ringba_data(test_data)
    
    print(f"\nOutput: {len(publishers)} publishers")
    for p in publishers:
        print(f"  {p.publisher_name}: {p.completed} completed, ${p.payout} payout")
    
    total_calls = sum(p.incoming for p in publishers)
    total_completed = sum(p.completed for p in publishers)
    total_payout = sum(p.payout for p in publishers)
    
    print(f"\nTOTALS:")
    print(f"  Publishers: {len(publishers)} (Expected: 2)")
    print(f"  Calls: {total_calls} (Expected: 54)")
    print(f"  Completed: {total_completed} (Expected: 46)")
    print(f"  Payout: ${total_payout} (Expected: $680)")
    
    if len(publishers) == 2 and total_calls == 54 and total_completed == 46:
        print("\n✅ FILTERING WORKS! MISSING publisher was filtered out!")
    else:
        print("\n❌ FILTERING FAILED! MISSING publisher was NOT filtered out!")

if __name__ == "__main__":
    test_filtering()


