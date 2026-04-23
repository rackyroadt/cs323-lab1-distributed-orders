"""
Phase 4 — Part 2: Fixing the Race Condition WITH a Lock

Same setup as before, but now we wrap the critical section
(read-modify-write) in a Lock. Only one process can be in
the critical section at a time, so updates never get lost.
"""

from multiprocessing import Process, Manager, Lock   # ← CHANGE 1: import Lock
import time

NUM_WORKERS = 5
PAYOUTS_PER_WORKER = 100
PAYOUT_AMOUNT = 10

def payout_worker(worker_id, shared_state, lock):     # ← CHANGE 2: receive lock
    """Each worker deducts PAYOUT_AMOUNT from the balance safely using a lock."""
    for i in range(PAYOUTS_PER_WORKER):
        with lock:                                    # ← CHANGE 3: critical section
            # Only ONE process can be inside this block at a time
            current = shared_state["balance"]
            time.sleep(0.0001)
            shared_state["balance"] = current - PAYOUT_AMOUNT

if __name__ == "__main__":
    manager = Manager()
    shared_state = manager.dict()
    shared_state["balance"] = 10000
    lock = Lock()                                      # ← CHANGE 4: create the lock

    expected_final = 10000 - (NUM_WORKERS * PAYOUTS_PER_WORKER * PAYOUT_AMOUNT)

    print(f"Starting {NUM_WORKERS} payout workers, each processing {PAYOUTS_PER_WORKER} payouts of ${PAYOUT_AMOUNT}...")
    print(f"Starting balance: ${shared_state['balance']}")
    print(f"Expected final balance: ${expected_final}")
    print("Using Lock() to synchronize access.\n")

    processes = []
    for worker_id in range(NUM_WORKERS):
        p = Process(target=payout_worker, args=(worker_id, shared_state, lock))  # ← pass lock
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    actual_final = shared_state["balance"]
    print(f"Actual final balance: ${actual_final}")

    if actual_final == expected_final:
        print("[OK] Balance is correct! Lock prevented the race condition.")
    else:
        overcounted = actual_final - expected_final
        print(f"[ERROR] Balance is STILL wrong by ${overcounted}.")
        
        