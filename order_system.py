from mpi4py import MPI
import time
import random

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    # ===== MASTER (the sportsbook server) =====
    
    # Shared list that collects all completed validations
    completed_slips = []
    
    bet_slips = [
        {"id": 1001, "pick": "Lakers -5.5 vs Warriors"},
        {"id": 1002, "pick": "Celtics ML vs Heat"},
        {"id": 1003, "pick": "OVER 225.5 Nuggets/Suns"},
        {"id": 1004, "pick": "Giannis OVER 30.5 PTS"},
        {"id": 1005, "pick": "Thunder +3 vs Mavericks"},
        {"id": 1006, "pick": "Jokic triple-double YES"},
        {"id": 1007, "pick": "SGA OVER 7.5 AST"},
    ]

    print(f"[Sportsbook] Received {len(bet_slips)} bet slips.")
    print(f"[Sportsbook] Dispatching to {size - 1} validation servers...\n")

    # Send bet slips to workers (round-robin)
    for i, slip in enumerate(bet_slips):
        worker_rank = (i % (size - 1)) + 1
        comm.send(slip, dest=worker_rank)
        print(f"[Sportsbook] Sent slip #{slip['id']} to server {worker_rank}")

    # Poison pill
    for worker_rank in range(1, size):
        comm.send(None, dest=worker_rank)

    # Collect results from workers as they finish (any order)
    expected_total = len(bet_slips)
    received = 0
    while received < expected_total:
        completed_entry = comm.recv(source=MPI.ANY_SOURCE)
        completed_slips.append(completed_entry)
        received += 1

    # Print the final summary
    print("\n" + "=" * 50)
    print("[Sportsbook] ALL SLIPS VALIDATED. FINAL SUMMARY:")
    print("=" * 50)
    for entry in completed_slips:
        print(f"  Slip #{entry['id']} -> {entry['pick']} [validated by Server {entry['validated_by']}]")
    print(f"\nTotal validated: {len(completed_slips)} slips")

else:
    # ===== WORKER (a validation server) =====
    while True:
        slip = comm.recv(source=0)
        if slip is None:
            break

        # Simulate validation work
        processing_time = random.uniform(1, 3)
        print(f"[Server {rank}] Validating slip #{slip['id']}: {slip['pick']} (will take {processing_time:.2f}s)")
        time.sleep(processing_time)
        
        completed_entry = {
            "id": slip["id"],
            "pick": slip["pick"],
            "validated_by": rank
        }
        
        # Send result back to master
        comm.send(completed_entry, dest=0)
        
        print(f"[Server {rank}] Finished slip #{slip['id']}")