from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    # ===== MASTER (the sportsbook server) =====
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

    for i, slip in enumerate(bet_slips):
        worker_rank = (i % (size - 1)) + 1
        comm.send(slip, dest=worker_rank)
        print(f"[Sportsbook] Sent slip #{slip['id']} to server {worker_rank}")

    # Poison pill: tell each worker to shut down
    for worker_rank in range(1, size):
        comm.send(None, dest=worker_rank)

else:
    # ===== WORKER (a validation server) =====
    while True:
        slip = comm.recv(source=0)
        if slip is None:
            break
        print(f"[Server {rank}] Validating slip #{slip['id']}: {slip['pick']}")