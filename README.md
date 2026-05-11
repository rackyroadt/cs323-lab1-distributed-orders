# CS323 Lab 1 — Distributed Order Processing (NBA Bet Slip Edition)

A small distributed system that simulates an online sportsbook receiving customer bet slips and distributing them to multiple validation servers for concurrent processing. Built using Python, `mpi4py` (for inter-process communication via MPI), and Python's `multiprocessing` module (for shared memory and synchronization).

---

## System Overview

The system is modeled as an NBA sportsbook:

- **Master process (rank 0)** — the sportsbook server. Generates bet slips and dispatches them to validation servers.
- **Worker processes (ranks 1, 2, 3, ...)** — validation servers. Each one receives a bet slip, simulates validation work (1–3 seconds), and sends the result back to the master.
- **Shared list** — the master aggregates all validated slips as workers finish, then prints a final summary.
- **Lock demonstration** — a separate script shows how concurrent writes to shared data can corrupt state, and how a `Lock()` fixes it.

---

## File Structure

```
cs323-lab1/
├── mpi_test.py       # Environment verification script (Phase 0)
├── order_system.py   # Main distributed system (Phases 1–3)
├── race_demo.py      # Race condition + Lock demonstration (Phase 4)
└── README.md         # This file
```

---

## Setup

### Requirements

- Windows 10/11 (tested), or Linux/macOS with equivalent MPI setup
- Python 3.11+
- Microsoft MPI (MS-MPI) v10.1.3 runtime **and** SDK
- `mpi4py` Python library

### Install

1. Download and install both installers from Microsoft MPI v10.1.3:
   - `msmpisetup.exe` (runtime)
   - `msmpisdk.msi` (SDK)
2. Install `mpi4py`:
   ```
   pip install mpi4py
   ```
3. Verify:
   ```
   mpiexec -n 4 python mpi_test.py
   ```
   Expected output — four lines like `Process 0 out of 4`, `Process 1 out of 4`, etc., in random order.

---

## Running the System

### Main distributed system (MPI)

```
mpiexec -n 4 python order_system.py
```

`-n 4` launches 4 processes total: 1 master + 3 worker validation servers. The master will dispatch 7 bet slips, workers will process them concurrently (with simulated 1–3 second delays), and the master will print a final summary of all validated slips.

**Note on `mpirun` vs `mpiexec`:** On Linux/macOS, MPI programs are typically launched with `mpirun`. On Windows with MS-MPI, the equivalent command is `mpiexec`. Both achieve the same result — spawning the specified number of MPI processes. This project uses `mpiexec` because it was developed on Windows.

### Race condition demo (multiprocessing)

```
python race_demo.py
```

This does **not** use MPI — it uses Python's `multiprocessing` module directly. Run it multiple times with the lock disabled (see Phase 4 section below) to observe inconsistencies, then enable the lock to see consistent results.

---

## Reflection Questions

### 1. How did you distribute orders among worker processes?

The master process (rank 0) uses a **round-robin distribution** strategy. Each bet slip is assigned to a worker rank using the formula:

```python
worker_rank = (i % (size - 1)) + 1
```

Where `i` is the slip index and `size` is the total number of MPI processes. The `+ 1` skips rank 0 (the master), so slips cycle through workers 1, 2, 3, 1, 2, 3, and so on. With 7 slips and 3 workers, worker 1 receives 3 slips and workers 2 and 3 each receive 2. The master uses `comm.send(slip, dest=worker_rank)` to send each slip; workers sit in a `while True` loop calling `comm.recv(source=0)` until they receive a `None` value (the poison pill) that tells them to terminate.

### 2. What happens if there are more orders than workers?

Because of the round-robin formula, workers simply handle more than one slip each — nothing is dropped or queued externally. In our test run with 7 slips and 3 workers, the distribution was:

- Worker 1: slips #1001, #1004, #1007
- Worker 2: slips #1002, #1005
- Worker 3: slips #1003, #1006

Each worker's `while True` receive loop keeps running until the poison pill arrives, so a worker handles slips sequentially as the master sends them. The system scales naturally — the same code works whether there are fewer or more slips than workers.

### 3. How did processing delays affect the order completion?

We introduced random delays between 1–3 seconds per slip using `time.sleep(random.uniform(1, 3))`. This made concurrency visible: workers started nearly simultaneously but finished in a different order than the slips were dispatched. For example, in one run:

- Slip #1001 was sent to Worker 1 first, but took 2.23s
- Slip #1003 was sent to Worker 3 third, but took only 1.69s → finished first

The final summary list reflects **completion order, not dispatch order**. Total wall-clock time is bounded by the *slowest worker's total workload*, not the sum of all delays. With 7 slips averaging ~2s each:

- Sequential processing would take ~14s
- Our 3-worker concurrent system finished in ~5–6s

This is the core benefit of parallelism.

### 4. How did you implement shared memory, and where was it initialized?

In `order_system.py`, the shared data structure `completed_slips` is a Python list, initialized inside the master's `if rank == 0:` block. Workers do **not** directly write to this list (MPI processes are separate programs and do not share memory by default). Instead:

1. Each worker processes its slip and sends the result back to the master via `comm.send(completed_entry, dest=0)`.
2. The master uses `comm.recv(source=MPI.ANY_SOURCE)` in a loop to accept results from whichever worker finishes first.
3. The master appends each result to `completed_slips`.

**Implementation note:** Our initial attempt used `multiprocessing.Manager().list()` passed through MPI, as suggested in the lab handout. On Windows, this caused the program to deadlock because `Manager()` internally spawns a helper subprocess that re-imports the script and re-initializes MPI, resulting in a hang. We resolved this by switching to an MPI-native pattern: results flow through MPI messages, and only the master writes to the shared list. This is a cleaner design anyway — it avoids concurrent writes entirely in the main system, which is the correct architectural choice for a master-worker collector.

The `Manager().list()` + `Lock()` concepts are still demonstrated in `race_demo.py`, where they apply naturally to `multiprocessing.Process` workers that genuinely contend for shared memory.

### 5. What issues occurred when multiple workers wrote to shared memory simultaneously?

We built a dedicated demonstration in `race_demo.py` to show this clearly. Five processes each perform 100 "bet payouts," deducting $10 per payout from a shared balance of $10,000. Expected final balance: **$5,000**. Without synchronization, each deduction is actually three operations:

1. Read the current balance
2. Subtract $10
3. Write the new balance back

When two processes interleave between steps 1 and 3, one update **overwrites** the other and a deduction is lost. Observed results without a lock:

```
Run 1: Actual $7,010 — off by $2,010 (201 payouts lost)
Run 2: Actual $6,320 — off by $1,320 (132 payouts lost)
```

Results were different every run — classic non-deterministic race condition behavior. Known as the **lost update problem**, this is the same bug pattern that causes real-world issues in banking, inventory, and ticketing systems.

### 6. How did you ensure consistent results when using multiple processes?

In `race_demo.py`, we wrapped the read-modify-write critical section in a `multiprocessing.Lock()`:

```python
with lock:
    current = shared_state["balance"]
    time.sleep(0.0001)
    shared_state["balance"] = current - PAYOUT_AMOUNT
```

The lock acts as a mutual exclusion primitive — only one process can be inside the `with lock:` block at any given moment. Other processes attempting to acquire the lock are blocked until the current holder releases it. After adding the lock, every single run produces exactly the expected **$5,000** balance. Runs are now deterministic and correct.

There is a tradeoff: locking serializes the critical section, so workers cannot execute that portion truly in parallel anymore. The program is slightly slower with the lock, but correct. This is a fundamental principle in concurrent programming: **correctness over raw speed** when the two are in tension.

For the main `order_system.py`, we avoided needing a lock entirely by designing the system so only the master writes to the shared list. Workers send their results through MPI messages, which are inherently serialized. This is a common and desirable pattern for master-worker systems.

---

## Team Contributors

| Name | GitHub | Main Contributions |
|---|---|---|
| Jiane Rackyle Sarting | [@rackyroadt](https://github.com/rackyroadt) | Phase 1 setup, Phase 3 shared collection, Phase 4 race/lock demo, README |
| John Lloyd Arvin | [@johnskie149](https://github.com/johnskie149) | Phase 2 processing delays and concurrency simulation |

*(Add additional groupmates here as they contribute.)*

---

## What We Learned

- **MPI's master-worker pattern** — a small amount of boilerplate (`if rank == 0 / else`) opens up massively parallel computation.
- **Concurrency is not parallelism** — workers ran concurrently but the program was still correct because messages are ordered.
- **Race conditions are real and reproducible** — seeing $2,000 of "lost money" in 5 seconds of runtime made the abstract concept concrete.
- **Synchronization has a cost** — locks slow things down, but correctness wins every time in production systems.
- **Windows + MPI + multiprocessing has sharp edges** — we hit a real-world deadlock with `Manager()` across MPI ranks and learned to redesign the data flow to avoid the problem.
