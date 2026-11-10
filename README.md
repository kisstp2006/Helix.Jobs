# Helix.Jobs

Work-stealing job system of the Helix engine.

## Design

- One worker per logical core minus one; the thread that calls `Initialize` takes part as worker 0 whenever it waits.
- Jobs scheduled from inside a job go to that worker's Chase-Lev deque; jobs from the main thread or other threads are dealt round-robin into per-worker bounded MPMC inboxes, so workers rarely compete for the same queue.
- Idle workers look at their own deque and inbox, then steal from random victims, then from a global overflow queue.
- Fixed pools of cache-line sized jobs (65536) and completion counters (16384); free indices move between threads in chains of 64 with one CAS per chain, fresh indices come from a bump counter.
- A `JobHandle` is a counter index plus a generation; a handle is complete once the generation moved on.
- Dependencies park jobs on a lock-free list in the counter they wait for (the list head carries the generation), so waiting jobs never occupy a worker.
- Sleeping workers wait on a semaphore built on atomic wait and notify.
- Every executed job is a `Job` scope in Helix.Profiler.
- Without thread support (web builds without pthreads) or with `workerCount = 0`, jobs run inside `Complete`.

## Usage

```cpp
using namespace helix::jobs;
Initialize();

JobHandle a = Schedule([] { LoadSomething(); });
JobHandle b = ParallelFor(count, 256, [data](helix::u32 i) { data[i] *= 2.0f; }, a);
JobHandle c = CombineDependencies(a, b);
Complete(c);

Shutdown();
```

Closures must be trivially copyable and at most 48 bytes; larger state is passed by pointer.
The raw API `Schedule(function, data, dependency)` and `ScheduleParallelFor` take plain function pointers.

## Performance (Ryzen 7 3750H, 7 workers, release)

| Benchmark | Result |
|---|---|
| 10000 empty jobs from one thread, 7 workers | 170 ns per job |
| `ParallelFor` over 100000 empty items, batch 64 | 1.3 ns per item |
| One empty job scheduled and completed in a loop (handoff to a worker and back) | 450 ns |

## Examples

`examples/frame_demo.cpp` opens a window, simulates 262144 particles with two dependent
`ParallelFor` jobs per frame, logs input and writes a profiler capture after 120 frames.

## Build

```
git clone --recursive https://github.com/kisstp2006/Helix.Jobs
cmake --preset windows-release
cmake --build --preset windows-release
ctest --preset windows-release
```

## License

GPL-3.0, see `LICENSE`.
