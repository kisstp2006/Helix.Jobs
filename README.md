# Helix.Jobs

Work-stealing job system of the Helix engine.

## Design

- One worker per logical core minus one; the thread that calls `Initialize` takes part as worker 0 whenever it waits.
- Chase-Lev deque per thread; idle workers steal from the others, then from a global queue used by foreign threads.
- Fixed pools of jobs (65536) and completion counters (16384) behind lock-free index stacks with per-thread caches.
- A `JobHandle` is a counter index plus a generation; a handle is complete once the generation moved on.
- Dependencies park jobs on the counter they wait for, so waiting jobs never occupy a worker.
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
| Schedule and complete one empty job, no workers | 105 ns |
| 10000 empty jobs from one thread, 7 workers | 270 ns per job |
| `ParallelFor` over 100000 empty items, batch 64 | 2.8 ns per item |

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
