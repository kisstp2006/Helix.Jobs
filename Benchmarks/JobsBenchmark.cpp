#include <Helix/Jobs/Jobs.h>
#include <Helix/Core/Containers/Array.h>
#include <Helix/Testing/Bench.h>

#include <atomic>
#include <cmath>

using namespace helix;
using namespace helix::jobs;
using helix::testing::DoNotOptimize;

namespace
{
    struct Init
    {
        Init() { Initialize(); }
        ~Init() { Shutdown(); }
    } init;

    inline f32 mathSqrtApprox(f32 x) { return std::sqrt(x) * 0.5f + 1.0f; }

    constexpr u32 kItems = 1u << 22;
    f32 values[kItems];
}

HX_BENCH(EmptyJobScheduleComplete)
{
    while (state.KeepRunning())
        Complete(Schedule([] {}));
}

HX_BENCH(EmptyJobs10kThroughput)
{
    state.SetItemsPerIteration(10000);
    Array<JobHandle> handles;
    handles.Reserve(10000);
    while (state.KeepRunning())
    {
        handles.Clear();
        for (int i = 0; i < 10000; i++)
            handles.Push(Schedule([] {}));
        Complete(CombineDependencies(Span<const JobHandle>(handles.Data(), handles.Size())));
    }
}

HX_BENCH(ParallelForEmpty100kBatch64)
{
    state.SetItemsPerIteration(100000);
    while (state.KeepRunning())
        Complete(ParallelFor(100000, 64, [](u32) {}));
}

HX_BENCH(ParallelSqrt4M)
{
    f32* data = values;
    state.SetItemsPerIteration(kItems);
    while (state.KeepRunning())
    {
        Complete(ParallelFor(kItems, 16384, [data](u32 i) { data[i] = mathSqrtApprox(f32(i)); }));
        DoNotOptimize(data[kItems / 3]);
    }
}

HX_BENCH(SerialSqrt4M)
{
    state.SetItemsPerIteration(kItems);
    while (state.KeepRunning())
    {
        for (u32 i = 0; i < kItems; i++)
            values[i] = mathSqrtApprox(f32(i));
        DoNotOptimize(values[kItems / 3]);
    }
}
