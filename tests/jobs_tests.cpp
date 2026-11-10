#include <helix/jobs/jobs.h>
#include <helix/core/containers/array.h>
#include <helix/testing/test.h>

#include <atomic>

using namespace helix;
using namespace helix::jobs;

namespace
{
    struct Session
    {
        explicit Session(i32 workers = -1) { Initialize({.workerCount = workers}); }
        ~Session() { Shutdown(); }
    };
}

HX_TEST(Jobs, ScheduleAndComplete)
{
    Session s;
    std::atomic<int> value{0};
    std::atomic<int>* p = &value;
    JobHandle h = Schedule([p] { p->fetch_add(1); });
    HX_CHECK(h.IsValid());
    Complete(h);
    HX_CHECK(IsCompleted(h));
    HX_CHECK_EQ(value.load(), 1);
    Complete(h);
    HX_CHECK(IsCompleted(JobHandle{}));
}

HX_TEST(Jobs, DependencyChainRunsInOrder)
{
    Session s;
    struct State
    {
        std::atomic<int> step{0};
        int order[3] = {};
    } state;
    State* p = &state;
    JobHandle a = Schedule([p] { p->order[0] = p->step.fetch_add(1); });
    JobHandle b = Schedule([p] { p->order[1] = p->step.fetch_add(1); }, a);
    JobHandle c = Schedule([p] { p->order[2] = p->step.fetch_add(1); }, b);
    Complete(c);
    HX_CHECK(IsCompleted(a) && IsCompleted(b));
    HX_CHECK_EQ(state.order[0], 0);
    HX_CHECK_EQ(state.order[1], 1);
    HX_CHECK_EQ(state.order[2], 2);
}

HX_TEST(Jobs, ParallelForVisitsEveryIndexOnce)
{
    Session s;
    constexpr u32 n = 100000;
    static std::atomic<u8> hits[n];
    for (auto& h : hits)
        h.store(0);
    std::atomic<u8>* data = hits;
    JobHandle h = ParallelFor(n, 64, [data](u32 i) { data[i].fetch_add(1, std::memory_order_relaxed); });
    Complete(h);
    u32 bad = 0;
    for (auto& v : hits)
        bad += v.load() == 1 ? 0 : 1;
    HX_CHECK_EQ(bad, 0u);
    HX_CHECK(!ParallelFor(0, 8, [](u32) {}).IsValid());
}

HX_TEST(Jobs, CombineDependencies)
{
    Session s;
    std::atomic<int> count{0};
    std::atomic<int>* p = &count;
    JobHandle handles[8];
    for (JobHandle& h : handles)
        h = Schedule([p] { p->fetch_add(1); });
    JobHandle all = CombineDependencies(Span<const JobHandle>(handles, 8));
    JobHandle after = Schedule([p] { p->store(p->load() * 10); }, all);
    Complete(after);
    HX_CHECK_EQ(count.load(), 80);
    JobHandle none = CombineDependencies(Span<const JobHandle>());
    Complete(none);
    HX_CHECK(IsCompleted(none));
}

HX_TEST(Jobs, NestedSchedulingFromJobs)
{
    Session s;
    struct State
    {
        std::atomic<int> leaves{0};
    } state;
    State* p = &state;
    JobHandle outer = ParallelFor(64, 1, [p](u32) {
        JobHandle inner = ParallelFor(16, 4, [p](u32) { p->leaves.fetch_add(1, std::memory_order_relaxed); });
        Complete(inner);
    });
    Complete(outer);
    HX_CHECK_EQ(state.leaves.load(), 64 * 16);
}

HX_TEST(Jobs, ManySmallJobsReusePools)
{
    Session s;
    std::atomic<int> count{0};
    std::atomic<int>* p = &count;
    for (int round = 0; round < 20; round++)
    {
        Array<JobHandle> handles;
        handles.Reserve(10000);
        for (int i = 0; i < 10000; i++)
            handles.Push(Schedule([p] { p->fetch_add(1, std::memory_order_relaxed); }));
        Complete(CombineDependencies(Span<const JobHandle>(handles.Data(), handles.Size())));
    }
    HX_CHECK_EQ(count.load(), 200000);
}

HX_TEST(Jobs, ZeroWorkersRunOnCompletingThread)
{
    Session s(0);
    HX_CHECK_EQ(WorkerCount(), 0u);
    int value = 0;
    int* p = &value;
    JobHandle a = Schedule([p] { *p += 1; });
    JobHandle b = ParallelFor(10, 3, [p](u32 i) { *p += int(i); }, a);
    HX_CHECK(!IsCompleted(b));
    Complete(b);
    HX_CHECK_EQ(value, 46);
}

HX_TEST(Jobs, RawFunctionApi)
{
    Session s;
    struct Data
    {
        std::atomic<u64> sum{0};
    } data;
    JobHandle h = ScheduleParallelFor([](void* d, u32 begin, u32 end) {
        u64 local = 0;
        for (u32 i = begin; i < end; i++)
            local += i;
        static_cast<Data*>(d)->sum.fetch_add(local);
    }, &data, 1000, 100);
    Complete(h);
    HX_CHECK_EQ(data.sum.load(), 999u * 1000u / 2u);
}
