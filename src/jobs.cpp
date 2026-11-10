#include <helix/jobs/jobs.h>
#include <helix/core/base/assert.h>
#include <helix/core/memory/allocator.h>
#include <helix/core/strings/format.h>
#include <helix/platform/thread.h>
#include <helix/profiler/profiler.h>

#include <atomic>

namespace helix::jobs
{
    namespace
    {
        constexpr u32 kMaxJobs = 1u << 16;
        constexpr u32 kMaxCounters = 1u << 14;
        constexpr u32 kMaxThreads = 64;
        constexpr u32 kDequeCapacity = 4096;
        constexpr u32 kGlobalCapacity = kMaxJobs;

        struct Job
        {
            JobFunction function;
            void* data;
            u32 begin;
            u32 end;
            u32 counter;
            u32 nextWaiting;
        };

        struct alignas(64) Counter
        {
            std::atomic<i32> pending{0};
            std::atomic<u32> generation{1};
            platform::SpinLock lock;
            bool done = true;
            u32 waitingHead = 0;
            alignas(16) u8 closure[detail::kClosureSize];
        };

        // Lock-free stack of indices; the upper 32 bits of the head are a tag against ABA.
        class IndexStack
        {
        public:
            void Init(std::atomic<u32>* next, u32 count)
            {
                next_ = next;
                head_.store(0, std::memory_order_relaxed);
                for (u32 i = count; i-- > 1;)
                    Push(i);
            }

            void Push(u32 index)
            {
                u64 old = head_.load(std::memory_order_relaxed);
                u64 desired;
                do
                {
                    next_[index].store(u32(old), std::memory_order_relaxed);
                    desired = (((old >> 32) + 1) << 32) | index;
                } while (!head_.compare_exchange_weak(old, desired, std::memory_order_release, std::memory_order_relaxed));
            }

            u32 Pop()
            {
                u64 old = head_.load(std::memory_order_acquire);
                for (;;)
                {
                    u32 index = u32(old);
                    if (!index)
                        return 0;
                    u32 next = next_[index].load(std::memory_order_relaxed);
                    u64 desired = (((old >> 32) + 1) << 32) | next;
                    if (head_.compare_exchange_weak(old, desired, std::memory_order_acquire, std::memory_order_acquire))
                        return index;
                }
            }

        private:
            std::atomic<u64> head_{0};
            std::atomic<u32>* next_ = nullptr;
        };

        // Chase-Lev work stealing deque: the owner pushes and pops at the bottom, thieves steal at the top.
        struct alignas(64) Deque
        {
            std::atomic<i64> top{0};
            alignas(64) std::atomic<i64> bottom{0};
            std::atomic<u32> items[kDequeCapacity];

            bool Push(u32 job)
            {
                i64 b = bottom.load(std::memory_order_relaxed);
                i64 t = top.load(std::memory_order_acquire);
                if (b - t >= i64(kDequeCapacity))
                    return false;
                items[b & (kDequeCapacity - 1)].store(job, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_release);
                bottom.store(b + 1, std::memory_order_relaxed);
                return true;
            }

            u32 Pop()
            {
                i64 b = bottom.load(std::memory_order_relaxed) - 1;
                bottom.store(b, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_seq_cst);
                i64 t = top.load(std::memory_order_relaxed);
                if (t > b)
                {
                    bottom.store(b + 1, std::memory_order_relaxed);
                    return 0;
                }
                u32 job = items[b & (kDequeCapacity - 1)].load(std::memory_order_relaxed);
                if (t == b)
                {
                    if (!top.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed))
                        job = 0;
                    bottom.store(b + 1, std::memory_order_relaxed);
                }
                return job;
            }

            u32 Steal()
            {
                i64 t = top.load(std::memory_order_acquire);
                std::atomic_thread_fence(std::memory_order_seq_cst);
                i64 b = bottom.load(std::memory_order_acquire);
                if (t >= b)
                    return 0;
                u32 job = items[t & (kDequeCapacity - 1)].load(std::memory_order_relaxed);
                if (!top.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed))
                    return 0;
                return job;
            }
        };

        // Per-thread cache in front of a shared index stack, refilled and drained in batches.
        struct IndexCache
        {
            static constexpr u32 kCapacity = 256;
            static constexpr u32 kBatch = 64;
            u32 items[kCapacity];
            u32 count = 0;

            u32 Pop(IndexStack& shared)
            {
                if (!count)
                {
                    for (u32 i = 0; i < kBatch; i++)
                    {
                        u32 v = shared.Pop();
                        if (!v)
                            break;
                        items[count++] = v;
                    }
                    if (!count)
                        return 0;
                }
                return items[--count];
            }

            void Push(IndexStack& shared, u32 index)
            {
                if (count == kCapacity)
                {
                    for (u32 i = 0; i < kBatch; i++)
                        shared.Push(items[--count]);
                }
                items[count++] = index;
            }

            void Flush(IndexStack& shared)
            {
                while (count)
                    shared.Push(items[--count]);
            }
        };

        struct System
        {
            Job jobs[kMaxJobs];
            std::atomic<u32> jobNext[kMaxJobs];
            Counter counters[kMaxCounters];
            std::atomic<u32> counterNext[kMaxCounters];
            IndexStack freeJobs;
            IndexStack freeCounters;
            Deque deques[kMaxThreads];
            platform::Thread threads[kMaxThreads];
            u32 workerCount = 0;
            std::atomic<bool> running{false};
            std::atomic<u32> sleeping{0};
            std::atomic<u32> wakesInFlight{0};
            platform::Semaphore wake{0};
            platform::Mutex globalMutex;
            u32 global[kGlobalCapacity];
            u32 globalHead = 0;
            u32 globalCount = 0;
            std::atomic<u32> globalSize{0};
        };

        System* sys = nullptr;
        thread_local i32 threadIndex = -1;
        thread_local IndexCache jobCache;
        thread_local IndexCache counterCache;

        void FreeJob(u32 job) { jobCache.Push(sys->freeJobs, job); }
        void FreeCounter(u32 counter) { counterCache.Push(sys->freeCounters, counter); }
        profiler::Marker jobMarker{"Job"};

        void PushGlobal(u32 job)
        {
            platform::ScopedLock lock(sys->globalMutex);
            HX_VERIFY_MSG(sys->globalCount < kGlobalCapacity, "global job queue overflow");
            sys->global[(sys->globalHead + sys->globalCount) % kGlobalCapacity] = job;
            sys->globalCount++;
            sys->globalSize.store(sys->globalCount, std::memory_order_relaxed);
        }

        u32 PopGlobal()
        {
            if (sys->globalSize.load(std::memory_order_relaxed) == 0)
                return 0;
            platform::ScopedLock lock(sys->globalMutex);
            if (!sys->globalCount)
                return 0;
            u32 job = sys->global[sys->globalHead];
            sys->globalHead = (sys->globalHead + 1) % kGlobalCapacity;
            sys->globalCount--;
            sys->globalSize.store(sys->globalCount, std::memory_order_relaxed);
            return job;
        }

        void Push(u32 job)
        {
            if (threadIndex < 0 || !sys->deques[threadIndex].Push(job))
                PushGlobal(job);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            if (sys->sleeping.load(std::memory_order_relaxed) > sys->wakesInFlight.load(std::memory_order_relaxed))
            {
                sys->wakesInFlight.fetch_add(1, std::memory_order_relaxed);
                sys->wake.Release();
            }
        }

        u32 FindWork()
        {
            u32 self = threadIndex >= 0 ? u32(threadIndex) : 0;
            if (threadIndex >= 0)
                if (u32 job = sys->deques[self].Pop())
                    return job;
            u32 threads = sys->workerCount + 1;
            for (u32 k = 1; k < threads; k++)
                if (u32 job = sys->deques[(self + k) % threads].Steal())
                    return job;
            if (threadIndex < 0)
                if (u32 job = sys->deques[0].Steal())
                    return job;
            return PopGlobal();
        }

        void Execute(u32 job);

        u32 AllocJob()
        {
            for (;;)
            {
                if (u32 j = jobCache.Pop(sys->freeJobs))
                    return j;
                if (u32 work = FindWork())
                    Execute(work);
                else
                    platform::YieldThread();
            }
        }

        u32 AllocCounter(i32 pending)
        {
            u32 c;
            for (;;)
            {
                c = counterCache.Pop(sys->freeCounters);
                if (c)
                    break;
                if (u32 work = FindWork())
                    Execute(work);
                else
                    platform::YieldThread();
            }
            Counter& k = sys->counters[c];
            k.lock.Lock();
            k.done = false;
            k.waitingHead = 0;
            k.pending.store(pending, std::memory_order_relaxed);
            k.lock.Unlock();
            return c;
        }

        void Finish(u32 counter);

        void Release(u32 job)
        {
            Job& j = sys->jobs[job];
            if (j.function)
            {
                Push(job);
                return;
            }
            u32 counter = j.counter;
            FreeJob(job);
            Finish(counter);
        }

        void Finish(u32 counter)
        {
            Counter& k = sys->counters[counter];
            if (k.pending.fetch_sub(1, std::memory_order_acq_rel) != 1)
                return;
            k.lock.Lock();
            k.done = true;
            u32 waiting = k.waitingHead;
            k.waitingHead = 0;
            u32 g = k.generation.load(std::memory_order_relaxed) + 1;
            k.generation.store(g ? g : 1, std::memory_order_release);
            k.lock.Unlock();
            FreeCounter(counter);
            while (waiting)
            {
                u32 next = sys->jobs[waiting].nextWaiting;
                Release(waiting);
                waiting = next;
            }
        }

        void Execute(u32 job)
        {
            Job& j = sys->jobs[job];
            if (j.function)
            {
                profiler::Scope scope(jobMarker);
                j.function(j.data, j.begin, j.end);
            }
            u32 counter = j.counter;
            FreeJob(job);
            Finish(counter);
        }

        bool Wait(u32 job, JobHandle dependency)
        {
            if (!dependency.IsValid())
                return false;
            Counter& k = sys->counters[dependency.index];
            k.lock.Lock();
            bool queued = k.generation.load(std::memory_order_relaxed) == dependency.generation && !k.done;
            if (queued)
            {
                sys->jobs[job].nextWaiting = k.waitingHead;
                k.waitingHead = job;
            }
            k.lock.Unlock();
            return queued;
        }

        JobHandle HandleOf(u32 counter) { return {counter, sys->counters[counter].generation.load(std::memory_order_acquire)}; }

        void WorkerMain(void* param)
        {
            threadIndex = i32(uptr(param));
            char name[32];
            FormatToBuffer(name, sizeof(name), "Worker {}", threadIndex);
            profiler::SetThreadName(name);
            u32 idle = 0;
            while (sys->running.load(std::memory_order_acquire))
            {
                if (u32 job = FindWork())
                {
                    Execute(job);
                    idle = 0;
                    continue;
                }
                if (++idle < 4096)
                {
                    platform::SpinPause();
                    continue;
                }
                sys->sleeping.fetch_add(1, std::memory_order_seq_cst);
                if (u32 job = FindWork())
                {
                    sys->sleeping.fetch_sub(1, std::memory_order_relaxed);
                    Execute(job);
                    idle = 0;
                    continue;
                }
                sys->wake.Acquire();
                sys->sleeping.fetch_sub(1, std::memory_order_relaxed);
                u32 inFlight = sys->wakesInFlight.load(std::memory_order_relaxed);
                while (inFlight && !sys->wakesInFlight.compare_exchange_weak(inFlight, inFlight - 1, std::memory_order_relaxed))
                {
                }
                idle = 0;
            }
            jobCache.Flush(sys->freeJobs);
            counterCache.Flush(sys->freeCounters);
        }

        JobHandle ScheduleJobs(JobFunction function, void* data, u32 count, u32 batchSize, bool parallel, JobHandle dependency, const void* closure, usize closureSize)
        {
            HX_ASSERT_MSG(sys, "job system not initialized");
            if (parallel && count == 0)
                return {};
            batchSize = batchSize ? batchSize : 1;
            u32 batches = parallel ? (count + batchSize - 1) / batchSize : 1;
            u32 counter = AllocCounter(i32(batches));
            JobHandle handle = HandleOf(counter);
            if (closure)
            {
                std::memcpy(sys->counters[counter].closure, closure, closureSize);
                data = sys->counters[counter].closure;
            }
            for (u32 b = 0; b < batches; b++)
            {
                u32 job = AllocJob();
                u32 begin = parallel ? b * batchSize : 0;
                u32 end = parallel ? Min(count, begin + batchSize) : 1;
                sys->jobs[job] = {function, data, begin, end, counter, 0};
                if (!Wait(job, dependency))
                    Push(job);
            }
            return handle;
        }
    }

    bool Initialize(const JobSystemDesc& desc)
    {
        if (sys)
            return true;
        sys = New<System>(HeapAllocator());
        sys->freeJobs.Init(sys->jobNext, kMaxJobs);
        sys->freeCounters.Init(sys->counterNext, kMaxCounters);
        u32 workers = desc.workerCount >= 0 ? u32(desc.workerCount) : (platform::LogicalCoreCount() > 1 ? platform::LogicalCoreCount() - 1 : 0);
        if (!platform::ThreadsSupported())
            workers = 0;
        workers = Min(workers, kMaxThreads - 1);
        threadIndex = 0;
        sys->running.store(true, std::memory_order_release);
        for (u32 i = 0; i < workers; i++)
        {
            if (!sys->threads[i + 1].Start({.function = &WorkerMain, .user = reinterpret_cast<void*>(uptr(i + 1)), .name = "helix-worker"}))
                break;
            sys->workerCount++;
        }
        return true;
    }

    void Shutdown()
    {
        if (!sys)
            return;
        sys->running.store(false, std::memory_order_release);
        sys->wake.Release(sys->workerCount);
        for (u32 i = 0; i < sys->workerCount; i++)
            sys->threads[i + 1].Join();
        jobCache.count = 0;
        counterCache.count = 0;
        Delete(HeapAllocator(), sys);
        sys = nullptr;
        threadIndex = -1;
    }

    bool IsInitialized() { return sys != nullptr; }
    u32 WorkerCount() { return sys ? sys->workerCount : 0; }

    JobHandle Schedule(JobFunction function, void* data, JobHandle dependency)
    {
        return ScheduleJobs(function, data, 1, 1, false, dependency, nullptr, 0);
    }

    JobHandle ScheduleParallelFor(JobFunction function, void* data, u32 count, u32 batchSize, JobHandle dependency)
    {
        return ScheduleJobs(function, data, count, batchSize, true, dependency, nullptr, 0);
    }

    JobHandle detail::ScheduleClosure(JobFunction function, const void* closure, usize size, u32 count, u32 batchSize, bool parallel, JobHandle dependency)
    {
        HX_ASSERT(size <= kClosureSize);
        return ScheduleJobs(function, nullptr, count, batchSize, parallel, dependency, closure, size);
    }

    JobHandle CombineDependencies(Span<const JobHandle> handles)
    {
        u32 counter = AllocCounter(i32(handles.Size()) + 1);
        JobHandle handle = HandleOf(counter);
        for (const JobHandle& h : handles)
        {
            if (!h.IsValid())
            {
                Finish(counter);
                continue;
            }
            u32 job = AllocJob();
            sys->jobs[job] = {nullptr, nullptr, 0, 0, counter, 0};
            if (!Wait(job, h))
            {
                FreeJob(job);
                Finish(counter);
            }
        }
        Finish(counter);
        return handle;
    }

    JobHandle CombineDependencies(JobHandle a, JobHandle b)
    {
        JobHandle list[2] = {a, b};
        return CombineDependencies(Span<const JobHandle>(list, 2));
    }

    bool IsCompleted(JobHandle handle)
    {
        if (!handle.IsValid() || !sys)
            return true;
        return sys->counters[handle.index].generation.load(std::memory_order_acquire) != handle.generation;
    }

    void Complete(JobHandle handle)
    {
        while (!IsCompleted(handle))
        {
            if (u32 job = FindWork())
                Execute(job);
            else
                platform::SpinPause();
        }
    }
}
