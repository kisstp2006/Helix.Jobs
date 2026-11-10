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

        struct alignas(64) Job
        {
            JobFunction function;
            void* data;
            u32 begin;
            u32 end;
            u32 counter;
            u32 nextWaiting;
        };

        constexpr u32 kClosed = 0xFFFFFFFFu;
        constexpr u64 Pack(u32 generation, u32 job) { return (u64(generation) << 32) | job; }

        // waiting holds (generation, first waiting job); a closed list or another generation refuses new waiters.
        struct alignas(64) Counter
        {
            std::atomic<i32> pending{0};
            std::atomic<u32> generation{1};
            std::atomic<u64> waiting{Pack(1, kClosed)};
            alignas(16) u8 closure[detail::kClosureSize];
        };

        // Free indices move between threads in chains of kChain, one tagged CAS per chain.
        class IndexPool
        {
        public:
            static constexpr u32 kChain = 64;

            void Init(std::atomic<u32>* next, std::atomic<u32>* chainNext, u32 count)
            {
                next_ = next;
                chainNext_ = chainNext;
                count_ = count;
                fresh_.store(1, std::memory_order_relaxed);
                head_.store(0, std::memory_order_relaxed);
            }

            void PushChain(u32 first)
            {
                u64 old = head_.load(std::memory_order_relaxed);
                u64 desired;
                do
                {
                    chainNext_[first].store(u32(old), std::memory_order_relaxed);
                    desired = (((old >> 32) + 1) << 32) | first;
                } while (!head_.compare_exchange_weak(old, desired, std::memory_order_release, std::memory_order_relaxed));
            }

            // Fills out with up to kChain indices, returns how many.
            u32 PopChain(u32* out)
            {
                u64 old = head_.load(std::memory_order_acquire);
                for (;;)
                {
                    u32 first = u32(old);
                    if (!first)
                        break;
                    u32 nextChain = chainNext_[first].load(std::memory_order_relaxed);
                    u64 desired = (((old >> 32) + 1) << 32) | nextChain;
                    if (head_.compare_exchange_weak(old, desired, std::memory_order_acquire, std::memory_order_acquire))
                    {
                        u32 n = 0;
                        for (u32 i = first; i && n < kChain; i = next_[i].load(std::memory_order_relaxed))
                            out[n++] = i;
                        return n;
                    }
                }
                u32 start = fresh_.fetch_add(kChain, std::memory_order_relaxed);
                if (start >= count_)
                    return 0;
                u32 n = Min(kChain, count_ - start);
                for (u32 i = 0; i < n; i++)
                    out[i] = start + i;
                return n;
            }

            void LinkAndPush(const u32* items, u32 n)
            {
                for (u32 i = 0; i + 1 < n; i++)
                    next_[items[i]].store(items[i + 1], std::memory_order_relaxed);
                next_[items[n - 1]].store(0, std::memory_order_relaxed);
                PushChain(items[0]);
            }

        private:
            std::atomic<u64> head_{0};
            std::atomic<u32> fresh_{1};
            std::atomic<u32>* next_ = nullptr;
            std::atomic<u32>* chainNext_ = nullptr;
            u32 count_ = 0;
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

        // Bounded MPMC queue (sequence number per cell): the inbox of one worker.
        struct alignas(64) Inbox
        {
            static constexpr u32 kCapacity = 1024;
            struct Cell
            {
                std::atomic<u32> sequence;
                u32 job;
            };

            Cell cells[kCapacity];
            alignas(64) std::atomic<u32> enqueuePos{0};
            alignas(64) std::atomic<u32> dequeuePos{0};

            Inbox()
            {
                for (u32 i = 0; i < kCapacity; i++)
                    cells[i].sequence.store(i, std::memory_order_relaxed);
            }

            bool Push(u32 job)
            {
                u32 pos = enqueuePos.load(std::memory_order_relaxed);
                for (;;)
                {
                    Cell& c = cells[pos & (kCapacity - 1)];
                    i32 dif = i32(c.sequence.load(std::memory_order_acquire) - pos);
                    if (dif == 0)
                    {
                        if (enqueuePos.compare_exchange_weak(pos, pos + 1, std::memory_order_seq_cst, std::memory_order_relaxed))
                        {
                            c.job = job;
                            c.sequence.store(pos + 1, std::memory_order_release);
                            return true;
                        }
                    }
                    else if (dif < 0)
                        return false;
                    else
                        pos = enqueuePos.load(std::memory_order_relaxed);
                }
            }

            u32 Pop()
            {
                u32 pos = dequeuePos.load(std::memory_order_relaxed);
                for (;;)
                {
                    Cell& c = cells[pos & (kCapacity - 1)];
                    i32 dif = i32(c.sequence.load(std::memory_order_acquire) - (pos + 1));
                    if (dif == 0)
                    {
                        if (dequeuePos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                        {
                            u32 job = c.job;
                            c.sequence.store(pos + kCapacity, std::memory_order_release);
                            return job;
                        }
                    }
                    else if (dif < 0)
                        return 0;
                    else
                        pos = dequeuePos.load(std::memory_order_relaxed);
                }
            }
        };

        struct IndexCache
        {
            static constexpr u32 kCapacity = 4 * IndexPool::kChain;
            u32 items[kCapacity];
            u32 count = 0;

            u32 Pop(IndexPool& pool)
            {
                if (!count)
                    count = pool.PopChain(items);
                return count ? items[--count] : 0;
            }

            void Push(IndexPool& pool, u32 index)
            {
                if (count == kCapacity)
                {
                    count -= IndexPool::kChain;
                    pool.LinkAndPush(items + count, IndexPool::kChain);
                }
                items[count++] = index;
            }

            void Flush(IndexPool& pool)
            {
                while (count)
                {
                    u32 n = Min(count, IndexPool::kChain);
                    count -= n;
                    pool.LinkAndPush(items + count, n);
                }
            }
        };

        struct System
        {
            Job jobs[kMaxJobs];
            std::atomic<u32> jobNext[kMaxJobs];
            std::atomic<u32> jobChainNext[kMaxJobs];
            Counter counters[kMaxCounters];
            std::atomic<u32> counterNext[kMaxCounters];
            std::atomic<u32> counterChainNext[kMaxCounters];
            IndexPool freeJobs;
            IndexPool freeCounters;
            Deque deques[kMaxThreads];
            Inbox inboxes[kMaxThreads];
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

        thread_local u32 nextInbox = 0;

        // Jobs scheduled from worker jobs stay local; jobs from the main or foreign threads are dealt out to worker inboxes.
        void Push(u32 job)
        {
            bool pushed = false, fenced = false;
            if (threadIndex > 0)
                pushed = sys->deques[threadIndex].Push(job);
            else if (sys->workerCount > 0)
                pushed = fenced = sys->inboxes[1 + (nextInbox++ % sys->workerCount)].Push(job);
            if (!pushed && threadIndex >= 0)
                pushed = sys->deques[threadIndex].Push(job);
            if (!pushed)
                PushGlobal(job);
            if (!fenced)
                std::atomic_thread_fence(std::memory_order_seq_cst);
            if (sys->sleeping.load(std::memory_order_relaxed) > sys->wakesInFlight.load(std::memory_order_relaxed))
            {
                sys->wakesInFlight.fetch_add(1, std::memory_order_relaxed);
                sys->wake.Release();
            }
        }

        thread_local u32 stealSeed = 0x9e3779b9u;

        u32 FindWork()
        {
            u32 self = threadIndex >= 0 ? u32(threadIndex) : 0;
            if (threadIndex >= 0)
            {
                if (u32 job = sys->deques[self].Pop())
                    return job;
                if (u32 job = sys->inboxes[self].Pop())
                    return job;
            }
            u32 threads = sys->workerCount + 1;
            if (threads > 1)
            {
                stealSeed ^= stealSeed << 13;
                stealSeed ^= stealSeed >> 17;
                stealSeed ^= stealSeed << 5;
                u32 start = stealSeed % threads;
                for (u32 k = 0; k < threads; k++)
                {
                    u32 victim = (start + k) % threads;
                    if (threadIndex >= 0 && victim == self)
                        continue;
                    if (u32 job = sys->inboxes[victim].Pop())
                        return job;
                    if (u32 job = sys->deques[victim].Steal())
                        return job;
                }
            }
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
            k.pending.store(pending, std::memory_order_relaxed);
            k.waiting.store(Pack(k.generation.load(std::memory_order_relaxed), 0), std::memory_order_release);
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
            u32 generation = k.generation.load(std::memory_order_relaxed);
            u32 waiting = u32(k.waiting.exchange(Pack(generation, kClosed), std::memory_order_acq_rel));
            u32 nextGeneration = generation + 1;
            k.generation.store(nextGeneration ? nextGeneration : 1, std::memory_order_release);
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
            u64 head = k.waiting.load(std::memory_order_acquire);
            for (;;)
            {
                if (u32(head >> 32) != dependency.generation || u32(head) == kClosed)
                    return false;
                sys->jobs[job].nextWaiting = u32(head);
                if (k.waiting.compare_exchange_weak(head, Pack(dependency.generation, job), std::memory_order_acq_rel, std::memory_order_acquire))
                    return true;
            }
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
        sys->freeJobs.Init(sys->jobNext, sys->jobChainNext, kMaxJobs);
        sys->freeCounters.Init(sys->counterNext, sys->counterChainNext, kMaxCounters);
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
