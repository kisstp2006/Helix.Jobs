#pragma once

#include <helix/core/base/span.h>
#include <helix/core/base/utility.h>

#include <cstring>
#include <new>
#include <type_traits>

namespace helix::jobs
{
    struct JobHandle
    {
        u32 index = 0;
        u32 generation = 0;
        bool IsValid() const { return generation != 0; }
    };

    using JobFunction = void (*)(void* data, u32 begin, u32 end);

    struct JobSystemDesc
    {
        // -1 picks logical cores minus one; 0 runs every job on the thread that calls Complete.
        i32 workerCount = -1;
    };

    bool Initialize(const JobSystemDesc& desc = {});
    void Shutdown();
    bool IsInitialized();
    u32 WorkerCount();

    JobHandle Schedule(JobFunction function, void* data, JobHandle dependency = {});
    JobHandle ScheduleParallelFor(JobFunction function, void* data, u32 count, u32 batchSize, JobHandle dependency = {});
    JobHandle CombineDependencies(Span<const JobHandle> handles);
    JobHandle CombineDependencies(JobHandle a, JobHandle b);
    void Complete(JobHandle handle);
    bool IsCompleted(JobHandle handle);

    namespace detail
    {
        inline constexpr usize kClosureSize = 48;
        JobHandle ScheduleClosure(JobFunction function, const void* closure, usize size, u32 count, u32 batchSize, bool parallel, JobHandle dependency);

        template <typename F>
        void InvokeOnce(void* data, u32, u32)
        {
            (*static_cast<F*>(data))();
        }

        template <typename F>
        void InvokeRange(void* data, u32 begin, u32 end)
        {
            F& f = *static_cast<F*>(data);
            for (u32 i = begin; i < end; i++)
                f(i);
        }
    }

    template <typename F>
    JobHandle Schedule(const F& f, JobHandle dependency = {})
    {
        static_assert(sizeof(F) <= detail::kClosureSize && std::is_trivially_copyable_v<F> && std::is_trivially_destructible_v<F>,
                      "job closures must be small and trivially copyable");
        return detail::ScheduleClosure(&detail::InvokeOnce<F>, &f, sizeof(F), 1, 1, false, dependency);
    }

    template <typename F>
    JobHandle ParallelFor(u32 count, u32 batchSize, const F& f, JobHandle dependency = {})
    {
        static_assert(sizeof(F) <= detail::kClosureSize && std::is_trivially_copyable_v<F> && std::is_trivially_destructible_v<F>,
                      "job closures must be small and trivially copyable");
        return detail::ScheduleClosure(&detail::InvokeRange<F>, &f, sizeof(F), count, batchSize, true, dependency);
    }
}
