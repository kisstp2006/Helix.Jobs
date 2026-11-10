#include <Helix/Core/Logging/Log.h>
#include <Helix/Jobs/Jobs.h>
#include <Helix/Platform/IO/FileSystem.h>
#include <Helix/Platform/Time/Time.h>
#include <Helix/Platform/Window/Window.h>
#include <Helix/Profiler/Profiler.h>

#include <cmath>

using namespace helix;
using namespace helix::platform;

namespace
{
    LogChannel DemoLog{"FrameDemo"};
    profiler::Counter workCounter{"demo.workMs"};

    constexpr u32 kParticles = 1u << 18;
    constexpr u32 kCaptureFrames = 120;

    struct Demo
    {
        Window* window = nullptr;
        Array<Event> events;
        f32* positions = nullptr;
        f32* velocities = nullptr;
        Stopwatch clock;
        u64 frame = 0;
        f64 autoClose = 0;
        f64 workTotal = 0;
        bool saved = false;
    };

    void Simulate(Demo& d, f32 dt)
    {
        HX_PROFILE_SCOPE("Simulate");
        f32* p = d.positions;
        f32* v = d.velocities;
        jobs::JobHandle integrate = jobs::ParallelFor(kParticles, 4096, [p, v, dt](u32 i) {
            v[i] -= p[i] * dt;
            p[i] += v[i] * dt;
        });
        jobs::JobHandle damp = jobs::ParallelFor(kParticles, 4096, [v](u32 i) { v[i] *= 0.9999f; }, integrate);
        jobs::Complete(damp);
    }

    void SaveCapture(Demo& d)
    {
        profiler::EndCapture();
        String path = JoinPath(TempDirectory(), "helix_frame_demo.hxprof");
        bool ok = profiler::SaveCapture(path);
        Array<u8> bytes;
        ReadFile(path, bytes);
        HX_LOG_INFO(DemoLog, "profiler capture {} ({} bytes): {}", ok ? "saved" : "FAILED", bytes.Size(), path);
        String trace = JoinPath(TempDirectory(), "helix_frame_demo.json");
        profiler::SaveChromeTrace(trace);
        HX_LOG_INFO(DemoLog, "chrome trace: {}", trace);
        HX_LOG_INFO(DemoLog, "average simulation {:.3} ms per frame over {} frames, {} workers", d.workTotal / f64(kCaptureFrames), kCaptureFrames,
                    jobs::WorkerCount());
        d.saved = true;
    }

    bool Frame(void* user)
    {
        auto& d = *static_cast<Demo*>(user);
        profiler::MarkFrame();
        HX_PROFILE_SCOPE("Frame");
        d.events.Clear();
        d.window->PollEvents(d.events);
        for (const Event& e : d.events)
        {
            if (e.type == EventType::KeyDown)
                HX_LOG_INFO(DemoLog, "key {}", KeyCodeName(e.key.key));
            else if (e.type == EventType::MouseDown)
                HX_LOG_INFO(DemoLog, "mouse button {} at {} {}", int(e.mouse.button), e.mouse.x, e.mouse.y);
        }
        Stopwatch work;
        Simulate(d, 1.0f / 60.0f);
        f64 ms = work.ElapsedMilliseconds();
        workCounter.Set(ms);
        if (d.frame < kCaptureFrames)
            d.workTotal += ms;
        if (++d.frame == kCaptureFrames)
            SaveCapture(d);
        if (d.autoClose > 0 && d.clock.ElapsedSeconds() > d.autoClose)
            return false;
        return !d.window->CloseRequested();
    }
}

int main(int argc, char** argv)
{
    static Demo demo;
    for (int i = 1; i < argc; i++)
        if (StringView(argv[i]) == "--auto-close")
            demo.autoClose = 4.0;
    jobs::Initialize();
    profiler::SetThreadName("Main");
    profiler::BeginCapture();
    demo.positions = static_cast<f32*>(HeapAllocator().Allocate(kParticles * sizeof(f32), 64));
    demo.velocities = static_cast<f32*>(HeapAllocator().Allocate(kParticles * sizeof(f32), 64));
    for (u32 i = 0; i < kParticles; i++)
    {
        demo.positions[i] = std::sin(f32(i));
        demo.velocities[i] = 0.0f;
    }
    demo.window = Window::Create({.title = "Helix frame demo", .width = 960, .height = 540});
    if (!demo.window)
    {
        HX_LOG_ERROR(DemoLog, "no window support on this system");
        return 1;
    }
    HX_LOG_INFO(DemoLog, "{} particles, {} worker threads", kParticles, jobs::WorkerCount());
    RunMainLoop(&Frame, &demo);
#if !HX_PLATFORM_WEB
    if (!demo.saved)
        SaveCapture(demo);
    Window::Destroy(demo.window);
    jobs::Shutdown();
#endif
    return 0;
}
