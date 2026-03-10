#include <iostream>
#include <vector>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <thread>
#include "tasksys.h"

// Тооцоолол хийх "Ажил" (Task) класс
class ComputeTask : public IRunnable
{
public:
    std::vector<double> results;
    int workload_intensity;

    ComputeTask(int num_tasks, int intensity)
        : results(num_tasks, 0.0), workload_intensity(intensity) {}

    void runTask(int taskID, int num_total_tasks) override
    {
        double val = 0.0;
        for (int i = 0; i < workload_intensity; ++i)
        {
            val += std::sin(i * 0.01 + taskID) * std::cos(i * 0.02 + taskID);
        }
        results[taskID] = val;
    }
};

double runBenchmark(ITaskSystem *system, IRunnable *task, int num_tasks, const std::string &name)
{
    std::cout << "Testing [" << name << "]..." << std::flush;
    auto start = std::chrono::high_resolution_clock::now();
    system->run(task, num_tasks);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    double seconds = elapsed.count();
    std::cout << " Done. Time: " << std::fixed << std::setprecision(4) << seconds << "s" << std::endl;
    return seconds;
}


double runBenchmarkAveraged(ITaskSystem *system, IRunnable *task, int num_tasks, const std::string &name, int runs)
{
    double total = 0.0;
    for (int i = 0; i < runs; ++i)
    {
        total += runBenchmark(system, task, num_tasks, name + " (run " + std::to_string(i + 1) + ")");
    }
    return total / runs;
}

int main(int argc, char *argv[])
{
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0)
        num_threads = 4;

    // Defaults
    int num_tasks = 5000;
    int workload_intensity = 2000;
    int runs = 3;

    // Optional command-line overrides: ./app [num_tasks] [workload_intensity] [runs]
    if (argc >= 2)
        num_tasks = std::stoi(argv[1]);
    if (argc >= 3)
        workload_intensity = std::stoi(argv[2]);
    if (argc >= 4)
    {
        runs = std::stoi(argv[3]);
        const int kMaxRuns = 20;
        if (runs < 1 || runs > kMaxRuns)
        {
            std::cerr << "Warning: runs=" << runs << " is out of range; clamping to " << kMaxRuns << ".\n";
            runs = std::max(1, std::min(runs, kMaxRuns));
        }
    }

    std::cout << "========================================" << std::endl;
    std::cout << "Task System Benchmark" << std::endl;
    std::cout << "Threads: " << num_threads << ", Tasks: " << num_tasks
              << ", Intensity: " << workload_intensity << ", Runs: " << runs << std::endl;
    std::cout << "========================================" << std::endl;

    ComputeTask task(num_tasks, workload_intensity);

    // 1. Serial System
    ITaskSystem *serialSystem = new TaskSystemSerial(num_threads);
    double serialTime = runBenchmarkAveraged(serialSystem, &task, num_tasks, "Serial System", runs);
    delete serialSystem;

    // 2. Parallel Spawn System (Step 1)
    ITaskSystem *spawnSystem = new TaskSystemParallelSpawn(num_threads);
    double spawnTime = runBenchmarkAveraged(spawnSystem, &task, num_tasks, "Parallel Spawn", runs);
    delete spawnSystem;

    // 3. Parallel Spinning System (Step 2)
    ITaskSystem *spinningSystem = new TaskSystemParallelThreadPoolSpinning(num_threads);
    double spinningTime = runBenchmarkAveraged(spinningSystem, &task, num_tasks, "Parallel Spinning Pool", runs);
    delete spinningSystem;

    // 4. Parallel Sleeping System (Step 3)
    ITaskSystem *sleepingSystem = new TaskSystemParallelThreadPoolSleeping(num_threads);
    double sleepingTime = runBenchmarkAveraged(sleepingSystem, &task, num_tasks, "Parallel Sleeping Pool", runs);
    delete sleepingSystem;

    std::cout << "========================================" << std::endl;
    std::cout << "Average times (s):\n"
              << "  Serial:   " << std::fixed << std::setprecision(4) << serialTime << "\n"
              << "  Spawn:    " << spawnTime << "\n"
              << "  Spinning: " << spinningTime << "\n"
              << "  Sleeping: " << sleepingTime << "\n";

    return 0;
}
