#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    std::mutex async_mutex_;
    std::unordered_map<TaskID, std::shared_ptr<std::shared_future<void>>> async_futures_;
    std::atomic<TaskID> next_task_id_{0};
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    int num_threads_;
    std::mutex async_mutex_;
    std::unordered_map<TaskID, std::shared_ptr<std::shared_future<void>>> async_futures_;
    std::atomic<TaskID> next_task_id_{0};
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    int num_threads_;
    std::vector<std::thread> threads_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> has_work_{false};
    std::atomic<int> next_task_{0};
    std::atomic<int> remaining_tasks_{0};
    IRunnable *current_runnable_{nullptr};
    int current_num_tasks_{0};

    std::mutex async_mutex_;
    std::unordered_map<TaskID, std::shared_ptr<std::shared_future<void>>> async_futures_;
    std::atomic<TaskID> next_task_id_{0};
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    struct AsyncJob
    {
        IRunnable *runnable;
        int num_total_tasks;
        std::atomic<int> next_task{0};
        std::shared_ptr<std::promise<void>> promise;
        std::atomic<bool> done{false};
        int remaining_deps;
        std::vector<TaskID> deps;
    };

    int num_threads_;
    std::vector<std::thread> threads_;
    std::thread scheduler_thread_;
    std::atomic<bool> stop_{false};

    // Execution state for the currently active job
    std::atomic<bool> has_active_job_{false};
    IRunnable *active_runnable_{nullptr};
    int active_num_tasks_{0};
    std::atomic<int> next_task_{0};
    std::atomic<int> remaining_tasks_{0};
    std::condition_variable done_cv_;

    // Dependency scheduling
    std::mutex work_mutex_;
    std::condition_variable work_cv_;
    std::deque<TaskID> ready_queue_;
    std::unordered_map<TaskID, std::shared_ptr<AsyncJob>> jobs_;
    std::unordered_map<TaskID, std::vector<TaskID>> dependents_;

    // Async tracking
    std::mutex async_mutex_;
    std::unordered_map<TaskID, std::shared_ptr<std::shared_future<void>>> async_futures_;
    std::atomic<TaskID> next_task_id_{0};
};

#endif