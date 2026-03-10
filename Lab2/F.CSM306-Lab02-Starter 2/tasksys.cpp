#include "tasksys.h"

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    auto promise_ptr = std::make_shared<std::promise<void>>();
    auto future = promise_ptr->get_future().share();

    TaskID my_id = next_task_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_[my_id] = std::make_shared<std::shared_future<void>>(future);
    }

    std::thread([this, runnable, num_total_tasks, deps, promise_ptr]() {
        // Wait for all dependencies
        for (TaskID dep : deps)
        {
            std::shared_ptr<std::shared_future<void>> dep_future;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                auto it = async_futures_.find(dep);
                if (it != async_futures_.end())
                {
                    dep_future = it->second;
                }
            }
            if (dep_future)
                dep_future->wait();
        }

        // Execute the work
        for (int i = 0; i < num_total_tasks; i++)
        {
            runnable->runTask(i, num_total_tasks);
        }

        promise_ptr->set_value();
    }).detach();

    return my_id;
}

void TaskSystemSerial::sync()
{
    std::vector<std::shared_ptr<std::shared_future<void>>> futures_copy;
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        futures_copy.reserve(async_futures_.size());
        for (auto &pair : async_futures_)
            futures_copy.push_back(pair.second);
    }

    for (auto &fut_ptr : futures_copy)
    {
        if (fut_ptr)
            fut_ptr->wait();
    }

    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_.clear();
    }
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads), num_threads_(num_threads)
{
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
        return;

    // Static partitioning: split tasks evenly among threads.
    int tasks_per_thread = num_total_tasks / num_threads_;
    std::vector<std::thread> threads;
    threads.reserve(num_threads_);

    for (int i = 0; i < num_threads_; i++)
    {
        int start_index = i * tasks_per_thread;
        // The last thread takes any remaining tasks as well.
        int end_index = (i == num_threads_ - 1) ? num_total_tasks : start_index + tasks_per_thread;

        threads.emplace_back([runnable, start_index, end_index, num_total_tasks]() {
            for (int j = start_index; j < end_index; j++)
            {
                runnable->runTask(j, num_total_tasks);
            }
        });
    }

    for (auto &t : threads)
        t.join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    auto promise_ptr = std::make_shared<std::promise<void>>();
    auto future = promise_ptr->get_future().share();

    TaskID my_id = next_task_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_[my_id] = std::make_shared<std::shared_future<void>>(future);
    }

    std::thread([this, runnable, num_total_tasks, deps, promise_ptr]() {
        for (TaskID dep : deps)
        {
            std::shared_ptr<std::shared_future<void>> dep_future;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                auto it = async_futures_.find(dep);
                if (it != async_futures_.end())
                {
                    dep_future = it->second;
                }
            }
            if (dep_future)
                dep_future->wait();
        }

        // Execute tasks in parallel (spawned)
        std::atomic<int> next_task{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads_);
        auto worker = [&](void) {
            while (true)
            {
                int task_id = next_task.fetch_add(1);
                if (task_id >= num_total_tasks)
                    break;
                runnable->runTask(task_id, num_total_tasks);
            }
        };
        for (int i = 0; i < num_threads_; i++)
            threads.emplace_back(worker);
        for (auto &t : threads)
            if (t.joinable())
                t.join();

        promise_ptr->set_value();
    }).detach();

    return my_id;
}

void TaskSystemParallelSpawn::sync()
{
    std::vector<std::shared_ptr<std::shared_future<void>>> futures_copy;
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        futures_copy.reserve(async_futures_.size());
        for (auto &entry : async_futures_)
            futures_copy.push_back(entry.second);
    }

    for (auto &fut_ptr : futures_copy)
    {
        if (fut_ptr)
            fut_ptr->wait();
    }

    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_.clear();
    }
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads)
{
    stop_.store(false);
    has_work_.store(false);
    next_task_.store(0);
    remaining_tasks_.store(0);
    current_runnable_ = nullptr;
    current_num_tasks_ = 0;

    threads_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; ++i)
    {
        threads_.emplace_back([this]() {
            while (!stop_.load(std::memory_order_acquire))
            {
                if (!has_work_.load(std::memory_order_acquire))
                {
                    std::this_thread::yield();
                    continue;
                }

                int task_id = next_task_.fetch_add(1);
                if (task_id >= current_num_tasks_)
                {
                    std::this_thread::yield();
                    continue;
                }

                if (current_runnable_)
                {
                    current_runnable_->runTask(task_id, current_num_tasks_);
                }

                if (remaining_tasks_.fetch_sub(1) == 1)
                {
                    has_work_.store(false, std::memory_order_release);
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    stop_.store(true, std::memory_order_release);
    for (auto &t : threads_)
    {
        if (t.joinable())
            t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
        return;

    // Ensure any previous work has completed
    sync();

    current_runnable_ = runnable;
    current_num_tasks_ = num_total_tasks;
    next_task_.store(0, std::memory_order_release);
    remaining_tasks_.store(num_total_tasks, std::memory_order_release);
    has_work_.store(true, std::memory_order_release);

    // Wait until all tasks are done
    while (remaining_tasks_.load(std::memory_order_acquire) > 0)
    {
        std::this_thread::yield();
    }

    has_work_.store(false, std::memory_order_release);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    auto promise_ptr = std::make_shared<std::promise<void>>();
    auto future = promise_ptr->get_future().share();

    TaskID my_id = next_task_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_[my_id] = std::make_shared<std::shared_future<void>>(future);
    }

    std::thread([this, runnable, num_total_tasks, deps, promise_ptr]() {
        for (TaskID dep : deps)
        {
            std::shared_ptr<std::shared_future<void>> dep_future;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                auto it = async_futures_.find(dep);
                if (it != async_futures_.end())
                {
                    dep_future = it->second;
                }
            }
            if (dep_future)
                dep_future->wait();
        }

        run(runnable, num_total_tasks);
        promise_ptr->set_value();
    }).detach();

    return my_id;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    std::vector<std::shared_ptr<std::shared_future<void>>> futures_copy;
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        futures_copy.reserve(async_futures_.size());
        for (auto &entry : async_futures_)
            futures_copy.push_back(entry.second);
    }

    for (auto &fut_ptr : futures_copy)
    {
        if (fut_ptr)
            fut_ptr->wait();
    }

    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_.clear();
    }
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads)
{
    stop_ = false;
    has_active_job_.store(false);

    // Worker threads: execute tasks from ready jobs (running multiple jobs concurrently).
    threads_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; ++i)
    {
        threads_.emplace_back([this]() {
            while (true)
            {
                TaskID job_id;
                {
                    std::unique_lock<std::mutex> lock(work_mutex_);
                    work_cv_.wait(lock, [this] { return stop_ || !ready_queue_.empty(); });
                    if (stop_)
                        return;
                    job_id = ready_queue_.front();
                    ready_queue_.pop_front();
                }

                std::shared_ptr<std::promise<void>> promise_to_set;
                std::vector<TaskID> newly_ready;

                // Grab a reference to the job without holding the lock while doing work.
                std::shared_ptr<AsyncJob> job;
                {
                    std::lock_guard<std::mutex> lock(work_mutex_);
                    auto it = jobs_.find(job_id);
                    if (it == jobs_.end())
                        continue;
                    job = it->second;
                }

                // Execute tasks for the job using shared atomic counter
                while (true)
                {
                    int task_id = job->next_task.fetch_add(1);
                    if (task_id >= job->num_total_tasks)
                        break;
                    if (job->runnable)
                        job->runnable->runTask(task_id, job->num_total_tasks);
                }

                // If this thread finished the job, notify dependents once
                if (!job->done.exchange(true))
                {
                    promise_to_set = job->promise;

                    std::lock_guard<std::mutex> lock(work_mutex_);

                    auto dit = dependents_.find(job_id);
                    if (dit != dependents_.end())
                    {
                        for (TaskID dependent : dit->second)
                        {
                            auto jt = jobs_.find(dependent);
                            if (jt == jobs_.end())
                                continue;

                            jt->second->remaining_deps -= 1;
                            if (jt->second->remaining_deps == 0)
                                newly_ready.push_back(dependent);
                        }
                        dependents_.erase(dit);
                    }

                    // Clean up job entry
                    jobs_.erase(job_id);
                }

                if (promise_to_set)
                    promise_to_set->set_value();

                if (!newly_ready.empty())
                {
                    std::lock_guard<std::mutex> lock(work_mutex_);
                    for (TaskID id : newly_ready)
                        ready_queue_.push_back(id);
                    work_cv_.notify_all();
                }
            }
        });
    }
}


TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    {
        std::lock_guard<std::mutex> lock(work_mutex_);
        stop_ = true;
    }
    work_cv_.notify_all();

    for (auto &t : threads_)
    {
        if (t.joinable())
            t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
        return;

    // Enqueue the job and wait for it to complete.
    TaskID id = runAsyncWithDeps(runnable, num_total_tasks, {});

    std::shared_ptr<std::shared_future<void>> fut;
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        auto it = async_futures_.find(id);
        if (it != async_futures_.end())
            fut = it->second;
    }

    if (fut)
        fut->wait();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    auto promise_ptr = std::make_shared<std::promise<void>>();
    auto future = promise_ptr->get_future().share();

    TaskID my_id = next_task_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_[my_id] = std::make_shared<std::shared_future<void>>(future);
    }

    auto job = std::make_shared<AsyncJob>();
    job->runnable = runnable;
    job->num_total_tasks = num_total_tasks;
    job->promise = promise_ptr;
    job->remaining_deps = static_cast<int>(deps.size());
    job->deps = deps;

    {
        std::lock_guard<std::mutex> lock(work_mutex_);
        jobs_[my_id] = job;

        if (job->remaining_deps == 0)
        {
            // Make the job available to all workers so tasks can be performed in parallel
            for (int i = 0; i < num_threads_; ++i)
                ready_queue_.push_back(my_id);
            work_cv_.notify_all();
        }
        else
        {
            for (TaskID dep : deps)
                dependents_[dep].push_back(my_id);
        }
    }

    return my_id;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    std::vector<std::shared_ptr<std::shared_future<void>>> futures_copy;
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        futures_copy.reserve(async_futures_.size());
        for (auto &entry : async_futures_)
            futures_copy.push_back(entry.second);
    }

    for (auto &fut_ptr : futures_copy)
    {
        if (fut_ptr)
            fut_ptr->wait();
    }

    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_.clear();
    }
}