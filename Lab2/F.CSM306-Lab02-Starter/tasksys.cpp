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
        async_futures_.push_back(std::make_shared<std::shared_future<void>>(future));
    }

    std::thread([this, runnable, num_total_tasks, deps, promise_ptr]() {
        // Wait for all dependencies
        for (TaskID dep : deps)
        {
            std::shared_ptr<std::shared_future<void>> dep_future;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                if (dep < (TaskID)async_futures_.size())
                {
                    dep_future = async_futures_[dep];
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
        futures_copy = async_futures_;
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
    {
        threads.emplace_back(worker);
    }

    for (auto &t : threads)
    {
        if (t.joinable())
            t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    auto promise_ptr = std::make_shared<std::promise<void>>();
    auto future = promise_ptr->get_future().share();

    TaskID my_id = next_task_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_.push_back(std::make_shared<std::shared_future<void>>(future));
    }

    std::thread([this, runnable, num_total_tasks, deps, promise_ptr]() {
        for (TaskID dep : deps)
        {
            std::shared_ptr<std::shared_future<void>> dep_future;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                if (dep < (TaskID)async_futures_.size())
                {
                    dep_future = async_futures_[dep];
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
        futures_copy = async_futures_;
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
        async_futures_.push_back(std::make_shared<std::shared_future<void>>(future));
    }

    std::thread([this, runnable, num_total_tasks, deps, promise_ptr]() {
        for (TaskID dep : deps)
        {
            std::shared_ptr<std::shared_future<void>> dep_future;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                if (dep < (TaskID)async_futures_.size())
                {
                    dep_future = async_futures_[dep];
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
        futures_copy = async_futures_;
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
    has_work_ = false;
    next_task_.store(0);
    remaining_tasks_.store(0);
    current_runnable_ = nullptr;
    current_num_tasks_ = 0;

    threads_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; ++i)
    {
        threads_.emplace_back([this]() {
            while (true)
            {
                // Sleep until there's work or the pool is stopping
                std::unique_lock<std::mutex> lock(work_mutex_);
                work_cv_.wait(lock, [this] { return stop_ || has_work_; });
                if (stop_)
                    break;

                // Capture shared work state while holding the mutex, then run without holding it.
                IRunnable *runnable = current_runnable_;
                int num_tasks = current_num_tasks_;
                lock.unlock();

                while (true)
                {
                    int task_id = next_task_.fetch_add(1);
                    if (task_id >= num_tasks)
                        break;

                    if (runnable)
                        runnable->runTask(task_id, num_tasks);

                    if (remaining_tasks_.fetch_sub(1) == 1)
                    {
                        // Last task completed: notify the waiting thread
                        std::lock_guard<std::mutex> guard(work_mutex_);
                        has_work_ = false;
                        done_cv_.notify_one();
                    }
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
        has_work_ = true;
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

    // Ensure any previous asynchronous work has completed
    sync();

    {
        std::lock_guard<std::mutex> lock(work_mutex_);
        current_runnable_ = runnable;
        current_num_tasks_ = num_total_tasks;
        next_task_.store(0);
        remaining_tasks_.store(num_total_tasks);
        has_work_ = true;
    }

    work_cv_.notify_all();

    std::unique_lock<std::mutex> lock(work_mutex_);
    done_cv_.wait(lock, [this] { return remaining_tasks_.load() == 0; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    auto promise_ptr = std::make_shared<std::promise<void>>();
    auto future = promise_ptr->get_future().share();

    TaskID my_id = next_task_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        async_futures_.push_back(std::make_shared<std::shared_future<void>>(future));
    }

    std::thread([this, runnable, num_total_tasks, deps, promise_ptr]() {
        for (TaskID dep : deps)
        {
            std::shared_ptr<std::shared_future<void>> dep_future;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                if (dep < (TaskID)async_futures_.size())
                {
                    dep_future = async_futures_[dep];
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

void TaskSystemParallelThreadPoolSleeping::sync()
{
    std::vector<std::shared_ptr<std::shared_future<void>>> futures_copy;
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        futures_copy = async_futures_;
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