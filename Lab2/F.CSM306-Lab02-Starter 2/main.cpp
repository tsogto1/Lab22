#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include "tasksys.h"

class SimpleTask : public IRunnable
{
public:
    std::string name;

    SimpleTask(std::string n)
    {
        name = n;
    }

    void runTask(int task_id, int num_total_tasks) override
    {
        std::cout << name << " task " << task_id << " running on thread "
                  << std::this_thread::get_id() << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
};

int main()
{
    int num_threads = std::thread::hardware_concurrency();

    if (num_threads == 0)
        num_threads = 4;

    TaskSystemParallelThreadPoolSleeping system(num_threads);

    std::cout << "Running dependency example\n";

    SimpleTask taskA("A");
    SimpleTask taskB("B");
    SimpleTask taskC("C");
    SimpleTask taskD("D");

    TaskID A = system.runAsyncWithDeps(&taskA, 128, {});

    TaskID B = system.runAsyncWithDeps(&taskB, 2, {A});

    TaskID C = system.runAsyncWithDeps(&taskC, 6, {A});

    TaskID D = system.runAsyncWithDeps(&taskD, 32, {B, C});

    system.sync();

    std::cout << "All tasks completed." << std::endl;

    return 0;
}