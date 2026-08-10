#pragma once
#include <RaeptorParallel/Jobs/Job.hpp>
#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace RaeptorParallel {

class Worker {
private:
  // ============================================================================
  //                               PRIVATE ATTRIBUTES
  // ============================================================================

  /**
   * @brief Flag indicating whether the worker is running.
   *
   * Used to control the main loop of the worker thread.
   */
  bool running = false;

  /**
   * @brief Thread object for the worker.
   *
   * Manages the execution of jobs in a separate thread.
   */
  std::thread workerThread;

  /**
   * @brief Mutex for synchronizing access to the job queue.
   *
   * Ensures thread-safe operations when adding or executing jobs.
   */
  std::mutex mtx;

  /**
   * @brief Map of job queues categorized by priority.
   *
   * Each priority level maps to a vector of job functions.
   */
  std::map<int, std::vector<std::shared_ptr<Job>>, std::greater<int>> jobs;

  /**
   * @brief Main loop function for the worker thread.
   *
   * Continuously executes jobs while the worker is running.
   */
  void run() {
    while (running) {
      std::shared_ptr<Job> job;
      int priority;
      {
        std::lock_guard<std::mutex> lock(mtx);
        if (!jobs.empty()) {
          std::chrono::time_point<std::chrono::steady_clock> now =
              std::chrono::steady_clock::now();
          for (auto it = jobs.begin(); it != jobs.end(); ++it) {
            if (!it->second.empty()) {
              Job *nextJob = it->second.front().get();
              if (now - nextJob->getLastExecutionTime() >=
                  std::chrono::milliseconds(nextJob->getDelay())) {
                job = std::move(it->second.front());
                priority = it->first;
                it->second.erase(it->second.begin());
                if (it->second.empty()) {
                  jobs.erase(it);
                }
                break;
              }
            }
          }
        }
      }
      if (job) {
        job->execute();
        if (job->getRepeatCount() > 0)
          job->setRepeatCount(job->getRepeatCount() - 1);
        if (job->getRepeatCount()) {
          job->updateLastExecutionTime();
          addJob(std::move(job), priority);
        }
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  }

public:
  // ============================================================================
  //                               PUBLIC METHODS
  // ============================================================================

  /**
   * @brief Default constructor for Worker.
   */
  ~Worker() { stop(); }

  /**
   * @brief Start the worker thread.
   *
   * Initializes and begins execution of the worker thread.
   */
  void start() {
    running = true;
    workerThread = std::thread(&Worker::run, this);
  }

  /**
   * @brief Stop the worker thread.
   *
   * Signals the worker thread to terminate and waits for it to finish.
   */
  void stop() {
    running = false;
    if (workerThread.joinable()) {
      workerThread.join();
    }
  }

  /**
   * @brief Check if the worker thread is running.
   *
   * @return true if the worker is running, false otherwise.
   */
  bool isRunning() const { return running; }

  /**
   * @brief Check if a specific job is currently scheduled in the worker.
   *
   * @param job The job to check for in the scheduled jobs.
   * @return true if the job is scheduled, false otherwise.
   */
  bool isJobScheduled(const std::shared_ptr<Job> &job) {
    std::lock_guard<std::mutex> lock(mtx);
    for (const auto &pair : jobs) {
      const auto &jobList = pair.second;
      if (std::find(jobList.begin(), jobList.end(), job) != jobList.end()) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Add a job to the worker.
   *
   * @param job The job function to add.
   * @param priority The priority level of the job (default is NORMAL).
   *
   * @note Jobs with higher priority values are executed first.
   */
  void addJob(std::shared_ptr<Job> job, int priority) {
    std::lock_guard<std::mutex> lock(mtx);
    jobs[priority].push_back(job);
    if (!running) {
      start();
    }
  }

  /**
   * @brief Overload of addJob to accept JobPriority enum.
   *
   * @param job The job function to add.
   * @param priority The priority level of the job as JobPriority enum.
   *
   * @note Jobs with higher priority values are executed first.
   */
  void addJob(std::shared_ptr<Job> job,
              JobPriority priority = JobPriority::NORMAL) {
    addJob(job, static_cast<int>(priority));
  }

  /**
   * @brief Remove a specific job from the worker.
   *
   * @param job The job to remove from the scheduled jobs.
   *
   * @note This will remove all instances of the specified job from the worker.
   */
  void removeJob(const std::shared_ptr<Job> &job) {
    std::lock_guard<std::mutex> lock(mtx);
    for (auto it = jobs.begin(); it != jobs.end(); ++it) {
      auto &jobList = it->second;
      jobList.erase(std::remove_if(jobList.begin(), jobList.end(),
                                   [&job](const std::shared_ptr<Job> &j) {
                                     return j == job;
                                   }),
                    jobList.end());
      if (jobList.empty()) {
        jobs.erase(it);
        break;
      }
    }
  }

  /**
   * @brief Clear all pending jobs in the worker.
   *
   * @note This removes all jobs without executing them.
   */
  void clearJobs() {
    std::lock_guard<std::mutex> lock(mtx);
    jobs.clear();
    stop();
  }
};

} // namespace RaeptorParallel
