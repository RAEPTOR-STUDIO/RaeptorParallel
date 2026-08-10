#pragma once
#include <chrono>
#include <ctime>
#include <limits>
#include <memory>

namespace RaeptorParallel {

/**
 * @brief Enum class for job priority levels.
 *
 * Used to specify the priority of jobs added to the worker.
 */
enum class JobPriority : int {
  /** Lowest priority level for jobs. */
  LOWEST = std::numeric_limits<int>::min(),
  /** Normal priority level for jobs. */
  NORMAL = 0,
  /** Highest priority level for jobs. */
  HIGHEST = std::numeric_limits<int>::max()
};

class Job {
private:
  int repeatCount =
      1; // Number of times to repeat the job, default is 1 (execute once)
  time_t delayMs = 0; // Delay in milliseconds before executing the job, default
                      // is 0 (no delay)
  std::chrono::time_point<std::chrono::steady_clock>
      lastExecutionTime; // Timestamp of
                         // the last
                         // execution
public:
  Job() { lastExecutionTime = std::chrono::steady_clock::now(); };
  virtual ~Job() = default;
  virtual void execute() {};
  void setRepeatCount(int count) { repeatCount = count; }
  void setDelay(int ms) { delayMs = ms; }
  void updateLastExecutionTime() {
    lastExecutionTime = std::chrono::steady_clock::now();
  }
  time_t getRepeatCount() const { return repeatCount; }
  time_t getDelay() const { return delayMs; }
  std::chrono::time_point<std::chrono::steady_clock>
  getLastExecutionTime() const {
    return lastExecutionTime;
  }
};

/**
 * @brief Factory function to create a job of a specific type.
 *
 * This function creates a shared pointer to a job of the specified type,
 * forwarding any constructor arguments to the job's constructor.
 *
 * @tparam JobType The type of job to create, must be derived from Job.
 * @tparam Args Variadic template parameters for the job's constructor
 * arguments.
 * @param args Arguments to forward to the job's constructor.
 * @return std::shared_ptr<JobType> A shared pointer to the created job.
 *
 * @note This function uses SFINAE to ensure that JobType is derived from Job.
 */
template <typename JobType, typename... Args,
          typename = std::enable_if_t<std::is_base_of<Job, JobType>::value>>
std::shared_ptr<JobType> CreateJob(Args &&...args) {
  return std::make_shared<JobType>(std::forward<Args>(args)...);
}

} // namespace RaeptorParallel
