#pragma once
#include <limits.h>
#include <stdlib.h>
#include <time.h>

#include <time.h>

static inline int64_t time_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/**
 * @brief Enum for job priority levels.
 *
 * Used to specify the priority of jobs added to the worker.
 */
typedef enum job_priority {
  /** Lowest priority level for jobs. */
  LOWEST = INT_MIN,
  /** Normal priority level for jobs. */
  NORMAL = 0,
  /** Highest priority level for jobs. */
  HIGHEST = INT_MAX
} job_priority_t;

typedef void (*job_function_t)(void); // Function pointer type for job execution

typedef struct job {
  int repeatCount;   // Number of times to repeat the job, default is 1 (execute
                     // once)
  time_t intervalMs; // Interval in milliseconds before executing the job,
                     // default is 0 (no interval)
  time_t lastExecutionTimeMs; // Timestamp of the last execution in milliseconds
  int owners;                 // Number of owners of the job
  job_function_t execute; // Function pointer to the job's execution function
} job_t;

static inline void job_init(job_t *job) {
  job->repeatCount = 0;
  job->intervalMs = 0;
  job->lastExecutionTimeMs = 0;
  job->owners = 0;
  job->execute = NULL;
}

static inline job_t *job_create() {
  job_t *job = malloc(sizeof(job_t));
  job_init(job);
  return job;
}

static inline void job_free(job_t **job) {
  free(*job);
  job = NULL;
}

static inline job_t *job_attach(job_t *job) {
  job->owners++;
  return job;
}

static inline void job_detach(job_t **job) {
  if ((*job)->owners > 0) {
    (*job)->owners--;
    if ((*job)->owners == 0) {
      job_free(job);
    }
  }
}

static inline void job_set_repeat_count(job_t *job, int count) {
  job->repeatCount = count;
}
static inline void job_set_interval(job_t *job, time_t ms) {
  job->intervalMs = ms;
}
static inline void job_update_last_execution_time(job_t *job) {
  job->lastExecutionTimeMs = time_ms();
}
static inline void job_set_execute(job_t *job, job_function_t execute) {
  job->execute = execute;
}

static inline int job_get_repeat_count(job_t *job) { return job->repeatCount; }
static inline time_t job_get_interval(job_t *job) { return job->intervalMs; }
static inline time_t job_get_last_execution_time(job_t *job) {
  return job->lastExecutionTimeMs;
}
static inline void job_execute(job_t *job) {
  if (job->execute != NULL)
    job->execute();
}
