#pragma once
#include <RaeptorContainers/auto.h>
#include <RaeptorContainers/dynamic_array.h>
#include <RaeptorContainers/rbtree.h>
#include <RaeptorParallel/Jobs/job.h>
#include <bits/pthreadtypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>

typedef struct RaeptorParallel_worker {
  /**
   * @brief Flag indicating whether the worker is running.
   *
   * Used to control the main loop of the worker thread.
   */
  bool running;

  /**
   * @brief Flag indicating whether the current job is detached.
   *
   * Used to manage the lifecycle of jobs and ensure proper cleanup.
   */
  bool current_job_detached;

  /**
   * @brief Thread object for the worker.
   *
   * Manages the execution of jobs in a separate thread.
   */
  pthread_t worker_thread;

  /**
   * @brief Mutex for synchronizing access to the job queue.
   *
   * Ensures thread-safe operations when adding or executing jobs.
   */
  pthread_mutex_t mtx;

  /**
   * @brief Map of job queues categorized by priority.
   *
   * Each priority level maps to a vector of job functions.
   */
  RaeptorContainers_rbtree(
      int, RaeptorContainers_darray(RaeptorParallel_job_t *)) jobs;
} RaeptorParallel_worker_t;

static inline void RaeptorParallel_worker_init(RaeptorParallel_worker_t *worker,
                                               pthread_t thread) {
  worker->running = false;
  worker->worker_thread = thread;
  pthread_mutex_init(&worker->mtx, NULL);
  RaeptorContainers_rbtree_init(&worker->jobs);
}

static inline RaeptorParallel_worker_t *
RaeptorParallel_worker_create(pthread_t thread) {
  RaeptorParallel_worker_t *worker =
      (RaeptorParallel_worker_t *)malloc(sizeof(RaeptorParallel_worker_t));
  RaeptorParallel_worker_init(worker, thread);
  return worker;
}

/**
 * @brief Main loop function for the worker thread.
 *
 * Continuously executes jobs while the worker is running.
 */
static inline void
RaeptorParallel_worker_run(RaeptorParallel_worker_t *worker) {
  worker->current_job_detached = false;
  while (worker->running) {
    RaeptorParallel_job_t *job = NULL;
    int priority;
    pthread_mutex_lock(&worker->mtx);
    if (!RaeptorContainers_rbtree_empty(&worker->jobs)) {
      time_t now = RaeptorParallel_time_ms();
      RaeptorContainers_rbtree_foreach(&worker->jobs, node, {
        if (!RaeptorContainers_darray_empty(&node->value)) {
          RaeptorParallel_job_t *next_job =
              RaeptorContainers_darray_front(&node->value);
          if (now - RaeptorParallel_job_get_last_execution_time(next_job) >=
              RaeptorParallel_job_get_interval(next_job)) {
            job = next_job;
            priority = node->key;
            RaeptorContainers_darray_remove(&node->value, 0);
            if (RaeptorContainers_darray_empty(&node->value)) {
              RaeptorContainers_darray_free(&node->value);
              RaeptorContainers_rbtree_remove(&worker->jobs, node->key);
            }
            break;
          }
        }
      });
    }
    pthread_mutex_unlock(&worker->mtx);
    if (job) {
      if (RaeptorParallel_job_get_repeat_count(job)) {
        RaeptorParallel_job_update_last_execution_time(job);
        auto node = RaeptorContainers_rbtree_search(&worker->jobs, priority);
        if (!node) {
          __typeof__(worker->jobs.root->value) new_array = {0};
          RaeptorContainers_rbtree_insert(&worker->jobs, priority, new_array);
          node = RaeptorContainers_rbtree_search(&worker->jobs, priority);
        }
        RaeptorContainers_darray_push(&node->value, job);
      }
      job->execute();
      if (worker->current_job_detached) {
        worker->current_job_detached = false;
        continue;
      }
      if (RaeptorParallel_job_get_repeat_count(job) == 0) {
        RaeptorParallel_job_detach(&job);
      } else if (RaeptorParallel_job_get_repeat_count(job) > 0)
        RaeptorParallel_job_set_repeat_count(
            job, RaeptorParallel_job_get_repeat_count(job) - 1);
    } else {
      if (worker->worker_thread == pthread_self() &&
          RaeptorContainers_rbtree_empty(&worker->jobs)) {
        worker->running = false;
        return;
      }
      struct timespec ts = {0, 10 * 1000 * 1000}; // 10 milliseconds
      nanosleep(&ts, NULL);
    }
  }
}

/**
 * @brief Wrapper function for the worker thread.
 *
 * This function is used as the entry point for the worker thread.
 */
static inline void *RaeptorParallel_worker_thread(void *arg) {
  RaeptorParallel_worker_run(arg);
  return NULL;
}

/**
 * @brief Start the worker thread.
 *
 * Initializes and begins execution of the worker thread.
 */
static inline void
RaeptorParallel_worker_start(RaeptorParallel_worker_t *worker) {
  worker->running = true;
  // Check if workerThread is not pthread_self() to avoid deadlock when calling
  // start from the worker thread itself
  if (worker->worker_thread && pthread_self() == worker->worker_thread) {
    RaeptorParallel_worker_run(worker);
    return;
  }
  pthread_create(&worker->worker_thread, NULL, RaeptorParallel_worker_thread,
                 worker);
}

/**
 * @brief Stop the worker thread.
 *
 * Signals the worker thread to terminate and waits for it to finish.
 */
static inline void
RaeptorParallel_worker_stop(RaeptorParallel_worker_t *worker) {
  worker->running = false;
  worker->current_job_detached = true;
  if (worker->worker_thread && pthread_self() != worker->worker_thread) {
    pthread_join(worker->worker_thread, NULL);
  }
}

/**
 * @brief Default constructor for Worker.
 */
static inline void
RaeptorParallel_worker_free(RaeptorParallel_worker_t *worker) {
  RaeptorParallel_worker_stop(worker);
  RaeptorContainers_rbtree_foreach(
      &worker->jobs, node, { RaeptorContainers_darray_free(&node->value); });
  RaeptorContainers_rbtree_free(&worker->jobs);
  pthread_mutex_destroy(&worker->mtx);
}

/**
 * @brief Check if the worker thread is running.
 *
 * @return true if the worker is running, false otherwise.
 */
static inline bool
RaeptorParallel_worker_is_running(RaeptorParallel_worker_t *worker) {
  return worker->running;
}

/**
 * @brief Check if a specific job is currently scheduled in the worker.
 *
 * @param job The job to check for in the scheduled jobs.
 * @return true if the job is scheduled, false otherwise.
 */
static inline bool
RaeptorParallel_worker_is_job_scheduled(RaeptorParallel_worker_t *worker,
                                        RaeptorParallel_job_t *job) {
  pthread_mutex_lock(&worker->mtx);
  bool found = false;
  RaeptorContainers_rbtree_foreach(&worker->jobs, node, {
    RaeptorContainers_darray_foreach(&node->value, i, val, {
      if (val == job) {
        found = true;
        break;
      }
    });
    if (found) {
      break;
    }
  });
  pthread_mutex_unlock(&worker->mtx);
  return found;
}

/**
 * @brief Add a job to the worker.
 *
 * @param job The job function to add.
 * @param priority The priority level of the job (default is NORMAL).
 *
 * @note Jobs with higher priority values are executed first.
 */
static inline void
RaeptorParallel_worker_add_job(RaeptorParallel_worker_t *worker,
                               RaeptorParallel_job_t *job, int priority) {
  pthread_mutex_lock(&worker->mtx);
  auto node = RaeptorContainers_rbtree_search(&worker->jobs, priority);
  if (!node) {
    __typeof__(worker->jobs.root->value) new_array = {0};
    RaeptorContainers_rbtree_insert(&worker->jobs, priority, new_array);
    node = RaeptorContainers_rbtree_search(&worker->jobs, priority);
  }
  RaeptorParallel_job_attach(job);
  RaeptorContainers_darray_push(&node->value, job);
  pthread_mutex_unlock(&worker->mtx);
}

/**
 * @brief Remove a specific job from the worker.
 *
 * @param job The job to remove from the scheduled jobs.
 *
 * @note This will remove all instances of the specified job from the worker.
 */
static inline void
RaeptorParallel_worker_remove_job(RaeptorParallel_worker_t *worker,
                                  RaeptorParallel_job_t *job) {
  pthread_mutex_lock(&worker->mtx);
  RaeptorContainers_rbtree_foreach(&worker->jobs, node, {
    int new_count = 0;
    for (size_t i = 0; i < node->value.count; i++) {
      if (node->value.items[i] != job) {
        node->value.items[new_count++] = node->value.items[i];
      } else {
        RaeptorParallel_job_detach(&job);
        worker->current_job_detached = true;
      }
    };
    node->value.count = new_count;
    if (RaeptorContainers_darray_empty(&node->value)) {
      RaeptorContainers_darray_free(&node->value);
      RaeptorContainers_rbtree_remove(&worker->jobs, node->key);
      break;
    }
  });
  pthread_mutex_unlock(&worker->mtx);
}

/**
 * @brief Clear all pending jobs in the worker.
 *
 * @note This removes all jobs without executing them.
 */
static inline void
RaeptorParallel_worker_clear_jobs(RaeptorParallel_worker_t *worker) {
  pthread_mutex_lock(&worker->mtx);
  RaeptorContainers_rbtree_foreach(&worker->jobs, node, {
    RaeptorContainers_darray_foreach(&node->value, i, val,
                                     { RaeptorParallel_job_detach(&val); });
    RaeptorContainers_darray_free(&node->value);
  });
  RaeptorContainers_rbtree_free(&worker->jobs);
  worker->jobs.root = NULL;
  pthread_mutex_unlock(&worker->mtx);
  RaeptorParallel_worker_stop(worker);
}
