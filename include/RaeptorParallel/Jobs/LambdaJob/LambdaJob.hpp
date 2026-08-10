#pragma once

#include <RaeptorParallel/Jobs/Job.hpp>
#include <functional>

namespace RaeptorParallel {

class LambdaJob : public Job {
private:
  std::function<void()> func;

public:
  LambdaJob(std::function<void()> func) : Job(), func(std::move(func)) {};
  ~LambdaJob() override = default;
  void execute() override { func(); }
};
} // namespace RaeptorParallel
