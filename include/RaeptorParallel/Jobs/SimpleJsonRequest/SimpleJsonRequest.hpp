#pragma once

#include <RaeptorParallel/Jobs/Job.hpp>
#include <curl/curl.h>
#include <functional>
#include <iostream>
#include <string>

namespace RaeptorParallel {

static size_t writeCallback(void *contents, size_t size, size_t nmemb,
                            void *userp) {
  size_t totalSize = size * nmemb;
  std::vector<char> *buffer = static_cast<std::vector<char> *>(userp);
  char *data = static_cast<char *>(contents);
  buffer->insert(buffer->end(), data, data + totalSize);
  return totalSize;
}

class SimpleJsonRequestJob : public Job {
private:
  std::string url;
  std::function<void(const std::string &)> callback;
  std::string performHttpRequest(const std::string &url) {
    CURL *curl = curl_easy_init();
    if (curl) {
      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      std::vector<char> buffer;
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);
      CURLcode res = curl_easy_perform(curl);
      curl_easy_cleanup(curl);
      if (res != CURLE_OK) {
        std::cerr << "CURL error: " << curl_easy_strerror(res) << std::endl;
        return "";
      }
      return std::string(buffer.data(), buffer.size());
    }
    return "";
  }

public:
  SimpleJsonRequestJob(std::string url,
                       std::function<void(const std::string &)> callback)
      : Job(), url(std::move(url)), callback(std::move(callback)) {};
  ~SimpleJsonRequestJob() override = default;
  void execute() override {
    // Perform the HTTP request and get the response as a string
    std::string response = performHttpRequest(url);
    // Call the callback with the response
    callback(response);
  };
};
} // namespace RaeptorParallel
