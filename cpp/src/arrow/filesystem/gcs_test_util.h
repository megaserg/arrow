// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <utility>

// boost/process/detail/windows/handle_workaround.hpp doesn't work
// without BOOST_USE_WINDOWS_H with MinGW because MinGW doesn't
// provide __kernel_entry without winternl.h.
//
// See also:
// https://github.com/boostorg/process/blob/develop/include/boost/process/detail/windows/handle_workaround.hpp
#include <boost/process.hpp>

#include <gtest/gtest.h>

#include "arrow/filesystem/gcsfs.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace fs {

using ::arrow::internal::TemporaryDir;

namespace bp = boost::process;

// TODO: allocate an ephemeral port
static const char* kFakegcsExecutableName = "fake-gcs-server";
// static const char* kFakegcsAccessKey = "fakegcs";
// static const char* kFakegcsSecretKey = "fakegcspass";

// Environment variables to configure another GCS-compatible service
static const char* kEnvConnectString = "ARROW_TEST_GCS_CONNECT_STRING";
static const char* kEnvAccessKey = "ARROW_TEST_GCS_ACCESS_KEY";
static const char* kEnvSecretKey = "ARROW_TEST_GCS_SECRET_KEY";

static std::string GenerateConnectString(int port) {
  std::stringstream ss;
  ss << "127.0.0.1:" << port;
  return ss.str();
}

// A fake-gcs-server test server, managed as a child process

class FakegcsTestServer {
 public:
  Status Start();

  Status Stop();

  std::string connect_string() const { return connect_string_; }

//   std::string access_key() const { return access_key_; }

//   std::string secret_key() const { return secret_key_; }

 private:
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string connect_string_;
//   std::string access_key_ = kFakegcsAccessKey;
//   std::string secret_key_ = kFakegcsSecretKey;
  std::shared_ptr<::boost::process::child> server_process_;
};

Status FakegcsTestServer::Start() {
//   const char* connect_str = std::getenv(kEnvConnectString);
//   const char* access_key = std::getenv(kEnvAccessKey);
//   const char* secret_key = std::getenv(kEnvSecretKey);
//   if (connect_str && access_key && secret_key) {
//     // Use external instance
//     connect_string_ = connect_str;
//     access_key_ = access_key;
//     secret_key_ = secret_key;
//     return Status::OK();
//   }

  ARROW_ASSIGN_OR_RAISE(temp_dir_, TemporaryDir::Make("gcsfs-test-"));

  // Get a copy of the current environment.
  // (NOTE: using "auto" would return a native_environment that mutates
  //  the current environment)
  bp::environment env = boost::this_process::environment();
//   env["MINIO_ACCESS_KEY"] = kFakegcsAccessKey;
//   env["MINIO_SECRET_KEY"] = kFakegcsSecretKey;

  int port = GetListenPort();
  connect_string_ = GenerateConnectString(port);

  auto exe_path = bp::search_path(kFakegcsExecutableName);
  if (exe_path.empty()) {
    return Status::IOError("Failed to find fake-gcs-server executable ('", kFakegcsExecutableName,
                           "') in PATH");
  }

  try {
    server_process_ = std::make_shared<bp::child>(
        // env, exe_path, "-host", "127.0.0.1", "-port", std::to_string(port), "-filesystem-root",
        // temp_dir_->path().ToString(), "-scheme", "http");
        env, "/usr/bin/docker", "run", "-i", "--rm", "-p", std::to_string(port) + ":4443", "fsouza/fake-gcs-server:latest", "-scheme", "http");
        // env, "/home/sergey.serebryakov/.local/bin/gcloud-storage-emulator", "start", "--port", std::to_string(port));
  } catch (const std::exception& e) {
    return Status::IOError("Failed to launch fake-gcs-server: ", e.what());
  }
  return Status::OK();
}

Status FakegcsTestServer::Stop() {
  if (server_process_ && server_process_->valid()) {
    // Brutal shutdown
    server_process_->terminate();
    server_process_->wait();
  }
  return Status::OK();
}

// A global test "environment", to ensure that the GCS API is initialized before
// running unit tests.

class GCSEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    // Change this to increase logging during tests
    GCSGlobalOptions options;
    options.log_level = GCSLogLevel::Fatal;
    ASSERT_OK(InitializeGCS(options));
  }

  void TearDown() override { ASSERT_OK(FinalizeGCS()); }
};

::testing::Environment* gcs_env = ::testing::AddGlobalTestEnvironment(new GCSEnvironment);

}  // namespace fs
}  // namespace arrow
