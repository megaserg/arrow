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

#include <sstream>
#include <string>
#include <tuple>
#include <utility>

#include <google/cloud/status.h>
#include <google/cloud/status_or.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/print.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace fs {
namespace internal {

using ::google::cloud::StatusOr;
using ::google::cloud::StatusCode;

template<typename T>
inline bool IsNotFound(const StatusOr<T>& error) {
  return error.status().code() == StatusCode::kNotFound;
}

template<typename T>
inline bool IsAlreadyExists(const StatusOr<T>& error) {
  return error.status().code() == StatusCode::kAlreadyExists;
}

// TODO qualify error messages with a prefix indicating context
// (e.g. "When completing multipart upload to bucket 'xxx', key 'xxx': ...")
Status ErrorToStatus(const std::string& prefix) {
  return Status::IOError(prefix);
}

// TODO qualify error messages with a prefix indicating context
// (e.g. "When completing multipart upload to bucket 'xxx', key 'xxx': ...")
template <typename T>
Status ErrorToStatus(const std::string& prefix,
                     const StatusOr<T>& error) {
  // XXX Handle fine-grained error types
  return Status::IOError(prefix, "GCS Error [code ",
                         static_cast<int>(error.status().code()),
                         "]: ", error.status().message());
}

template <typename T, typename... Args>
Status ErrorToStatus(const std::tuple<Args&...>& prefix,
                     const StatusOr<T>& error) {
  std::stringstream ss;
  ::arrow::internal::PrintTuple(&ss, prefix);
  return ErrorToStatus(ss.str(), error);
}

template <typename... Args>
Status ErrorToStatus(const std::tuple<Args&...>& prefix) {
  std::stringstream ss;
  ::arrow::internal::PrintTuple(&ss, prefix);
  return ErrorToStatus(ss.str());
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow