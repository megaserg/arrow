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
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/util/macros.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace fs {

extern ARROW_EXPORT const char* kGCSDefaultRegion;

/// Options for the GCSFileSystem implementation.
struct ARROW_EXPORT GCSOptions {
  /// GCP region to connect to (default "us-central-1")
  std::string region = kGCSDefaultRegion;

  /// If non-empty, override region with a connect string such as "localhost:9000"
  // XXX perhaps instead take a URL like "http://localhost:9000"?
  std::string endpoint_override;
  /// GCS connection transport, default "https"
  std::string scheme = "https";

  /// TODO(megaserg): AWS credentials provider
  // std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;

  /// Configure with the default AWS credentials provider chain.
  // void ConfigureDefaultCredentials();

  bool Equals(const GCSOptions& other) const;

  /// \brief Initialize with default credentials provider chain
  ///
  /// This is recommended if you use the standard AWS environment variables
  /// and/or configuration file.
  static GCSOptions Defaults();

  static Result<GCSOptions> FromUri(const ::arrow::internal::Uri& uri,
                                    std::string* out_path = NULLPTR);
  static Result<GCSOptions> FromUri(const std::string& uri,
                                    std::string* out_path = NULLPTR);
};

/// GCS-backed FileSystem implementation.
///
/// Some implementation notes:
/// - buckets are special and the operations available on them may be limited
///   or more expensive than desired.
class ARROW_EXPORT GCSFileSystem : public FileSystem {
 public:
  ~GCSFileSystem() override;

  std::string type_name() const override { return "GCS"; }
  GCSOptions options() const;

  bool Equals(const FileSystem& other) const override;

  /// \cond FALSE
  using FileSystem::GetFileInfo;
  /// \endcond
  Result<FileInfo> GetFileInfo(const std::string& path) override;
  Result<std::vector<FileInfo>> GetFileInfo(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override {
    return Status::NotImplemented("CreateDir");
  }

  Status DeleteDir(const std::string& path) override {
    return Status::NotImplemented("DeleteDir");
  }
  Status DeleteDirContents(const std::string& path) override {
    return Status::NotImplemented("DeleteDirContents");
  }

  Status DeleteRootDirContents() override {
    return Status::NotImplemented("Cannot delete all GCS buckets");
  }

  Status DeleteFile(const std::string& path) override {
    return Status::NotImplemented("DeleteFile");
  }

  Status Move(const std::string& src, const std::string& dest) override {
    return Status::NotImplemented("Move");
  }

  Status CopyFile(const std::string& src, const std::string& dest) override {
    return Status::NotImplemented("CopyFile");
  }

  /// Create a sequential input stream for reading from a GCS object.
  ///
  /// NOTE: Reads from the stream will be synchronous and unbuffered.
  /// You way want to wrap the stream in a BufferedInputStream or use
  /// a custom readahead strategy to avoid idle waits.
  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;

  /// Create a random access file for reading from a GCS object.
  ///
  /// See OpenInputStream for performance notes.
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(const std::string& path) override {
    return Status::NotImplemented("OpenOutputStream");
  }

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(const std::string& path) override {
    return Status::NotImplemented("OpenAppendStream");
  }

  /// Create a GCSFileSystem instance from the given options.
  static Result<std::shared_ptr<GCSFileSystem>> Make(const GCSOptions& options);

 protected:
  explicit GCSFileSystem(const GCSOptions& options);

  class Impl;
  std::unique_ptr<Impl> impl_;
};

enum class GCSLogLevel : int8_t { Off, Fatal, Error, Warn, Info, Debug, Trace };

struct ARROW_EXPORT GCSGlobalOptions {
  GCSLogLevel log_level;
};

/// Initialize the GCS APIs.  It is required to call this function at least once
/// before using GCSFileSystem.
ARROW_EXPORT
Status InitializeGCS(const GCSGlobalOptions& options);

/// Ensure the GCS APIs are initialized, but only if not already done.
/// If necessary, this will call InitializeGCS() with some default options.
ARROW_EXPORT
Status EnsureGCSInitialized();

/// Shutdown the GCS APIs.
ARROW_EXPORT
Status FinalizeGCS();

}  // namespace fs
}  // namespace arrow
