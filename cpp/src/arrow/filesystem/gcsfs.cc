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

#include "arrow/filesystem/gcsfs.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <utility>

#include <google/cloud/storage/client.h>
#include <google/cloud/storage/object_stream.h>
#include <google/cloud/storage/oauth2/google_credentials.h>

#include "arrow/buffer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/gcs_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {

using internal::Uri;

namespace fs {

namespace gcs = google::cloud::storage;
using ::google::cloud::StatusOr;

using ::arrow::fs::internal::ErrorToStatus;
using ::arrow::fs::internal::IsAlreadyExists;
using ::arrow::fs::internal::IsNotFound;

const char* kGCSDefaultRegion = "us-central-1";

static const char kSep = '/';
static const std::string kSepString = "/";

namespace {

std::mutex gcs_init_lock;
// Aws::SDKOptions aws_options;
std::atomic<bool> gcs_initialized(false);

// Status DoInitializeGCS(const GCSGlobalOptions& options) {
  // Aws::Utils::Logging::LogLevel aws_log_level;

// #define LOG_LEVEL_CASE(level_name)
//   case GCSLogLevel::level_name:
//     aws_log_level = Aws::Utils::Logging::LogLevel::level_name;
//     break;

//   switch (options.log_level) {
//     LOG_LEVEL_CASE(Fatal)
//     LOG_LEVEL_CASE(Error)
//     LOG_LEVEL_CASE(Warn)
//     LOG_LEVEL_CASE(Info)
//     LOG_LEVEL_CASE(Debug)
//     LOG_LEVEL_CASE(Trace)
//     default:
//       aws_log_level = Aws::Utils::Logging::LogLevel::Off;
//   }

// #undef LOG_LEVEL_CASE

  // aws_options.loggingOptions.logLevel = aws_log_level;
  // // By default the AWS SDK logs to files, log to console instead
  // aws_options.loggingOptions.logger_create_fn = [] {
  //   return std::make_shared<Aws::Utils::Logging::ConsoleLogSystem>(
  //       aws_options.loggingOptions.logLevel);
  // };
  // Aws::InitAPI(aws_options);
// }

}  // namespace

Status InitializeGCS(const GCSGlobalOptions& options) {
  std::lock_guard<std::mutex> lock(gcs_init_lock);
  gcs_initialized.store(true);
  return Status::OK();
}

Status FinalizeGCS() {
  std::lock_guard<std::mutex> lock(gcs_init_lock);
  gcs_initialized.store(false);
  return Status::OK();
}

Status EnsureGCSInitialized() {
  std::lock_guard<std::mutex> lock(gcs_init_lock);
  if (!gcs_initialized.load()) {
    // GCSGlobalOptions options{GCSLogLevel::Fatal};
    gcs_initialized.store(true);
    return Status::OK();
  }
  return Status::OK();
}

// void GCSOptions::ConfigureDefaultCredentials() {
//   // credentials_provider =
//   //     std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
// }

GCSOptions GCSOptions::Defaults() {
  GCSOptions options;
  // options.ConfigureDefaultCredentials();
  return options;
}

Result<GCSOptions> GCSOptions::FromUri(const Uri& uri, std::string* out_path) {
  GCSOptions options;

  const auto bucket = uri.host();
  auto path = uri.path();
  if (bucket.empty()) {
    if (!path.empty()) {
      return Status::Invalid("Missing bucket name in GCS URI");
    }
  } else {
    if (path.empty()) {
      path = bucket;
    } else {
      if (path[0] != '/') {
        return Status::Invalid("GCS URI should be absolute, not relative");
      }
      path = bucket + path;
    }
  }
  if (out_path != nullptr) {
    *out_path = std::string(internal::RemoveTrailingSlash(path));
  }

  std::unordered_map<std::string, std::string> options_map;
  ARROW_ASSIGN_OR_RAISE(const auto options_items, uri.query_items());
  for (const auto& kv : options_items) {
    options_map.emplace(kv.first, kv.second);
  }

  // options.ConfigureDefaultCredentials();

  auto it = options_map.find("region");
  if (it != options_map.end()) {
    options.region = it->second;
  }
  it = options_map.find("scheme");
  if (it != options_map.end()) {
    options.scheme = it->second;
  }
  it = options_map.find("endpoint_override");
  if (it != options_map.end()) {
    options.endpoint_override = it->second;
  }

  return options;
}

Result<GCSOptions> GCSOptions::FromUri(const std::string& uri_string, std::string* out_path) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, out_path);
}

bool GCSOptions::Equals(const GCSOptions& other) const {
  return (region == other.region && endpoint_override == other.endpoint_override &&
          scheme == other.scheme);
}

namespace {

Status CheckGCSInitialized() {
  if (!gcs_initialized.load()) {
    return Status::Invalid(
        "GCS subsystem not initialized; please call InitializeGCS() "
        "before carrying out any GCS-related operation");
  }
  return Status::OK();
}

// XXX Sanitize paths by removing leading slash?

struct GCSPath {
  std::string full_path;
  std::string bucket;
  std::string key;
  std::vector<std::string> key_parts;

  static Status FromString(const std::string& s, GCSPath* out) {
    const auto src = internal::RemoveTrailingSlash(s);
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      *out = {std::string(src), std::string(src), "", {}};
      return Status::OK();
    }
    out->full_path = std::string(src);
    out->bucket = std::string(src.substr(0, first_sep));
    out->key = std::string(src.substr(first_sep + 1));
    out->key_parts = internal::SplitAbstractPath(out->key);
    return Validate(out);
  }

  static Status Validate(GCSPath* path) {
    auto result = internal::ValidateAbstractPathParts(path->key_parts);
    if (!result.ok()) {
      return Status::Invalid(result.message(), " in path ", path->full_path);
    } else {
      return result;
    }
  }

  GCSPath parent() const {
    DCHECK(!key_parts.empty());
    auto parent = GCSPath{"", bucket, "", key_parts};
    parent.key_parts.pop_back();
    parent.key = internal::JoinAbstractPath(parent.key_parts);
    parent.full_path = parent.bucket + kSep + parent.key;
    return parent;
  }

  bool has_parent() const { return !key.empty(); }

  bool empty() const { return bucket.empty() && key.empty(); }

  bool operator==(const GCSPath& other) const {
    return bucket == other.bucket && key == other.key;
  }
};

// XXX return in OutcomeToStatus instead?
// Status PathNotFound(const GCSPath& path) {
//   return Status::IOError("Path does not exist '", path.full_path, "'");
// }

Status PathNotFound(const std::string& bucket, const std::string& key) {
  return Status::IOError("Path does not exist '", bucket, kSep, key, "'");
}

Status NotAFile(const GCSPath& path) {
  return Status::IOError("Not a regular file: '", path.full_path, "'");
}

Status ValidateFilePath(const GCSPath& path) {
  if (path.bucket.empty() || path.key.empty()) {
    return NotAFile(path);
  }
  return Status::OK();
}

/*
  Returns gcs::ObjectReadStream stream.
  Use like this:
    std::string line;
    while (std::getline(stream, line, '\n')) {
      std::cout << line << "\n";
    }
*/
gcs::ObjectReadStream GetObjectRange(gcs::Client* client,
                                     const GCSPath& path,
                                     int64_t start,
                                     int64_t length,
                                     void* out) {
  return client->ReadObject(
    path.bucket,
    path.key,
    gcs::ReadRange(start, start+length)
  );
}

// A RandomAccessFile that reads from a GCS object
class ObjectInputFile : public io::RandomAccessFile {
 public:
  ObjectInputFile(gcs::Client* client, const GCSPath& path)
      : client_(client), path_(path) {}

  Status Init() {
    // Issue a HEAD Object to get the content-length and ensure any
    // errors (e.g. file not found) don't wait until the first Read() call.

    StatusOr<gcs::ObjectMetadata> object_metadata = client_->GetObjectMetadata(path_.bucket, path_.key);
    if (!object_metadata) {
      return ErrorToStatus("When reading information for key '" + path_.key +
          "' in bucket '" + path_.bucket + "': " + object_metadata.status().message());
    }

    // GCSModel::HeadObjectRequest req;
    // req.SetBucket(ToAwsString(path_.bucket));
    // req.SetKey(ToAwsString(path_.key));

    // auto outcome = client_->HeadObject(req);
    // if (!outcome.IsSuccess()) {
    //   if (IsNotFound(outcome.GetError())) {
    //     return PathNotFound(path_);
    //   } else {
    //     return ErrorToStatus(
    //         std::forward_as_tuple("When reading information for key '", path_.key,
    //                               "' in bucket '", path_.bucket, "': "),
    //         outcome.GetError());
    //   }
    // }

    content_length_ = object_metadata->size();
    DCHECK_GE(content_length_, 0);
    return Status::OK();
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return Status::OK();
  }

  Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return Status::IOError("Cannot ", action, " past end of file");
    }
    return Status::OK();
  }

  // RandomAccessFile APIs

  Status Close() override {
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, content_length_ - position);
    if (nbytes == 0) {
      return 0;
    }

    // Read the desired range of bytes
    gcs::ObjectReadStream stream = GetObjectRange(client_, path_, position, nbytes, out);

    if (!stream.status().ok()) {
      return Status::IOError("Read object failed on: " + path_.full_path + " (" + stream.status().message() + ")");
    }

    stream.read(static_cast<char*>(out), nbytes);
    int64_t length_returned = stream.gcount();

    if (!stream) {
      return Status::IOError("Read object fail on: " + path_.full_path);
    }

    stream.Close();

    return Result<int64_t>(length_returned);
    ////  auto reader = client->ReadObject(bucket_name, "quickstart.txt");
    // std::string contents{std::istreambuf_iterator<char>{result}, {}};
    ////  std::cout << contents << "\n";
    // out = (void*)contents.c_str();

    // auto& stream = result.GetBody();
    // stream.ignore(nbytes);
    // NOTE: the stream is a stringstream by default, there is no actual error
    // to check for.  However, stream.fail() may return true if EOF is reached.
    // return contents.length();
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(nbytes));
    if (nbytes > 0) {
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            ReadAt(position, nbytes, buf->mutable_data()));
      DCHECK_LE(bytes_read, nbytes);
      RETURN_NOT_OK(buf->Resize(bytes_read));
    }
    return std::move(buf);
  }

  Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

 protected:
  gcs::Client* client_;
  GCSPath path_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
};


// This function assumes info->path() is already set
void FileObjectToInfo(const gcs::ObjectMetadata& obj, FileInfo* info) {
  info->set_type(FileType::File);
  info->set_size(static_cast<int64_t>(obj.size()));
  info->set_mtime(obj.updated());
}

}  // namespace

class GCSFileSystem::Impl {
 public:
  GCSOptions options_;
  std::unique_ptr<gcs::Client> client_;

  const int32_t kListObjectsMaxKeys = 1000;
  // At most 1000 keys per multiple-delete request
  const int32_t kMultipleDeleteMaxKeys = 1000;
  // Limit recursing depth, since a recursion bomb can be created
  const int32_t kMaxNestingDepth = 100;

  explicit Impl(GCSOptions options) : options_(std::move(options)) {}

  Status Init() {

    // gcs::ChannelOptions channel_options;
#ifdef __linux__
    // const std::string cert_file = "/etc/ssl/certs/ca-certificates.crt";
    // if (!cert_file.empty()) {
    //   channel_options.set_ssl_root_path(cert_file);
    // }
#endif

    // auto creds = gcs::oauth2::GoogleDefaultCredentials(channel_options);
    // if (!creds) {
    //   return Status::IOError("Failed to find GCS credentials");
    // }

    // // Status was OK, so create a Client with the given Credentials.
    // auto client_options = gcs::ClientOptions(*creds);
    // client_options.set_endpoint(options_.scheme + "://" + options_.endpoint_override);

    auto client_options = gcs::ClientOptions::CreateDefaultClientOptions();

    if (!options_.endpoint_override.empty()) {
      client_options->set_endpoint(options_.scheme + "://" + options_.endpoint_override);
      auto creds = gcs::oauth2::CreateAnonymousCredentials();
      client_options->set_credentials(creds);
    }

    client_.reset(new gcs::Client(*client_options));

    // TODO(megaserg): if linux?

    // try {
    //   auto creds = gcs::oauth2::GoogleDefaultCredentials(channel_options);
    //   if (!creds) {
    //     return Status::IOError("Failed to initialize GCS credentials: " + creds.status().message());
    //   }

    //   gcs::ClientOptions client_options(*creds, channel_options);
    //   auto client = gcs::Client(client_options, gcs::StrictIdempotencyPolicy());
    //   client_ = google::cloud::StatusOr<gcs::Client>(client);
    //   if (!client_) {
    //     return Status::IOError("Failed to initialize GCS client: " + client_.status().message());
    //   }
    // } catch (const std::exception& e) {
    //   return Status::IOError("Failed to initialize GCS: " + std::string(e.what()));
    // }

    return Status::OK();
  }

  GCSOptions options() const { return options_; }

  // On Minio, an empty "directory" doesn't satisfy the same API requests as
  // a non-empty "directory".  This is a Minio-specific quirk, but we need
  // to handle it for unit testing.

  Status ls(const std::string& bucket, const std::string& key, int* count, const int max_count) {
    // gcs::internal::PaginationRange<gcs::ObjectOrPrefix, gcs::internal::ListObjectsRequest, gcs::internal::ListObjectsResponse>
    auto objects_reader = client_->ListObjectsAndPrefixes(bucket, gcs::Prefix(internal::EnsureTrailingSlash(key)), gcs::Delimiter(kSepString));

    std::cout << "LISTING " << bucket << " / " << key << std::endl;
    for (const auto& object_metadata : objects_reader) {
      if (!object_metadata.ok()) {
        return Status::IOError("List objects failed on: " + key + " (" + object_metadata.status().message() + ")");
      }
      auto result = *std::move(object_metadata);
      if (absl::holds_alternative<gcs::ObjectMetadata>(result)) {
        std::cout << "FOUND object " << absl::get<gcs::ObjectMetadata>(result).name() << std::endl;
      } else if (absl::holds_alternative<std::string>(result)) {
        std::cout << "FOUND prefix " << absl::get<std::string>(result) << std::endl;
      }

      (*count)++;
      if (*count >= max_count) {
        break;
      }
    }
    return Status::OK();
  }

  Status IsEmptyDirectory(const std::string& bucket, const std::string& key, bool* out) {
    int count = 0;
    RETURN_NOT_OK(ls(bucket, key, &count, 2));
    *out = (count == 1 || (count == 0 && key == ""));
    return Status::OK();

    // auto objects_reader = client_->ListObjectsAndPrefixes(bucket, gcs::Prefix(key + kSepString), gcs::Delimiter(kSepString));
    // for (auto&& object_metadata : objects_reader) {
    //   if (!object_metadata) {
    //     return Status::IOError("List bucket objects failed on: " + key + " (" + object_metadata.status().message() + ")");
    //   }
    //   *out = false;
    //   return Status::OK();
    // }

    // *out = true;
    // return Status::OK();

    // GCSModel::HeadObjectRequest req;
    // req.SetBucket(bucket);
    // req.SetKey(key + kSep);


    // auto outcome = client_->HeadObject(req);
    // if (outcome.IsSuccess()) {
    //   *out = true;
    //   return Status::OK();
    // }
    // if (IsNotFound(outcome.GetError())) {
    //   *out = false;
    //   return Status::OK();
    // }
    // return ErrorToStatus(std::forward_as_tuple("When reading information for key '", key,
    //                                            "' in bucket '", bucket, "': "),
    //                      outcome.GetError());
  }

  Status IsEmptyDirectory(const GCSPath& path, bool* out) {
    return IsEmptyDirectory(path.bucket, path.key, out);
  }

  Status IsNonEmptyDirectory(const GCSPath& path, bool* out) {
    int count = 0;
    RETURN_NOT_OK(ls(path.bucket, path.key, &count, 2));
    *out = count == 2;
    return Status::OK();

    // gcs::ListObjectsReader objects_reader = client_->ListObjects(path.bucket, gcs::Prefix(path.key + kSepString), gcs::Delimiter(kSepString));
    // for (auto&& object_metadata : objects_reader) {
    //   if (!object_metadata) {
    //     return Status::IOError("List bucket objects failed on: " + path.key + " (" + object_metadata.status().message() + ")");
    //   }
    //   *out = true;
    //   return Status::OK();
    // }

    // *out = false;
    // return Status::OK();

    // return ErrorToStatus(
    //     std::forward_as_tuple("When listing objects under key '", path.key,
    //                           "' in bucket '", path.bucket, "': "),
    //     outcome.GetError());

    // GCSModel::ListObjectsV2Request req;
    // req.SetBucket(ToAwsString(path.bucket));
    // req.SetPrefix(ToAwsString(path.key) + kSep);
    // req.SetDelimiter(Aws::String() + kSep);
    // req.SetMaxKeys(1);
    // auto outcome = client_->ListObjects(req);
    // if (outcome.IsSuccess()) {
    //   *out = outcome.GetResult().GetKeyCount() > 0;
    //   return Status::OK();
    // }
    // if (IsNotFound(outcome.GetError())) {
    //   *out = false;
    //   return Status::OK();
    // }
    // return ErrorToStatus(
    //     std::forward_as_tuple("When listing objects under key '", path.key,
    //                           "' in bucket '", path.bucket, "': "),
    //     outcome.GetError());
  }

  // List objects under a given prefix, issuing continuation requests if necessary
  template <typename ResultCallable, typename ErrorCallable>
  Status ListObjectsV2(const std::string& bucket, const std::string& prefix,
                       ResultCallable&& result_callable, ErrorCallable&& error_callable) {

    for (auto&& object_metadata : client_->ListObjects(bucket, gcs::Prefix(prefix))) {
      if (!object_metadata) {
        return error_callable(object_metadata.status().message());
      }
      RETURN_NOT_OK(result_callable(object_metadata.value()));
    }
    return Status::OK();
  }

  // Recursive workhorse for GetTargetStats(FileSelector...)
  Status Walk(const FileSelector& select, const std::string& bucket,
              const std::string& key, std::vector<FileInfo>* out) {

    // Check that the bucket exists
    StatusOr<gcs::BucketMetadata> bucket_metadata = client_->GetBucketMetadata(bucket);
    if (!bucket_metadata) {
      if (IsNotFound(bucket_metadata)) {
        if (select.allow_not_found) {
          return Status::OK();
        } else {
          return Status::IOError("Bucket '", bucket, "' does not exist: ", bucket_metadata.status().message());
        }
      } else {
        return ErrorToStatus(std::forward_as_tuple(
            "When getting information for bucket '", bucket, "': "), bucket_metadata);
      }
    }

    int32_t nesting_depth = 0;
    return Walk(select, bucket, key, nesting_depth, out);
  }

  Status Walk(const FileSelector& select, const std::string& bucket,
              const std::string& key, int32_t nesting_depth, std::vector<FileInfo>* out) {
    if (nesting_depth >= kMaxNestingDepth) {
      return Status::IOError("GCS filesystem tree exceeds maximum nesting depth (",
                             kMaxNestingDepth, ")");
    }

    bool is_empty = true;
    std::vector<std::string> child_keys;

    auto handle_results = [&](const gcs::ObjectMetadata& result_metadata) -> Status {
      // Walk "files"

      is_empty = false;
      FileInfo info;
      const auto child_key = internal::RemoveTrailingSlash(result_metadata.name());
      if (child_key == util::string_view(key)) {
        // Amazon can return the "directory" key itself as part of the results, skip
        return Status::OK();
      }
      std::stringstream child_path;
      child_path << bucket << kSep << child_key;
      info.set_path(child_path.str());
      FileObjectToInfo(result_metadata, &info);
      out->push_back(std::move(info));

//      // Walk "directories"
//      for (const auto& prefix : result.GetCommonPrefixes()) {
//        is_empty = false;
//        const auto child_key =
//            internal::RemoveTrailingSlash(FromAwsString(prefix.GetPrefix()));
//        std::stringstream ss;
//        ss << bucket << kSep << child_key;
//        FileInfo info;
//        info.set_path(ss.str());
//        info.set_type(FileType::Directory);
//        out->push_back(std::move(info));
//        if (select.recursive) {
//          child_keys.emplace_back(child_key);
//        }
//      }
      return Status::OK();
    };

    auto handle_error = [&](const std::string& error) -> Status {
      return ErrorToStatus(std::forward_as_tuple("When listing objects under key '", key,
                                                 "' in bucket '", bucket, "': ", error));
    };

    RETURN_NOT_OK(
        ListObjectsV2(bucket, key, std::move(handle_results), std::move(handle_error)));

    // Recurse
    if (select.recursive && nesting_depth < select.max_recursion) {
      for (const auto& child_key : child_keys) {
        RETURN_NOT_OK(Walk(select, bucket, child_key, nesting_depth + 1, out));
      }
    }

    // If no contents were found, perhaps it's an empty "directory",
    // or perhaps it's a nonexistent entry.  Check.
    if (is_empty && !select.allow_not_found) {
      RETURN_NOT_OK(IsEmptyDirectory(bucket, key, &is_empty));
      if (!is_empty) {
        return PathNotFound(bucket, key);
      }
    }
    return Status::OK();
  }

  Status ListBuckets(std::vector<std::string>* out) {
    out->clear();
    gcs::ListBucketsReader bucket_list = client_->ListBuckets();
    for (auto&& bucket_metadata : bucket_list) {
      if (!bucket_metadata) {
        return ErrorToStatus(std::forward_as_tuple("When listing buckets: "), bucket_metadata);
      }
      out->emplace_back(bucket_metadata.value().name());
    }
    return Status::OK();
  }
};

GCSFileSystem::GCSFileSystem(const GCSOptions& options) : impl_(new Impl{options}) {}

GCSFileSystem::~GCSFileSystem() {}

Result<std::shared_ptr<GCSFileSystem>> GCSFileSystem::Make(const GCSOptions& options) {
  RETURN_NOT_OK(CheckGCSInitialized());

  std::shared_ptr<GCSFileSystem> ptr(new GCSFileSystem(options));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

bool GCSFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& gcsfs = ::arrow::internal::checked_cast<const GCSFileSystem&>(other);
  return options().Equals(gcsfs.options());
}

GCSOptions GCSFileSystem::options() const { return impl_->options(); }

Result<FileInfo> GCSFileSystem::GetFileInfo(const std::string& s) {
  GCSPath path;
  RETURN_NOT_OK(GCSPath::FromString(s, &path));
  FileInfo info;
  info.set_path(s);

  if (path.empty()) {
    // It's the root path ""
    info.set_type(FileType::Directory);
    return info;
  } else if (path.key.empty()) {
    // It's a bucket
    StatusOr<gcs::BucketMetadata> bucket_metadata = impl_->client_->GetBucketMetadata(path.bucket);
    if (bucket_metadata) {
      // NOTE: GCS doesn't have a bucket modification time.  Only a creation
      // time is available, and you have to list all buckets to get it.
      info.set_type(FileType::Directory);
      return info;
    }

    if (!IsNotFound(bucket_metadata)) {
      return ErrorToStatus(std::forward_as_tuple(
          "When getting information for bucket '", path.bucket, "': "), bucket_metadata);
    }

    info.set_type(FileType::NotFound);
    return info;

  } else {
    // It's an object

    StatusOr<gcs::BucketMetadata> bucket_metadata = impl_->client_->GetBucketMetadata(path.bucket);
    if (!bucket_metadata) {
      if (IsNotFound(bucket_metadata)) {
        info.set_type(FileType::NotFound);
        return info;
      } else {
        return ErrorToStatus(std::forward_as_tuple(
            "When getting information for bucket '", path.bucket, "': "), bucket_metadata);
      }
    }

    StatusOr<gcs::ObjectMetadata> object_metadata = impl_->client_->GetObjectMetadata(path.bucket, path.key);

    if (object_metadata) {
      FileObjectToInfo(object_metadata.value(), &info);
      return info;
    }

    if (!IsNotFound(object_metadata)) {
      return ErrorToStatus(std::forward_as_tuple(
          "When getting information for key '", path.key, "' in bucket '", path.bucket, "': "), object_metadata);
    }

    int count = 0;
    RETURN_NOT_OK(impl_->ls(path.bucket, path.key, &count, 2));

    if (count == 0) {
      info.set_type(FileType::NotFound);
    } else {
      info.set_type(FileType::Directory);
    }
    return info;

    // // Not found => perhaps it's an empty "directory"
    // bool is_dir = false;
    // RETURN_NOT_OK(impl_->IsEmptyDirectory(path, &is_dir));
    // if (is_dir) {
    //   info.set_type(FileType::Directory);
    //   return info;
    // }

    // // Not found => perhaps it's a non-empty "directory"
    // RETURN_NOT_OK(impl_->IsNonEmptyDirectory(path, &is_dir));
    // if (is_dir) {
    //   info.set_type(FileType::Directory);
    // } else {
    //   info.set_type(FileType::NotFound);
    // }
    // return info;
  }
}

Result<std::vector<FileInfo>> GCSFileSystem::GetFileInfo(const FileSelector& select) {
  GCSPath base_path;
  RETURN_NOT_OK(GCSPath::FromString(select.base_dir, &base_path));

  std::vector<FileInfo> results;

  if (base_path.empty()) {
    // List all buckets
    std::vector<std::string> buckets;
    RETURN_NOT_OK(impl_->ListBuckets(&buckets));
    for (const auto& bucket : buckets) {
      FileInfo info;
      info.set_path(bucket);
      info.set_type(FileType::Directory);
      results.push_back(std::move(info));
      if (select.recursive) {
        RETURN_NOT_OK(impl_->Walk(select, bucket, "", &results));
      }
    }
    return results;
  }

  // Nominal case -> walk a single bucket
  RETURN_NOT_OK(impl_->Walk(select, base_path.bucket, base_path.key, &results));
  return results;
}

Result<std::shared_ptr<io::InputStream>> GCSFileSystem::OpenInputStream(
    const std::string& s) {
  GCSPath path;
  RETURN_NOT_OK(GCSPath::FromString(s, &path));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<io::RandomAccessFile>> GCSFileSystem::OpenInputFile(
    const std::string& s) {
  GCSPath path;
  RETURN_NOT_OK(GCSPath::FromString(s, &path));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

}  // namespace fs
}  // namespace arrow
