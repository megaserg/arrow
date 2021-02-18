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

#include <exception>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// boost/process/detail/windows/handle_workaround.hpp doesn't work
// without BOOST_USE_WINDOWS_H with MinGW because MinGW doesn't
// provide __kernel_entry without winternl.h.
//
// See also:
// https://github.com/boostorg/process/blob/develop/include/boost/process/detail/windows/handle_workaround.hpp
#include <boost/process.hpp>

#include <gtest/gtest.h>

#include <google/cloud/storage/client.h>
#include <google/cloud/storage/object_stream.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/gcs_internal.h"
#include "arrow/filesystem/gcs_test_util.h"
#include "arrow/filesystem/gcsfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace fs {

using ::arrow::internal::PlatformFilename;
using ::arrow::internal::UriEscape;

using ::arrow::fs::internal::ErrorToStatus;

namespace bp = boost::process;

namespace gcs = google::cloud::storage;
using ::google::cloud::StatusOr;

// NOTE: Connecting in Python:
// >>> fs = gcsfs.GCSFileSystem(
// client_kwargs=dict(endpoint_url='http://127.0.0.1:9000'))
// >>> fs.ls('')
// ['bucket']
// or:
// >>> from fs_gcsfs import GCSFS
// >>> fs = GCSFS('bucket', endpoint_url='http://127.0.0.1:9000')

class GCSTestMixin : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_OK(fakegcs_.Start());

    client_config_ = gcs::ClientOptions::CreateDefaultClientOptions();
    std::string endpoint = "http://" + fakegcs_.connect_string();
    client_config_->set_endpoint(endpoint);
    auto creds = gcs::oauth2::CreateAnonymousCredentials();
    client_config_->set_credentials(creds);
    // const std::string cert_file = "/etc/ssl/certs/ca-certificates.crt";
    // client_config_->channel_options().set_ssl_root_path(cert_file);

    client_.reset(new gcs::Client(*client_config_));
  }

  void TearDown() override { ASSERT_OK(fakegcs_.Stop()); }

 protected:
  FakegcsTestServer fakegcs_;
  StatusOr<gcs::ClientOptions> client_config_;
  std::unique_ptr<gcs::Client> client_;
};

void AssertObjectContents(gcs::Client* client, const std::string& bucket,
                          const std::string& key, const std::string& expected) {

  StatusOr<gcs::ObjectMetadata> object_metadata = client->GetObjectMetadata(bucket, key);
  if (!object_metadata) {
    throw std::runtime_error(object_metadata.status().message());
  }

  auto length = static_cast<int64_t>(expected.length());
  ASSERT_EQ(object_metadata->size(), length);

  auto stream = client->ReadObject(bucket, key);
  // std::string actual{std::istreambuf_iterator<char>{reader}, {}};
  std::string actual;
  actual.resize(length + 1);
  stream.read(&actual[0], length + 1);
  ASSERT_EQ(stream.gcount(), length);  // EOF was reached before length + 1
  actual.resize(length);

  ASSERT_EQ(actual.size(), expected.size());
  ASSERT_TRUE(actual == expected);  // Avoid ASSERT_EQ on large data
}

////////////////////////////////////////////////////////////////////////////
// GCSOptions tests

TEST(GCSOptions, FromUri) {
  std::string path;
  GCSOptions options;

  ASSERT_OK_AND_ASSIGN(options, GCSOptions::FromUri("gs://", &path));
  ASSERT_EQ(options.region, kGCSDefaultRegion);
  ASSERT_EQ(options.scheme, "https");
  ASSERT_EQ(options.endpoint_override, "");
  ASSERT_EQ(path, "");

  ASSERT_OK_AND_ASSIGN(options, GCSOptions::FromUri("gs:", &path));
  ASSERT_EQ(path, "");

  ASSERT_OK_AND_ASSIGN(options, GCSOptions::FromUri("gs://mybucket/", &path));
  ASSERT_EQ(options.region, kGCSDefaultRegion);
  ASSERT_EQ(options.scheme, "https");
  ASSERT_EQ(options.endpoint_override, "");
  ASSERT_EQ(path, "mybucket");

  ASSERT_OK_AND_ASSIGN(options, GCSOptions::FromUri("gs://mybucket/foo/bar/", &path));
  ASSERT_EQ(options.region, kGCSDefaultRegion);
  ASSERT_EQ(options.scheme, "https");
  ASSERT_EQ(options.endpoint_override, "");
  ASSERT_EQ(path, "mybucket/foo/bar");

  ASSERT_OK_AND_ASSIGN(
      options,
      GCSOptions::FromUri(
          "gs://mybucket/foo/bar/?region=utopia&endpoint_override=localhost&scheme=http",
          &path));
  ASSERT_EQ(options.region, "utopia");
  ASSERT_EQ(options.scheme, "http");
  ASSERT_EQ(options.endpoint_override, "localhost");
  ASSERT_EQ(path, "mybucket/foo/bar");

  // Missing bucket name
  ASSERT_RAISES(Invalid, GCSOptions::FromUri("gs:///foo/bar/", &path));
}

////////////////////////////////////////////////////////////////////////////
// Basic test for the Fakegcs test server.

class TestFakegcsServer : public GCSTestMixin {
 public:
  void SetUp() override { GCSTestMixin::SetUp(); }

 protected:
};

TEST_F(TestFakegcsServer, Connect) {
  // Just a dummy connection test.  Check that we can list buckets,
  // and that there are none (the server is launched in an empty temp dir).
  int count = 0;
  gcs::ListBucketsReader bucket_list = client_->ListBuckets();
  for (auto&& bucket_metadata : bucket_list) {
    if (!bucket_metadata) {
      throw std::runtime_error(bucket_metadata.status().message());
    }
    ++count;
  }
  ASSERT_EQ(count, 0);
}

////////////////////////////////////////////////////////////////////////////
// Concrete GCS tests

class TestGCSFS : public GCSTestMixin {
 public:
  void SetUp() override {
    GCSTestMixin::SetUp();
    MakeFileSystem();

    // Set up test bucket
    {
      StatusOr<gcs::BucketMetadata> bucket_metadata =
          client_->CreateBucket("bucket", gcs::BucketMetadata());
      if (!bucket_metadata) {
        throw std::runtime_error(bucket_metadata.status().message());
      }
    }
    {
      StatusOr<gcs::BucketMetadata> bucket_metadata =
          client_->CreateBucket("empty-bucket", gcs::BucketMetadata());
      if (!bucket_metadata) {
        throw std::runtime_error(bucket_metadata.status().message());
      }
    }

    // TODO(megaserg): Empty directory testing doesn't quite work with fake-gcs-server.
    // {
    //   std::string contents = "TO BE DELETED";
    //   StatusOr<gcs::ObjectMetadata> object_metadata =
    //       client_->InsertObject("bucket", "emptydir/filetodelete", std::move(contents));
    //   if (!object_metadata) {
    //     throw std::runtime_error(object_metadata.status().message());
    //   }
    //   google::cloud::Status delete_status = client_->DeleteObject("bucket", "emptydir/filetodelete");
    //   if (!delete_status.ok()) {
    //     throw std::runtime_error(delete_status.message());
    //   }
    // }
    {
      std::string contents = "sub data";
      StatusOr<gcs::ObjectMetadata> object_metadata =
          client_->InsertObject("bucket", "somedir/subdir/subfile", std::move(contents));
      if (!object_metadata) {
        throw std::runtime_error(object_metadata.status().message());
      }
    }
    {
      std::string contents = "some data";
      StatusOr<gcs::ObjectMetadata> object_metadata =
          client_->InsertObject("bucket", "somefile", std::move(contents));
      if (!object_metadata) {
        throw std::runtime_error(object_metadata.status().message());
      }
    }
  }

  void MakeFileSystem() {
    options_.scheme = "http";
    options_.endpoint_override = fakegcs_.connect_string();
    ASSERT_OK_AND_ASSIGN(fs_, GCSFileSystem::Make(options_));
  }

 protected:
  GCSOptions options_;
  std::shared_ptr<GCSFileSystem> fs_;
};

TEST_F(TestGCSFS, GetFileInfoRoot) { AssertFileInfo(fs_.get(), "", FileType::Directory); }

TEST_F(TestGCSFS, GetFileInfoBucket) {
  AssertFileInfo(fs_.get(), "bucket", FileType::Directory);
  AssertFileInfo(fs_.get(), "empty-bucket", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-bucket", FileType::NotFound);
  // Trailing slashes
  AssertFileInfo(fs_.get(), "bucket/", FileType::Directory);
  AssertFileInfo(fs_.get(), "empty-bucket/", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-bucket/", FileType::NotFound);
}

TEST_F(TestGCSFS, GetFileInfoObject) {
  // "Directories"
  // AssertFileInfo(fs_.get(), "bucket/emptydir", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/somedir", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/somedir/subdir", FileType::Directory, kNoSize);

  // "Files"
  AssertFileInfo(fs_.get(), "bucket/somefile", FileType::File, 9);
  AssertFileInfo(fs_.get(), "bucket/somedir/subdir/subfile", FileType::File, 8);

  // Nonexistent
  // AssertFileInfo(fs_.get(), "bucket/emptyd", FileType::NotFound);
  AssertFileInfo(fs_.get(), "bucket/somed", FileType::NotFound);
  AssertFileInfo(fs_.get(), "non-existent-bucket/somed", FileType::NotFound);

  // Trailing slashes
  // AssertFileInfo(fs_.get(), "bucket/emptydir/", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "bucket/somefile/", FileType::File, 9);
  AssertFileInfo(fs_.get(), "bucket/somed/", FileType::NotFound);
  // AssertFileInfo(fs_.get(), "bucket/emptyd/", FileType::NotFound);
  AssertFileInfo(fs_.get(), "non-existent-bucket/somed/", FileType::NotFound);
}

TEST_F(TestGCSFS, GetFileInfoSelector) {
  FileSelector select;
  std::vector<FileInfo> infos;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "bucket", FileType::Directory);
  AssertFileInfo(infos[1], "empty-bucket", FileType::Directory);

  // Empty bucket
  select.base_dir = "empty-bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  // Nonexistent bucket
  select.base_dir = "nonexistent-bucket";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;
  // Non-empty bucket
  select.base_dir = "bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 2);
  AssertFileInfo(infos[0], "bucket/somedir/subdir/subfile", FileType::File, 8);
  AssertFileInfo(infos[1], "bucket/somefile", FileType::File, 9);

  // Empty "directory"
  // select.base_dir = "bucket/emptydir";
  // ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  // ASSERT_EQ(infos.size(), 0);
  // Non-empty "directories"
  select.base_dir = "bucket/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "bucket/somedir/subdir/subfile", FileType::File, 8);
  select.base_dir = "bucket/somedir/subdir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "bucket/somedir/subdir/subfile", FileType::File, 8);
  // Nonexistent
  select.base_dir = "bucket/nonexistent";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // Trailing slashes
  select.base_dir = "empty-bucket/";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.base_dir = "nonexistent-bucket/";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.base_dir = "bucket/";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 2);
}

TEST_F(TestGCSFS, GetFileInfoSelectorRecursive) {
  FileSelector select;
  std::vector<FileInfo> infos;
  select.recursive = true;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 4);
  SortInfos(&infos);
  // TODO(megaserg): Directory listing doesn't quite work with fake-gcs-server either.
  AssertFileInfo(infos[0], "bucket", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/somedir/subdir/subfile", FileType::File, 8);
  // AssertFileInfo(infos[2], "bucket/somedir/subdir", FileType::Directory);
  // AssertFileInfo(infos[3], "bucket/somedir", FileType::Directory);
  AssertFileInfo(infos[2], "bucket/somefile", FileType::File, 9);
  AssertFileInfo(infos[3], "empty-bucket", FileType::Directory);

  // Empty bucket
  select.base_dir = "empty-bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // Non-empty bucket
  select.base_dir = "bucket";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 2);
  AssertFileInfo(infos[0], "bucket/somedir/subdir/subfile", FileType::File, 8);
  // AssertFileInfo(infos[1], "bucket/somedir/subdir", FileType::Directory);
  // AssertFileInfo(infos[2], "bucket/somedir", FileType::Directory);
  AssertFileInfo(infos[1], "bucket/somefile", FileType::File, 9);

  // Empty "directory"
  // select.base_dir = "bucket/emptydir";
  // ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  // ASSERT_EQ(infos.size(), 0);

  // Non-empty "directories"
  select.base_dir = "bucket/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 1);
  // AssertFileInfo(infos[0], "bucket/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[0], "bucket/somedir/subdir/subfile", FileType::File, 8);
}

TEST_F(TestGCSFS, OpenInputStream) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buf;

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->OpenInputStream("nonexistent-bucket/somefile"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/zzzt"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("bucket/somefile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(2));
  AssertBufferEqual(*buf, "so");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "ta");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "");

  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("bucket/somedir/subdir/subfile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "sub data");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "");

  // "Directories"
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/emptydir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("bucket"));
}

TEST_F(TestGCSFS, OpenInputFile) {
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->OpenInputFile("nonexistent-bucket/somefile"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("bucket/zzzt"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile("bucket/somefile"));
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_ASSIGN(buf, file->Read(4));
  AssertBufferEqual(*buf, "some");
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_EQ(4, file->Tell());

  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(2, 5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_EQ(4, file->Tell());
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(5, 20));
  AssertBufferEqual(*buf, "data");
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(9, 20));
  AssertBufferEqual(*buf, "");

  char result[10];
  ASSERT_OK_AND_EQ(5, file->ReadAt(2, 5, &result));
  ASSERT_OK_AND_EQ(4, file->ReadAt(5, 20, &result));
  ASSERT_OK_AND_EQ(0, file->ReadAt(9, 0, &result));

  // Reading past end of file
  ASSERT_RAISES(IOError, file->ReadAt(10, 20));

  ASSERT_OK(file->Seek(5));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "da");
  ASSERT_OK(file->Seek(9));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "");
  // Seeking past end of file
  ASSERT_RAISES(IOError, file->Seek(10));
}

TEST_F(TestGCSFS, FileSystemFromUri) {
  std::stringstream ss;
  ss << "gs://"
     << "bucket/somedir/subdir/subfile"
     << "?scheme=http&endpoint_override=" << UriEscape(fakegcs_.connect_string());

  std::string path;
  ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFromUri(ss.str(), &path));
  ASSERT_EQ(path, "bucket/somedir/subdir/subfile");

  // Check the filesystem has the right connection parameters
  AssertFileInfo(fs.get(), path, FileType::File, 8);
}

//////////////////////////////////////////////////////////////////////////
// Generic GCS tests

class TestGCSFSGeneric : public GCSTestMixin, public GenericFileSystemTest {
 public:
  void SetUp() override {
    GCSTestMixin::SetUp();
    // Set up test bucket
    {
      StatusOr<gcs::BucketMetadata> bucket_metadata =
          client_->CreateBucket("gcsfs-test-bucket", gcs::BucketMetadata());
      if (!bucket_metadata) {
        throw std::runtime_error(bucket_metadata.status().message());
      }
    }

    options_.scheme = "http";
    options_.endpoint_override = fakegcs_.connect_string();
    ASSERT_OK_AND_ASSIGN(gcsfs_, GCSFileSystem::Make(options_));
    fs_ = std::make_shared<SubTreeFileSystem>("gcsfs-test-bucket", gcsfs_);
  }

 protected:
  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  bool have_implicit_directories() const override { return true; }
  bool allow_write_file_over_dir() const override { return true; }
  bool allow_move_dir() const override { return false; }
  bool allow_append_to_file() const override { return false; }
  bool have_directory_mtimes() const override { return false; }
  bool have_flaky_directory_tree_deletion() const override { return false; }

  GCSOptions options_;
  std::shared_ptr<GCSFileSystem> gcsfs_;
  std::shared_ptr<FileSystem> fs_;
};

// TODO(megaserg): Need to implement at least CreateDir and OpenOutputStream for this to work.
// GENERIC_FS_TEST_FUNCTIONS(TestGCSFSGeneric);

}  // namespace fs
}  // namespace arrow
