# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: language_level = 3

from pyarrow.lib cimport check_status
from pyarrow.lib import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_fs cimport *
from pyarrow._fs cimport FileSystem


cpdef enum GCSLogLevel:
    Off = <int8_t> CGCSLogLevel_Off
    Fatal = <int8_t> CGCSLogLevel_Fatal
    Error = <int8_t> CGCSLogLevel_Error
    Warn = <int8_t> CGCSLogLevel_Warn
    Info = <int8_t> CGCSLogLevel_Info
    Debug = <int8_t> CGCSLogLevel_Debug
    Trace = <int8_t> CGCSLogLevel_Trace


def initialize_gcs(GCSLogLevel log_level=GCSLogLevel.Fatal):
    cdef CGCSGlobalOptions options
    options.log_level = <CGCSLogLevel> log_level
    check_status(CInitializeGCS(options))


def finalize_gcs():
    check_status(CFinalizeGCS())


cdef class GCSFileSystem(FileSystem):
    """GCS-backed FileSystem implementation

    If neither access_key nor secret_key are provided then attempts to
    initialize from AWS environment variables, otherwise both access_key and
    secret_key must be provided.

    Note: GCS buckets are special and the operations available on them may be
    limited or more expensive than desired.

    Parameters
    ----------
    region: str, default 'us-central-1'
        AWS region to connect to.
    scheme: str, default 'https'
        GCS connection transport scheme.
    endpoint_override: str, default None
        Override region with a connect string such as "localhost:9000"
    """

    cdef:
        CGCSFileSystem* gcsfs

    def __init__(self, region=None,
                 scheme=None, endpoint_override=None):
        cdef:
            CGCSOptions options
            shared_ptr[CGCSFileSystem] wrapped

        options = CGCSOptions.Defaults()

        if region is not None:
            options.region = tobytes(region)
        if scheme is not None:
            options.scheme = tobytes(scheme)
        if endpoint_override is not None:
            options.endpoint_override = tobytes(endpoint_override)

        with nogil:
            wrapped = GetResultValue(CGCSFileSystem.Make(options))

        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.gcsfs = <CGCSFileSystem*> wrapped.get()

    @classmethod
    def _reconstruct(cls, kwargs):
        return cls(**kwargs)

    def __reduce__(self):
        cdef CGCSOptions opts = self.gcsfs.options()
        return (
            GCSFileSystem, (
                frombytes(opts.region),
                frombytes(opts.scheme),
                frombytes(opts.endpoint_override)
            )
        )
