#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/result.h>
#include <arrow/util/compression.h>

#include <memory>
#include <string>
#include <utility>

#include "arrow/arrow_common.pb.h"  // generated from your proto

inline arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>>
ResolveFileSystem(const std::string& path, arrow::common::FileSystem filesystem) {
  std::string resolved_path = path;

  switch (filesystem) {
    case arrow::common::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), resolved_path);
    }

    case arrow::common::FILE_SYSTEM_S3:
    case arrow::common::FILE_SYSTEM_GCS:
    case arrow::common::FILE_SYSTEM_HDFS: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(resolved_path, &resolved_path));
      return std::make_pair(std::move(fs), resolved_path);
    }

    case arrow::common::FILE_SYSTEM_AUTO:
    default: {
      ARROW_ASSIGN_OR_RAISE(auto fs,
                            arrow::fs::FileSystemFromUriOrPath(resolved_path, &resolved_path));
      return std::make_pair(std::move(fs), resolved_path);
    }
  }
}

inline arrow::Result<arrow::Compression::type> ResolveCompression(
    const std::string& path, arrow::common::Compression compression) {
  switch (compression) {
    case arrow::common::COMPRESSION_UNCOMPRESSED:
      return arrow::Compression::UNCOMPRESSED;

    case arrow::common::COMPRESSION_SNAPPY:
      return arrow::Compression::SNAPPY;

    case arrow::common::COMPRESSION_GZIP:
      return arrow::Compression::GZIP;

    case arrow::common::COMPRESSION_BROTLI:
      return arrow::Compression::BROTLI;

    case arrow::common::COMPRESSION_ZSTD:
      return arrow::Compression::ZSTD;

    case arrow::common::COMPRESSION_LZ4:
      return arrow::Compression::LZ4;

    case arrow::common::COMPRESSION_LZ4_FRAME:
      return arrow::Compression::LZ4_FRAME;

    case arrow::common::COMPRESSION_LZO:
      return arrow::Compression::LZO;

    case arrow::common::COMPRESSION_BZ2:
      return arrow::Compression::BZ2;

    case arrow::common::COMPRESSION_AUTO:
    default:
      return arrow::util::Codec::GetCompressionType(path);
  }
}
