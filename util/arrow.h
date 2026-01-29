#pragma once

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/gcsfs.h>
#include <arrow/filesystem/hdfs.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/result.h>
#include <arrow/util/compression.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/arrow_common.pb.h"  // generated from your proto

inline arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>>
ResolveFileSystem(const std::string& path, arrow::common::FileSystem filesystem,
                  const arrow::common::FileSystemOptions& filesystem_options) {
  std::string resolved_path = path;

  auto to_std_map = [](const auto& proto_map) {
    std::unordered_map<std::string, std::string> result;
    result.reserve(static_cast<size_t>(proto_map.size()));
    for (const auto& [key, value] : proto_map) {
      result.emplace(key, value);
    }
    return result;
  };
  auto to_key_value_metadata =
      [](const auto& proto_map) -> std::shared_ptr<const arrow::KeyValueMetadata> {
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(static_cast<size_t>(proto_map.size()));
    values.reserve(static_cast<size_t>(proto_map.size()));
    for (const auto& [key, value] : proto_map) {
      keys.push_back(key);
      values.push_back(value);
    }
    if (keys.empty()) {
      return {};
    }
    return std::static_pointer_cast<const arrow::KeyValueMetadata>(
        arrow::KeyValueMetadata::Make(std::move(keys), std::move(values)));
  };

  auto resolve_uri_path = [&resolved_path]() -> arrow::Result<std::string> {
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(resolved_path, &resolved_path));
    return resolved_path;
  };

  switch (filesystem) {
    case arrow::common::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), resolved_path);
    }

    case arrow::common::FILE_SYSTEM_S3:
    case arrow::common::FILE_SYSTEM_GCS:
    case arrow::common::FILE_SYSTEM_HDFS: {
      break;
    }

    case arrow::common::FILE_SYSTEM_AUTO:
    default: {
      break;
    }
  }

  switch (filesystem_options.options_case()) {
    case arrow::common::FileSystemOptions::kS3: {
      const auto& proto_options = filesystem_options.s3();
      arrow::fs::S3Options options;
      options.smart_defaults = proto_options.smart_defaults();
      options.region = proto_options.region();
      options.connect_timeout = proto_options.connect_timeout();
      options.request_timeout = proto_options.request_timeout();
      options.endpoint_override = proto_options.endpoint_override();
      options.scheme = proto_options.scheme();
      options.role_arn = proto_options.role_arn();
      options.session_name = proto_options.session_name();
      options.external_id = proto_options.external_id();
      options.load_frequency = proto_options.load_frequency();
      options.proxy_options = arrow::fs::S3ProxyOptions{
          proto_options.proxy_options().scheme(),
          proto_options.proxy_options().host(),
          proto_options.proxy_options().port(),
          proto_options.proxy_options().username(),
          proto_options.proxy_options().password(),
      };
      options.credentials_kind = static_cast<arrow::fs::S3CredentialsKind>(
          proto_options.credentials_kind());
      options.force_virtual_addressing = proto_options.force_virtual_addressing();
      options.background_writes = proto_options.background_writes();
      options.allow_bucket_creation = proto_options.allow_bucket_creation();
      options.allow_bucket_deletion = proto_options.allow_bucket_deletion();
      options.check_directory_existence_before_creation =
          proto_options.check_directory_existence_before_creation();
      options.allow_delayed_open = proto_options.allow_delayed_open();
      options.default_metadata = to_key_value_metadata(proto_options.default_metadata());
      options.retry_strategy = arrow::fs::S3RetryStrategy{
          static_cast<arrow::fs::S3RetryStrategyKind>(proto_options.retry_strategy().kind()),
          proto_options.retry_strategy().max_attempts(),
      };
      options.sse_customer_key =
          std::string(proto_options.sse_customer_key().begin(),
                      proto_options.sse_customer_key().end());
      options.tls_ca_file_path = proto_options.tls_ca_file_path();
      options.tls_ca_dir_path = proto_options.tls_ca_dir_path();
      options.tls_verify_certificates = proto_options.tls_verify_certificates();

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::S3FileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::common::FileSystemOptions::kGcs: {
      const auto& proto_options = filesystem_options.gcs();
      arrow::fs::GcsOptions options;
      options.credentials.anonymous = proto_options.credentials().anonymous();
      options.credentials.access_token = proto_options.credentials().access_token();
      options.credentials.target_service_account = proto_options.credentials().target_service_account();
      options.credentials.json_credentials = proto_options.credentials().json_credentials();
      if (proto_options.credentials().expiration().seconds() != 0 ||
          proto_options.credentials().expiration().nanos() != 0) {
        options.credentials.expiration =
            std::chrono::system_clock::from_time_t(
                proto_options.credentials().expiration().seconds()) +
            std::chrono::nanoseconds(proto_options.credentials().expiration().nanos());
      }
      options.endpoint_override = proto_options.endpoint_override();
      options.scheme = proto_options.scheme();
      options.default_bucket_location = proto_options.default_bucket_location();
      if (proto_options.has_retry_limit_seconds()) {
        options.retry_limit_seconds = proto_options.retry_limit_seconds();
      }
      options.default_metadata = to_key_value_metadata(proto_options.default_metadata());
      if (proto_options.has_project_id()) {
        options.project_id = proto_options.project_id();
      }

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::GcsFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::common::FileSystemOptions::kAzure: {
      const auto& proto_options = filesystem_options.azure();
      arrow::fs::AzureOptions options;
      options.account_name = proto_options.account_name();
      options.blob_storage_authority = proto_options.blob_storage_authority();
      options.dfs_storage_authority = proto_options.dfs_storage_authority();
      options.blob_storage_scheme = proto_options.blob_storage_scheme();
      options.dfs_storage_scheme = proto_options.dfs_storage_scheme();
      options.default_metadata = to_key_value_metadata(proto_options.default_metadata());
      options.background_writes = proto_options.background_writes();
      options.credentials.kind =
          static_cast<arrow::fs::AzureCredentialKind>(proto_options.credentials().kind());
      options.credentials.storage_shared_key = proto_options.credentials().storage_shared_key();
      options.credentials.sas_token = proto_options.credentials().sas_token();
      options.credentials.tenant_id = proto_options.credentials().tenant_id();
      options.credentials.client_id = proto_options.credentials().client_id();
      options.credentials.client_secret = proto_options.credentials().client_secret();

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::AzureFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::common::FileSystemOptions::kHdfs: {
      const auto& proto_options = filesystem_options.hdfs();
      arrow::fs::HdfsOptions options;
      options.connection_config.host = proto_options.connection_config().host();
      options.connection_config.port = proto_options.connection_config().port();
      options.connection_config.user = proto_options.connection_config().user();
      options.connection_config.kerb_ticket = proto_options.connection_config().kerb_ticket();
      options.connection_config.extra_conf =
          to_std_map(proto_options.connection_config().extra_conf());
      options.buffer_size = proto_options.buffer_size();
      options.replication = proto_options.replication();
      options.default_block_size = proto_options.default_block_size();

      ARROW_RETURN_NOT_OK(resolve_uri_path());
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::HadoopFileSystem::Make(options));
      return std::make_pair(std::move(fs), resolved_path);
    }
    case arrow::common::FileSystemOptions::OPTIONS_NOT_SET:
      break;
  }

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

inline arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>>
ResolveFileSystem(const std::string& path, const arrow::common::Common& common) {
  return ResolveFileSystem(path, common.filesystem(), common.filesystem_options());
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
