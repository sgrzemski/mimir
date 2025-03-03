# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

NOTE: As semantic versioning states all 0.y.z releases can contain breaking changes in API (flags, grpc API, any backward compatibility)

We use *breaking :warning:* to mark changes that are not backward compatible (relates only to v0.y.z releases.)

## Unreleased

### Fixed
- [#33](https://github.com/thanos-io/objstore/pull/33) Tracing: Add `ContextWithTracer()` to inject the tracer into the context.
- [#34](https://github.com/thanos-io/objstore/pull/34) Fix ignored options when creating shared credential Azure client.
- [#62](https://github.com/thanos-io/objstore/pull/62) S3: Fix ignored context cancellation in `Iter` method.
- [#77](https://github.com/thanos-io/objstore/pull/77) Fix buckets wrapped with metrics from being unable to determine object sizes in `Upload`.
- [#78](https://github.com/thanos-io/objstore/pull/78) S3: Fix possible concurrent modification of the PutUserMetadata map.

### Added
- [#15](https://github.com/thanos-io/objstore/pull/15) Add Oracle Cloud Infrastructure Object Storage Bucket support.
- [#25](https://github.com/thanos-io/objstore/pull/25) S3: Support specifying S3 storage class.
- [#32](https://github.com/thanos-io/objstore/pull/32) Swift: Support authentication using application credentials.
- [#41](https://github.com/thanos-io/objstore/pull/41) S3: Support S3 session token.
- [#43](https://github.com/thanos-io/objstore/pull/43) filesystem: abort filesystem bucket operations if the context has been cancelled
- [#44](https://github.com/thanos-io/objstore/pull/44) Add new metric to count total number of fetched bytes from bucket
- [#50](https://github.com/thanos-io/objstore/pull/50) Add Huawei Cloud OBS Object Storage Support
- [#59](https://github.com/thanos-io/objstore/pull/59) Adding method `IsCustomerManagedKeyError` on the bucket interface.
- [#61](https://github.com/thanos-io/objstore/pull/61) Add OpenTelemetry TracingBucket.
    > This also changes the behaviour of `client.NewBucket`. Now it returns, uninstrumented and untraced bucket.
    You can combine `objstore.WrapWithMetrics` and `tracing/{opentelemetry,opentracing}.WrapWithTraces` to have old behavior.
- [#69](https://github.com/thanos-io/objstore/pull/69) [#66](https://github.com/thanos-io/objstore/pull/66) Add `objstore_bucket_operation_transferred_bytes` that counts the number of total bytes read from the bucket operation Get/GetRange and also counts the number of total bytes written to the bucket operation Upload.
- [#64](https://github.com/thanos-io/objstore/pull/64) OCI: OKE Workload Identity support.
- [#73](https://github.com/thanos-io/objstore/pull/73) Аdded file path to erros from DownloadFile
- [#51](https://github.com/thanos-io/objstore/pull/51) Azure: Support using connection string authentication.
- [#76](https://github.com/thanos-io/objstore/pull/76) GCS: Query for object names only in `Iter` to possibly improve performance when listing objects.

### Changed
- [#38](https://github.com/thanos-io/objstore/pull/38) *: Upgrade minio-go version to `v7.0.45`.
- [#39](https://github.com/thanos-io/objstore/pull/39) COS: Upgrade cos sdk version to `v0.7.40`.
- [#35](https://github.com/thanos-io/objstore/pull/35) Azure: Update Azure SDK and fix breaking changes.
- [#65](https://github.com/thanos-io/objstore/pull/65) *: Upgrade minio-go version to `v7.0.61`.
- [#70](https://github.com/thanos-io/objstore/pull/70) GCS: Update cloud.google.com/go/storage version to `v1.27.0`.
- [#71](https://github.com/thanos-io/objstore/pull/71) Replace method `IsCustomerManagedKeyError` for a more generic `IsAccessDeniedErr` on the bucket interface.
### Removed
