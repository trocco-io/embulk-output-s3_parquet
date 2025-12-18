package org.embulk.output.s3_parquet.aws

import java.util.Optional

import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.output.s3_parquet.aws.AwsS3Configuration.Task
import software.amazon.awssdk.services.s3.{
  S3ClientBuilder,
  S3AsyncClientBuilder
}

/*
 * These are advanced settings, so write no documentation.
 */
object AwsS3Configuration {

  trait Task {

    @Config("accelerate_mode_enabled")
    @ConfigDefault("null")
    def getAccelerateModeEnabled: Optional[Boolean]

    @Config("chunked_encoding_disabled")
    @ConfigDefault("null")
    def getChunkedEncodingDisabled: Optional[Boolean]

    @Config("dualstack_enabled")
    @ConfigDefault("null")
    def getDualstackEnabled: Optional[Boolean]

    @Config("force_global_bucket_access_enabled")
    @ConfigDefault("null")
    def getForceGlobalBucketAccessEnabled: Optional[Boolean]

    @Config("path_style_access_enabled")
    @ConfigDefault("null")
    def getPathStyleAccessEnabled: Optional[Boolean]

    @Config("payload_signing_enabled")
    @ConfigDefault("null")
    def getPayloadSigningEnabled: Optional[Boolean]

  }

  def apply(task: Task): AwsS3Configuration = {
    new AwsS3Configuration(task)
  }
}

class AwsS3Configuration(task: Task) {

  def configureS3ClientBuilder(builder: S3ClientBuilder): Unit = {
    task.getAccelerateModeEnabled.ifPresent(v => builder.accelerate(v))
    task.getDualstackEnabled.ifPresent(v => builder.dualstackEnabled(v))
    task.getForceGlobalBucketAccessEnabled.ifPresent(v =>
      builder.forcePathStyle(!v) // v2では逆の意味になる
    )
    task.getPathStyleAccessEnabled.ifPresent(v => builder.forcePathStyle(v))
    // Note: chunked_encoding_disabled and payload_signing_enabled are not directly supported in SDK v2
    // These would need to be configured at the HTTP client level if needed
  }

  def configureS3ClientBuilder(builder: S3AsyncClientBuilder): Unit = {
    task.getAccelerateModeEnabled.ifPresent(v => builder.accelerate(v))
    task.getDualstackEnabled.ifPresent(v => builder.dualstackEnabled(v))
    task.getForceGlobalBucketAccessEnabled.ifPresent(v =>
      builder.forcePathStyle(!v) // v2では逆の意味になる
    )
    task.getPathStyleAccessEnabled.ifPresent(v => builder.forcePathStyle(v))
    // Note: chunked_encoding_disabled and payload_signing_enabled are not directly supported in SDK v2
    // These would need to be configured at the HTTP client level if needed
  }

}
