package org.embulk.output.s3_parquet.aws

import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager

object Aws {

  trait Task
      extends AwsCredentials.Task
      with AwsEndpointConfiguration.Task
      with AwsClientConfiguration.Task
      with AwsS3Configuration.Task

  def apply(task: Task): Aws = {
    new Aws(task)
  }

}

class Aws(task: Aws.Task) {

  def withS3[A](f: S3Client => A): A = {
    val svc = createS3Client()
    try f(svc)
    finally svc.close()
  }

  def withTransferManager[A](f: S3TransferManager => A): A = {
    val svc = createTransferManager()
    try f(svc)
    finally svc.close()
  }

  def withGlue[A](f: GlueClient => A): A = {
    val svc = createGlueClient()
    try f(svc)
    finally svc.close()
  }

  private def createS3Client(): S3Client = {
    val endpointConfig = AwsEndpointConfiguration(task)
    val clientConfig = AwsClientConfiguration(task)
    val s3Config = AwsS3Configuration(task)

    val builder = S3Client
      .builder()
      .credentialsProvider(AwsCredentials(task).createAwsCredentialsProvider)
      .region(endpointConfig.getRegion)
      .httpClientBuilder(clientConfig.createHttpClientBuilder)

    endpointConfig.getEndpointOverride.foreach(builder.endpointOverride)
    s3Config.configureS3ClientBuilder(builder)

    builder.build()
  }

  private def createS3AsyncClient(): S3AsyncClient = {
    val endpointConfig = AwsEndpointConfiguration(task)
    val s3Config = AwsS3Configuration(task)

    val builder = S3AsyncClient
      .builder()
      .credentialsProvider(AwsCredentials(task).createAwsCredentialsProvider)
      .region(endpointConfig.getRegion)

    endpointConfig.getEndpointOverride.foreach(builder.endpointOverride)
    s3Config.configureS3ClientBuilder(builder)

    builder.build()
  }

  private def createTransferManager(): S3TransferManager = {
    val s3AsyncClient = createS3AsyncClient()
    S3TransferManager
      .builder()
      .s3Client(s3AsyncClient)
      .build()
  }

  private def createGlueClient(): GlueClient = {
    val endpointConfig = AwsEndpointConfiguration(task)
    val clientConfig = AwsClientConfiguration(task)

    val builder = GlueClient
      .builder()
      .credentialsProvider(AwsCredentials(task).createAwsCredentialsProvider)
      .region(endpointConfig.getRegion)
      .httpClientBuilder(clientConfig.createHttpClientBuilder)

    endpointConfig.getEndpointOverride.foreach(builder.endpointOverride)

    builder.build()
  }
}
