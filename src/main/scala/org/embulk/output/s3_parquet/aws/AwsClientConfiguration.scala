package org.embulk.output.s3_parquet.aws

import java.util.Optional

import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.output.s3_parquet.aws.AwsClientConfiguration.Task
import software.amazon.awssdk.http.apache.ApacheHttpClient

object AwsClientConfiguration {

  trait Task {

    @Config("http_proxy")
    @ConfigDefault("null")
    def getHttpProxy: Optional[HttpProxy.Task]

  }

  def apply(task: Task): AwsClientConfiguration = {
    new AwsClientConfiguration(task)
  }
}

class AwsClientConfiguration(task: Task) {

  def createHttpClientBuilder: ApacheHttpClient.Builder = {
    val builder = ApacheHttpClient.builder()

    task.getHttpProxy.ifPresent { proxyTask =>
      HttpProxy(proxyTask).createProxyConfiguration.foreach { proxyConfig =>
        builder.proxyConfiguration(proxyConfig)
      }
    }

    builder
  }

}
