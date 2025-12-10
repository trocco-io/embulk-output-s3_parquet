package org.embulk.output.s3_parquet.aws

import java.net.URI
import java.util.Optional

import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.output.s3_parquet.aws.AwsEndpointConfiguration.Task
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

import scala.util.Try

object AwsEndpointConfiguration {

  trait Task {

    @Config("endpoint")
    @ConfigDefault("null")
    def getEndpoint: Optional[String]

    @Config("region")
    @ConfigDefault("null")
    def getRegion: Optional[String]

  }

  def apply(task: Task): AwsEndpointConfiguration = {
    new AwsEndpointConfiguration(task)
  }
}

class AwsEndpointConfiguration(task: Task) {

  def getRegion: Region = {
    if (task.getRegion.isPresent) {
      Region.of(task.getRegion.get())
    }
    else {
      Try(new DefaultAwsRegionProviderChain().getRegion)
        .getOrElse(Region.US_EAST_1)
    }
  }

  def getEndpointOverride: Option[URI] = {
    if (task.getEndpoint.isPresent) {
      Some(URI.create(task.getEndpoint.get()))
    }
    else {
      None
    }
  }

}
