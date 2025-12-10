package org.embulk.output.s3_parquet.aws

import java.net.URI
import java.util.Optional

import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.aws.HttpProxy.Task
import software.amazon.awssdk.http.apache.ProxyConfiguration

object HttpProxy {

  trait Task {

    @Config("host")
    @ConfigDefault("null")
    def getHost: Optional[String]

    @Config("port")
    @ConfigDefault("null")
    def getPort: Optional[Int]

    @Config("protocol")
    @ConfigDefault("\"https\"")
    def getProtocol: String

    @Config("user")
    @ConfigDefault("null")
    def getUser: Optional[String]

    @Config("password")
    @ConfigDefault("null")
    def getPassword: Optional[String]

  }

  def apply(task: Task): HttpProxy = {
    new HttpProxy(task)
  }

}

class HttpProxy(task: Task) {

  def createProxyConfiguration: Option[ProxyConfiguration] = {
    if (task.getHost.isPresent) {
      val builder = ProxyConfiguration.builder()

      val host = task.getHost.get()
      val port =
        task.getPort.orElse(if (task.getProtocol == "https") 443 else 80)
      val scheme = task.getProtocol match {
        case "http"  => "http"
        case "https" => "https"
        case other =>
          throw new ConfigException(
            s"'$other' is unsupported: `protocol` must be one of ['http', 'https']."
          )
      }

      builder.endpoint(URI.create(s"$scheme://$host:$port"))

      task.getUser.ifPresent(u => {
        task.getPassword.ifPresent(p => {
          builder.username(u)
          builder.password(p)
        })
      })

      Some(builder.build())
    }
    else {
      None
    }
  }
}
