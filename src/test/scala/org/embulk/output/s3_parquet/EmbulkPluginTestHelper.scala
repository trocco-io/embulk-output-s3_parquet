package org.embulk.output.s3_parquet

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ExecutionException

import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3AsyncClient}
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.{
  DownloadDirectoryRequest,
  DirectoryDownload
}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType
import org.embulk.config.{ConfigLoader, ConfigSource, TaskSource}
import org.embulk.spi.{ExecAction, ExecInternal, ExecSessionInternal, Schema}
import org.embulk.test.{EmbulkTestRuntime, PageTestUtils}
import org.msgpack.value.Value
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.diagrams.Diagrams

import scala.jdk.CollectionConverters._
import scala.util.Using

abstract class EmbulkPluginTestHelper
    extends AnyFunSuite
    with BeforeAndAfter
    with Diagrams {
  import implicits._

  protected val runtime: EmbulkTestRuntime = new EmbulkTestRuntime()

  private def exec: ExecSessionInternal = runtime.getExec

  val TEST_S3_ENDPOINT: String = "http://localhost:4566"
  val TEST_S3_REGION: String = "us-east-1"
  val TEST_S3_ACCESS_KEY_ID: String = "test"
  val TEST_S3_SECRET_ACCESS_KEY: String = "test"
  val TEST_BUCKET_NAME: String = "my-bucket"
  val TEST_PATH_PREFIX: String = "path/to/parquet-"

  before {
    withLocalStackS3Client(
      _.createBucket(
        CreateBucketRequest.builder().bucket(TEST_BUCKET_NAME).build()
      )
    )
  }

  after {
    exec.cleanup()

    withLocalStackS3Client { cli =>
      @scala.annotation.tailrec
      def rmRecursive(continuationToken: Option[String]): Unit = {
        val requestBuilder = ListObjectsV2Request
          .builder()
          .bucket(TEST_BUCKET_NAME)
        continuationToken.foreach(requestBuilder.continuationToken)

        val response = cli.listObjectsV2(requestBuilder.build())
        response.contents().asScala.foreach { obj =>
          cli.deleteObject(
            DeleteObjectRequest
              .builder()
              .bucket(TEST_BUCKET_NAME)
              .key(obj.key())
              .build()
          )
        }
        if (response.isTruncated)
          rmRecursive(Some(response.nextContinuationToken()))
      }
      rmRecursive(None)
    }
    withLocalStackS3Client(
      _.deleteBucket(
        DeleteBucketRequest.builder().bucket(TEST_BUCKET_NAME).build()
      )
    )
  }

  def execDoWith[A](f: => A): A =
    try ExecInternal.doWith(
      exec,
      new ExecAction[A] {
        override def run(): A = f
      }
    )
    catch {
      case ex: ExecutionException => throw ex.getCause
    }

  def runOutput(
      outConfig: ConfigSource,
      schema: Schema,
      data: Seq[Seq[Any]],
      messageTypeTest: MessageType => Unit = { _ => }
  ): Seq[Seq[AnyRef]] = {
    execDoWith {
      val plugin = new S3ParquetOutputPlugin()
      plugin.transaction(
        outConfig,
        schema,
        1,
        (taskSource: TaskSource) => {
          Using.resource(plugin.open(taskSource, schema, 0)) { output =>
            try {
              PageTestUtils
                .buildPage(
                  exec.getBufferAllocator,
                  schema,
                  data.flatten: _*
                )
                .foreach(output.add)
              output.commit()
            }
            catch {
              case ex: Throwable =>
                output.abort()
                throw ex
            }
          }
          Seq.empty
        }
      )
    }

    readS3Parquet(TEST_BUCKET_NAME, TEST_PATH_PREFIX, messageTypeTest)
  }

  private def withLocalStackS3Client[A](f: S3Client => A): A = {
    val client: S3Client = S3Client
      .builder()
      .endpointOverride(URI.create(TEST_S3_ENDPOINT))
      .region(Region.of(TEST_S3_REGION))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(
            TEST_S3_ACCESS_KEY_ID,
            TEST_S3_SECRET_ACCESS_KEY
          )
        )
      )
      .forcePathStyle(true)
      .build()

    try f(client)
    finally client.close()
  }

  private def readS3Parquet(
      bucket: String,
      prefix: String,
      messageTypeTest: MessageType => Unit = { _ => }
  ): Seq[Seq[AnyRef]] = {
    val tmpDir: Path = Files.createTempDirectory("embulk-output-parquet")

    val s3AsyncClient = S3AsyncClient
      .builder()
      .endpointOverride(URI.create(TEST_S3_ENDPOINT))
      .region(Region.of(TEST_S3_REGION))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(
            TEST_S3_ACCESS_KEY_ID,
            TEST_S3_SECRET_ACCESS_KEY
          )
        )
      )
      .forcePathStyle(true)
      .build()

    val transferManager =
      S3TransferManager.builder().s3Client(s3AsyncClient).build()

    try {
      val downloadRequest = DownloadDirectoryRequest
        .builder()
        .destination(tmpDir)
        .bucket(bucket)
        .listObjectsV2RequestTransformer(builder =>
          builder.prefix(prefix).build()
        )
        .build()

      val directoryDownload: DirectoryDownload =
        transferManager.downloadDirectory(downloadRequest)
      directoryDownload.completionFuture().join()
    }
    finally {
      transferManager.close()
      s3AsyncClient.close()
    }

    def listFiles(file: File): Seq[File] = {
      file
        .listFiles()
        .flatMap(f =>
          if (f.isFile) Seq(f)
          else listFiles(f)
        )
        .toSeq
    }

    listFiles(tmpDir.toFile)
      .map(_.getAbsolutePath)
      .foldLeft(Seq[Seq[AnyRef]]()) {
        (result: Seq[Seq[AnyRef]], path: String) =>
          result ++ readParquetFile(path, messageTypeTest)
      }
  }

  private def readParquetFile(
      pathString: String,
      messageTypeTest: MessageType => Unit = { _ => }
  ): Seq[Seq[AnyRef]] = {
    Using.resource(
      ParquetFileReader.open(
        HadoopInputFile
          .fromPath(new HadoopPath(pathString), new Configuration())
      )
    ) { reader => messageTypeTest(reader.getFileMetaData.getSchema) }

    val reader: ParquetReader[GenericRecord] = ParquetReader
      .builder(
        new AvroReadSupport[GenericRecord](),
        new HadoopPath(pathString)
      )
      .build()

    Iterator
      .continually(reader.read())
      .takeWhile(_ != null)
      .map(record => record.getSchema.getFields.map(f => record.get(f.name())))
      .toSeq
  }

  def loadConfigSourceFromYamlString(yaml: String): ConfigSource = {
    new ConfigLoader(runtime.getModelManager).fromYamlString(yaml)
  }

  def newDefaultConfig: ConfigSource =
    loadConfigSourceFromYamlString(
      s"""
         |endpoint: $TEST_S3_ENDPOINT
         |bucket: $TEST_BUCKET_NAME
         |path_prefix: $TEST_PATH_PREFIX
         |auth_method: basic
         |access_key_id: $TEST_S3_ACCESS_KEY_ID
         |secret_access_key: $TEST_S3_SECRET_ACCESS_KEY
         |path_style_access_enabled: true
         |default_timezone: Asia/Tokyo
         |""".stripMargin
    )

  def json(str: String): Value = new JsonParser().parse(str)
}
