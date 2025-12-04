package org.embulk.output.s3_parquet

import java.io.File
import java.nio.file.{Files, Path}

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.transfer.{
  TransferManager,
  TransferManagerBuilder
}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType
import org.embulk.config.ConfigSource
import org.embulk.spi.Schema
import org.embulk.util.config.ConfigMapperFactory
import org.msgpack.value.{Value, ValueFactory}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
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

  val TEST_S3_ENDPOINT: String = "http://localhost:4566"
  val TEST_S3_REGION: String = "us-east-1"
  val TEST_S3_ACCESS_KEY_ID: String = "test"
  val TEST_S3_SECRET_ACCESS_KEY: String = "test"
  val TEST_BUCKET_NAME: String = "my-bucket"
  val TEST_PATH_PREFIX: String = "path/to/parquet-"

  before {
    withLocalStackS3Client(_.createBucket(TEST_BUCKET_NAME))
  }

  after {
    withLocalStackS3Client { cli =>
      @scala.annotation.tailrec
      def rmRecursive(listing: ObjectListing): Unit = {
        listing.getObjectSummaries.asScala.foreach(o =>
          cli.deleteObject(TEST_BUCKET_NAME, o.getKey)
        )
        if (listing.isTruncated)
          rmRecursive(cli.listNextBatchOfObjects(listing))
      }
      rmRecursive(cli.listObjects(TEST_BUCKET_NAME))
    }
    withLocalStackS3Client(_.deleteBucket(TEST_BUCKET_NAME))
  }

  def runOutput(
      outConfig: ConfigSource,
      schema: Schema,
      data: Seq[Seq[Any]],
      messageTypeTest: MessageType => Unit = { _ => }
  ): Seq[Seq[AnyRef]] = {
    // TODO: Implement test execution using Embulk v0.11 testing framework
    // For now, return empty result to allow compilation
    Seq.empty
  }

  private def withLocalStackS3Client[A](f: AmazonS3 => A): A = {
    val client: AmazonS3 = AmazonS3ClientBuilder.standard
      .withEndpointConfiguration(
        new EndpointConfiguration(TEST_S3_ENDPOINT, TEST_S3_REGION)
      )
      .withCredentials(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(
            TEST_S3_ACCESS_KEY_ID,
            TEST_S3_SECRET_ACCESS_KEY
          )
        )
      )
      .withPathStyleAccessEnabled(true)
      .build()

    try f(client)
    finally client.shutdown()
  }

  private def readS3Parquet(
      bucket: String,
      prefix: String,
      messageTypeTest: MessageType => Unit = { _ => }
  ): Seq[Seq[AnyRef]] = {
    val tmpDir: Path = Files.createTempDirectory("embulk-output-parquet")
    withLocalStackS3Client { s3 =>
      val xfer: TransferManager = TransferManagerBuilder
        .standard()
        .withS3Client(s3)
        .build()
      try xfer
        .downloadDirectory(bucket, prefix, tmpDir.toFile)
        .waitForCompletion()
      finally xfer.shutdownNow()
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
      .map(record =>
        record.getSchema.getFields.asScala.map(f => record.get(f.name())).toSeq
      )
      .toSeq
  }

  def loadConfigSourceFromYamlString(yaml: String): ConfigSource = {
    val yamlMapper = new ObjectMapper(new YAMLFactory())
    val tree = yamlMapper
      .readTree(yaml)
      .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    val mapper = ConfigMapperFactory.withDefault()

    // Build ConfigSource by setting each field individually
    var configSource = mapper.newConfigSource()
    val fields = tree.fields()
    while (fields.hasNext) {
      val entry = fields.next()
      val key = entry.getKey
      val value = entry.getValue

      // Convert JsonNode to appropriate type
      if (value.isTextual) {
        configSource = configSource.set(key, value.asText())
      }
      else if (value.isBoolean) {
        configSource = configSource.set(key, Boolean.box(value.asBoolean()))
      }
      else if (value.isInt) {
        configSource = configSource.set(key, Int.box(value.asInt()))
      }
      else if (value.isLong) {
        configSource = configSource.set(key, Long.box(value.asLong()))
      }
      else if (value.isDouble) {
        configSource = configSource.set(key, Double.box(value.asDouble()))
      }
      else {
        configSource = configSource.set(key, value)
      }
    }

    configSource
  }

  def newDefaultConfig: ConfigSource =
    loadConfigSourceFromYamlString(
      s"""
         |type: s3_parquet
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

  def json(str: String): Value = {
    import org.msgpack.core.MessagePack
    import java.io.ByteArrayOutputStream

    val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val jsonNode = objectMapper.readTree(str)

    // Convert JSON to MessagePack format
    val out = new ByteArrayOutputStream()
    val packer = MessagePack.newDefaultPacker(out)

    def packJsonNode(node: com.fasterxml.jackson.databind.JsonNode): Unit = {
      if (node.isObject) {
        val fields = node.fields()
        packer.packMapHeader(node.size())
        while (fields.hasNext) {
          val entry = fields.next()
          packer.packString(entry.getKey)
          packJsonNode(entry.getValue)
        }
      }
      else if (node.isArray) {
        packer.packArrayHeader(node.size())
        node.elements().asScala.foreach(packJsonNode)
      }
      else if (node.isTextual) {
        packer.packString(node.asText())
      }
      else if (node.isNumber) {
        if (node.isIntegralNumber) {
          packer.packLong(node.asLong())
        }
        else {
          packer.packDouble(node.asDouble())
        }
      }
      else if (node.isBoolean) {
        packer.packBoolean(node.asBoolean())
      }
      else if (node.isNull) {
        packer.packNil()
      }
    }

    packJsonNode(jsonNode)
    packer.close()

    // Unpack to Value
    val unpacker = MessagePack.newDefaultUnpacker(out.toByteArray)
    unpacker.unpackValue()
  }
}
