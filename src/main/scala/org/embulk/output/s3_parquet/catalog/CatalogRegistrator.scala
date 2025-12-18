package org.embulk.output.s3_parquet.catalog

import java.util.{Optional, Map => JMap}

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.config.ConfigException
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.output.s3_parquet.implicits
import org.embulk.spi.{Schema, Column => EmbulkColumn}
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.glue.model._

import scala.jdk.CollectionConverters._
import scala.util.Try

object CatalogRegistrator {

  trait Task extends org.embulk.util.config.Task {
    @Config("catalog_id")
    @ConfigDefault("null")
    def getCatalogId: Optional[String]

    @Config("database")
    def getDatabase: String

    @Config("table")
    def getTable: String

    @Config("column_options")
    @ConfigDefault("{}")
    def getColumnOptions: JMap[String, ColumnOption]

    @Config("operation_if_exists")
    @ConfigDefault("\"delete\"")
    def getOperationIfExists: String
  }

  trait ColumnOption {
    @Config("type")
    def getType: String
  }

  import implicits._

  def fromTask(
      task: CatalogRegistrator.Task,
      aws: Aws,
      schema: Schema,
      location: String,
      compressionCodec: CompressionCodecName,
      defaultGlueTypes: Map[EmbulkColumn, GlueDataType] = Map.empty
  ): CatalogRegistrator =
    CatalogRegistrator(
      aws = aws,
      catalogId = task.getCatalogId,
      database = task.getDatabase,
      table = task.getTable,
      operationIfExists = task.getOperationIfExists,
      location = location,
      compressionCodec = compressionCodec,
      schema = schema,
      columnOptions = task.getColumnOptions,
      defaultGlueTypes = defaultGlueTypes
    )
}

case class CatalogRegistrator(
    aws: Aws,
    catalogId: Option[String] = None,
    database: String,
    table: String,
    operationIfExists: String,
    location: String,
    compressionCodec: CompressionCodecName,
    schema: Schema,
    columnOptions: Map[String, CatalogRegistrator.ColumnOption],
    defaultGlueTypes: Map[EmbulkColumn, GlueDataType] = Map.empty
) {

  import implicits._

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[CatalogRegistrator])

  def run(): Unit = {
    if (doesTableExists()) {
      operationIfExists match {
        case "skip" =>
          logger.info(
            s"Skip to register the table: ${database}.${table}"
          )
          return

        case "delete" =>
          logger.info(s"Delete the table: ${database}.${table}")
          deleteTable()

        case unknown =>
          throw new ConfigException(s"Unsupported operation: $unknown")
      }
    }
    registerNewParquetTable()
    showNewTableInfo()
  }

  def showNewTableInfo(): Unit = {
    val requestBuilder = GetTableRequest
      .builder()
      .databaseName(database)
      .name(table)

    catalogId.foreach(requestBuilder.catalogId)

    val t = aws.withGlue(_.getTable(requestBuilder.build())).table()
    logger.info(s"Created a table: ${t.toString}")
  }

  def doesTableExists(): Boolean = {
    val requestBuilder = GetTableRequest
      .builder()
      .databaseName(database)
      .name(table)

    catalogId.foreach(requestBuilder.catalogId)

    Try(aws.withGlue(_.getTable(requestBuilder.build()))).isSuccess
  }

  def deleteTable(): Unit = {
    val requestBuilder = DeleteTableRequest
      .builder()
      .databaseName(database)
      .name(table)

    catalogId.foreach(requestBuilder.catalogId)

    aws.withGlue(_.deleteTable(requestBuilder.build()))
  }

  def registerNewParquetTable(): Unit = {
    logger.info(s"Create a new table: ${database}.${table}")

    val tableInputBuilder = TableInput
      .builder()
      .name(table)
      .description("Created by embulk-output-s3_parquet")
      .tableType("EXTERNAL_TABLE")
      .parameters(
        Map(
          "EXTERNAL" -> "TRUE",
          "classification" -> "parquet",
          "parquet.compression" -> compressionCodec.name()
        ).asJava
      )
      .storageDescriptor(
        StorageDescriptor
          .builder()
          .columns(getGlueSchema.asJava)
          .location(location)
          .compressed(isCompressed)
          .inputFormat(
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
          )
          .outputFormat(
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
          )
          .serdeInfo(
            SerDeInfo
              .builder()
              .serializationLibrary(
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
              )
              .parameters(Map("serialization.format" -> "1").asJava)
              .build()
          )
          .build()
      )

    val requestBuilder = CreateTableRequest
      .builder()
      .databaseName(database)
      .tableInput(tableInputBuilder.build())

    catalogId.foreach(requestBuilder.catalogId)

    aws.withGlue(_.createTable(requestBuilder.build()))
  }

  private def getGlueSchema: Seq[Column] = {
    schema.getColumns.map { c: EmbulkColumn =>
      Column
        .builder()
        .name(c.getName)
        .`type`(
          columnOptions
            .get(c.getName)
            .map(_.getType)
            .getOrElse(defaultGlueTypes(c).name)
        )
        .build()
    }
  }

  private def isCompressed: Boolean = {
    !compressionCodec.equals(CompressionCodecName.UNCOMPRESSED)
  }

}
