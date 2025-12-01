package org.embulk.output.s3_parquet

import java.util.{Locale, MissingFormatArgumentException, Optional}

import com.amazonaws.services.s3.model.CannedAccessControlList
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.embulk.util.config.{
  Config,
  ConfigDefault,
  ConfigMapper,
  ConfigMapperFactory,
  Task,
  TaskMapper
}
import org.embulk.config.{ConfigException, ConfigSource, DataSource, TaskSource}
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.output.s3_parquet.catalog.CatalogRegistrator
import org.embulk.output.s3_parquet.parquet.ParquetFileWriteSupport

trait PluginTask extends Task with ParquetFileWriteSupport.Task with Aws.Task {

  @Config("bucket")
  def getBucket: String

  @Config("path_prefix")
  @ConfigDefault("\"\"")
  def getPathPrefix: String

  @Config("sequence_format")
  @ConfigDefault("\"%03d.%02d.\"")
  def getSequenceFormat: String

  @Config("file_ext")
  @ConfigDefault("\"parquet\"")
  def getFileExt: String

  @Config("compression_codec")
  @ConfigDefault("\"uncompressed\"")
  def getCompressionCodecString: String

  def getCompressionCodec: CompressionCodecName
  def setCompressionCodec(v: CompressionCodecName): Unit

  @Config("canned_acl")
  @ConfigDefault("\"private\"")
  def getCannedAclString: String

  def getCannedAcl: CannedAccessControlList
  def setCannedAcl(v: CannedAccessControlList): Unit

  @Config("block_size")
  @ConfigDefault("null")
  def getBlockSize: Optional[Int]

  @Config("page_size")
  @ConfigDefault("null")
  def getPageSize: Optional[Int]

  @Config("max_padding_size")
  @ConfigDefault("null")
  def getMaxPaddingSize: Optional[Int]

  @Config("enable_dictionary_encoding")
  @ConfigDefault("null")
  def getEnableDictionaryEncoding: Optional[Boolean]

  @Config("buffer_dir")
  @ConfigDefault("null")
  def getBufferDir: Optional[String]

  @Config("catalog")
  @ConfigDefault("null")
  def getCatalog: Optional[CatalogRegistrator.Task]
}

object PluginTask {

  private val CONFIG_MAPPER_FACTORY: ConfigMapperFactory =
    ConfigMapperFactory.builder().addDefaultModules().build()

  def loadConfig(config: ConfigSource): PluginTask = {
    val configMapper: ConfigMapper = CONFIG_MAPPER_FACTORY.createConfigMapper()
    val task = configMapper.map(config, classOf[PluginTask])
    // sequence_format
    try task.getSequenceFormat.format(0, 0)
    catch {
      case e: MissingFormatArgumentException =>
        throw new ConfigException(
          s"Invalid sequence_format: ${task.getSequenceFormat}",
          e
        )
    }

    // compression_codec
    CompressionCodecName
      .values()
      .find(
        _.name()
          .toLowerCase(Locale.ENGLISH)
          .equals(task.getCompressionCodecString)
      ) match {
      case Some(v) => task.setCompressionCodec(v)
      case None =>
        val unsupported: String = task.getCompressionCodecString
        val supported: String = CompressionCodecName
          .values()
          .map(v => s"'${v.name().toLowerCase}'")
          .mkString(", ")
        throw new ConfigException(
          s"'$unsupported' is unsupported: `compression_codec` must be one of [$supported]."
        )
    }

    // canned_acl
    CannedAccessControlList
      .values()
      .find(_.toString.equals(task.getCannedAclString)) match {
      case Some(v) => task.setCannedAcl(v)
      case None =>
        val unsupported: String = task.getCannedAclString
        val supported: String = CannedAccessControlList
          .values()
          .map(v => s"'${v.toString}'")
          .mkString(", ")
        throw new ConfigException(
          s"'$unsupported' is unsupported: `canned_acl` must be one of [$supported]."
        )
    }

    ParquetFileWriteSupport.configure(task)
    task
  }

  def loadTask(taskSource: TaskSource): PluginTask = {
    val taskMapper: TaskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper()
    taskMapper.map(taskSource, classOf[PluginTask])
  }

  def dumpTask(task: PluginTask): TaskSource = {
    // Use reflection to copy all getter values to TaskSource
    val taskSource = CONFIG_MAPPER_FACTORY.newTaskSource()

    // Get all methods from the task interface
    val methods = task.getClass.getMethods

    methods.foreach { method =>
      val methodName = method.getName
      // Find getter methods (starts with "get" and has no parameters)
      if (methodName.startsWith("get") && method.getParameterCount == 0 && methodName != "getClass") {
        try {
          val value = method.invoke(task)
          if (value != null) {
            // Convert getter name to property name (e.g., getBucket -> bucket)
            val propertyName =
              methodName.substring(3, 4).toLowerCase + methodName.substring(4)
            taskSource.set(propertyName, value)
          }
        }
        catch {
          case _: Exception => // Ignore methods that can't be invoked
        }
      }
    }

    taskSource
  }

  def getConfigMapperFactory: ConfigMapperFactory = CONFIG_MAPPER_FACTORY

}
