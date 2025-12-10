package org.embulk.output.s3_parquet

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.parquet.hadoop.ParquetWriter
import org.embulk.config.TaskReport
import org.embulk.output.s3_parquet.aws.Aws
import org.embulk.spi.{Page, PageReader, TransactionalPageOutput}
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.transfer.s3.model.{
  CompletedFileUpload,
  FileUpload,
  UploadFileRequest
}

case class S3ParquetPageOutput(
    outputLocalFile: String,
    reader: PageReader,
    writer: ParquetWriter[PageReader],
    aws: Aws,
    destBucket: String,
    destKey: String
) extends TransactionalPageOutput {

  private var isClosed: Boolean = false

  override def add(page: Page): Unit = {
    reader.setPage(page)
    while (reader.nextRecord()) {
      ContextClassLoaderSwapper.usingPluginClass {
        writer.write(reader)
      }
    }
  }

  override def finish(): Unit = {}

  override def close(): Unit = {
    synchronized {
      if (!isClosed) {
        ContextClassLoaderSwapper.usingPluginClass {
          writer.close()
        }
        isClosed = true
      }
    }
  }

  override def abort(): Unit = {
    close()
    cleanup()
  }

  override def commit(): TaskReport = {
    close()
    val result: CompletedFileUpload =
      ContextClassLoaderSwapper.usingPluginClass {
        aws.withTransferManager { transferManager =>
          val uploadRequest = UploadFileRequest
            .builder()
            .putObjectRequest(
              PutObjectRequest
                .builder()
                .bucket(destBucket)
                .key(destKey)
                .build()
            )
            .source(Paths.get(outputLocalFile))
            .build()

          val fileUpload: FileUpload = transferManager.uploadFile(uploadRequest)
          fileUpload.completionFuture().join()
        }
      }
    cleanup()

    val response = result.response()
    PluginTask.CONFIG_MAPPER_FACTORY
      .newTaskReport()
      .set("bucket", destBucket)
      .set("key", destKey)
      .set("etag", response.eTag())
      .set(
        "version_id",
        if (response.versionId() != null) response.versionId() else ""
      )
  }

  private def cleanup(): Unit = {
    Files.delete(Paths.get(outputLocalFile))
  }
}
