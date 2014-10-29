package com.ambiata.notion.distcopy

import java.io.FileInputStream
import java.util.UUID

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.{TransferManagerConfiguration, TransferManager}
import com.ambiata.mundane.control._
import com.ambiata.mundane.error.Throwables
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr._
import com.ambiata.saws.core.{S3Action, Clients}
import com.ambiata.saws.s3.{S3, S3Address}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Job}
import MemoryConversions._

import scalaz.effect.IO

object DistCopyJob {
  def run(conf: Configuration, client: AmazonS3Client, mappings: Mappings, mappersNumber: Int): ResultTIO[Unit] = for {
    job <- ResultT.safe[IO, Job](Job.getInstance(conf))
    ctx <- ResultT.safe[IO, MrContext](MrContext.newContext("notion-distcopy-sync", job))
    _   <- ResultT.safe[IO, Unit]({
      job.setJobName(ctx.id.value)
      job.setInputFormatClass(classOf[DistCopyInputFormat])
      job.getConfiguration.setBoolean("mapreduce.map.speculative", false)
      job.setNumReduceTasks(0)
    })

    _   <- DistCopyInputFormat.setMappings(job, ctx, client, mappings, mappersNumber)

    _   <- ResultT.safe[IO, Unit]({
      job.setJarByClass(classOf[DistCopyMapper])
      job.setMapperClass(classOf[DistCopyMapper])
      job.setMapOutputKeyClass(classOf[NullWritable])
      job.setMapOutputValueClass(classOf[NullWritable])
      val tmpout = new Path(ctx.output, "distcopy")
      job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
      FileOutputFormat.setOutputPath(job, tmpout)
    })
    b   <- ResultT.safe[IO, Boolean] (job.waitForCompletion(true))
    _   <- ResultT.unless[IO](b, ResultT.fail("notion dist-copy failed."))
  } yield ()
}


/*
  Validation is delayed until the mappers so that the client is not required to pre-compute the
  validity of all workloads. A faster-fail check can be added in downstream projects when possible
 */
class DistCopyMapper extends Mapper[NullWritable, Mapping, NullWritable, NullWritable] {
  private var transferManager: TransferManager = null
  val client = Clients.s3

  override def setup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    // create a transfer manager for all uploads to spare resources
    // don't shut it down after the upload because that shuts down the client
    val configuration = new TransferManagerConfiguration
    configuration.setMinimumUploadPartSize(10.mb.toBytes.value)
    configuration.setMultipartUploadThreshold(100.mb.toBytes.value.toInt)
    transferManager = new TransferManager(client)
    transferManager.setConfiguration(configuration)
  }

  override def map(key: NullWritable, value: Mapping, context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    value match {
      case DownloadMapping(from, destination) =>
        validateDownload(from, destination, client, context.getConfiguration)
        val tmpOutput: Path = FileOutputFormat.getWorkOutputPath(context)
        val tmpDestination: Path = new Path(tmpOutput.toString + "/" + UUID.randomUUID())
        val outputStream = FileSystem.get(context.getConfiguration).create(tmpDestination)
        try {
          val action = from.withStreamMultipart(32.mb, in => Streams.pipe(in, outputStream.getWrappedStream), () => context.progress())
          val result = action.execute(client).unsafePerformIO() match {
            case Error(e) => sys.error(Result.asString(e))
            case Ok(_) =>
              Hdfs.mv(tmpDestination, destination).run(context.getConfiguration).run.unsafePerformIO()
          }

        } catch {
          case t: Throwable =>
            sys.error(Throwables.renderWithStack(t))
        } finally {
          context.progress()
          outputStream.close()
        }
      case UploadMapping(from, destination)   =>
        validateUpload(from, destination, client, context.getConfiguration)

        try {
          val fs = FileSystem.get(context.getConfiguration)
          val inputStream = fs.open(from)
          val metadata = S3.ServerSideEncryption
          val length = fs.getFileStatus(from).getLen
          metadata.setContentLength(length)

          // This should really be handled by `saws`
          val uploadAction = {
            if (length > 10.mb.toBytes.value) {
              destination.putStreamMultiPartWithTransferManager(
                  transferManager
                , inputStream.getWrappedStream
                , () => context.progress()
                , metadata)
                .map( upload => upload() )
            } else
              destination.putStreamWithMetadata(inputStream, metadata)
          }

          val result = uploadAction.execute(client).unsafePerformIO() match {
            case Error(e) => sys.error(Result.asString(e))
            case Ok(_) =>
              ()
          }
        } catch {
          case t: Throwable =>
            sys.error(Throwables.renderWithStack(t))
        } finally {
          context.progress()
        }
    }
  }

  override def cleanup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    super.cleanup(context)
    transferManager.shutdownNow()
  }

  def validateDownload(from: S3Address, to: Path, client: AmazonS3Client, conf: Configuration): Unit = {
    // Check source file exists
    from.exists.execute(client).unsafePerformIO() match {
      case Error(e) => sys.error(Result.asString(e))
      case Ok(false) => sys.error(s"notion dist-copy failed - no source file. ( ${from.render} )")
      case Ok(true) => ()
    }
    // Check no file in target location
    Hdfs.exists(to).run(conf).run.unsafePerformIO() match {
      case Error(e) => sys.error(Result.asString(e))
      case Ok(true) => sys.error(s"notion dist-copy failed - target file exists. ( ${to.toString} )")
      case Ok(false) => ()
    }
  }

  def validateUpload(from: Path, to: S3Address, client: AmazonS3Client, conf: Configuration): Unit = {
    // Check source file exists
    Hdfs.exists(from).run(conf).run.unsafePerformIO() match {
      case Error(e) => sys.error(Result.asString(e))
      case Ok(false) => sys.error(s"notion dist-copy failed - no source file. ( ${from.toString} )")
      case Ok(true) => ()
    }
    // Check no file in target location
    to.exists.execute(client).unsafePerformIO() match {
      case Error(e) => sys.error(Result.asString(e))
      case Ok(true) => sys.error(s"notion dist-copy failed - target file exists. ( ${to.render} )")
      case Ok(false) => ()
    }
  }
}
