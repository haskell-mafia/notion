package com.ambiata.notion.distcopy

import java.util.UUID

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.com.amazonaws.services.s3.transfer.{TransferManagerConfiguration, TransferManager}
import com.ambiata.mundane.control._
import com.ambiata.mundane.error.Throwables
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.{S3, S3Address}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream, FSDataInputStream}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Job}
import MemoryConversions._

import scalaz._, Scalaz._, effect._, effect.Effect._

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

  override def map(key: NullWritable, value: Mapping, context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit =
    (value match {
      case DownloadMapping(from, destination) => for {
        _               <- validateDownload(from, destination, client, context.getConfiguration)
        tmpOutput       = FileOutputFormat.getWorkOutputPath(context)
        tmpDestination  = new Path(tmpOutput.toString + "/" + UUID.randomUUID())
        _               <- ResultT.using(ResultT.safe[IO, FSDataOutputStream](FileSystem.get(context.getConfiguration).create(tmpDestination))) {
          outputStream => from.withStreamMultipart(32.mb, in => Streams.pipe(in, outputStream), () => context.progress()).executeT(client)
        }
        _               <- Hdfs.mv(tmpDestination, destination).run(context.getConfiguration)
      } yield ()

      case UploadMapping(from, destination)   => for {
        _        <- validateUpload(from, destination, client, context.getConfiguration)
        fs       = FileSystem.get(context.getConfiguration)
        length   <- ResultT.safe[IO, Long]({
          fs.getFileStatus(from).getLen
        })
        metadata = S3.ServerSideEncryption
        _        = metadata.setContentLength(length)
        input    <- ResultT.safe[IO, FSDataInputStream](fs.open(from))

          // This should really be handled by `saws`
        _        <- (if (length > 10.mb.toBytes.value) {
              destination.putStreamMultiPartWithTransferManager(
                  transferManager
                , input
                , () => context.progress()
                , metadata)
                .map( upload => upload() )
            } else {
              destination.putStreamWithMetadata(input, metadata)
        }).executeT(client)
      } yield ()
    }).run.unsafePerformIO() match {
      case Error(e) =>
        sys.error(Result.asString(e))
      case Ok(_) =>
        ()
    }

  override def cleanup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    super.cleanup(context)
    transferManager.shutdownNow()
  }

  def validateDownload(from: S3Address, to: Path, client: AmazonS3Client, conf: Configuration): ResultTIO[Unit] = {
    // Check source file exists
    from.exists.executeT(client).flatMap(
      ResultT.unless(_,
        ResultT.failIO[Unit](s"notion dist-copy failed - no source file. ( ${from.render} )")
      )) >>
    // Check no file in target location
    Hdfs.exists(to).run(conf).flatMap(
      ResultT.when(_,
        ResultT.failIO[Unit](s"notion dist-copy failed - target file exists. ( ${to.toString} )")
      ))
  }

  def validateUpload(from: Path, to: S3Address, client: AmazonS3Client, conf: Configuration): ResultTIO[Unit] = {
    // Check source file exists
    Hdfs.exists(from).run(conf).flatMap(
      ResultT.unless(_,
        ResultT.failIO[Unit](s"notion dist-copy failed - no source file. ( ${from.toString} )")
      )) >>
    // Check no file in target location
    to.exists.executeT(client).flatMap(
      ResultT.when(_,
        ResultT.failIO[Unit](s"notion dist-copy failed - target file exists. ( ${to.render} )")
      ))
  }
}
