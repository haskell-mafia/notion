package com.ambiata.notion.distcopy

import java.io._
import java.util.UUID

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.com.amazonaws.services.s3.transfer.{TransferManagerConfiguration, TransferManager}
import com.ambiata.mundane.control._
import com.ambiata.mundane.error.Throwables
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr._
import com.ambiata.saws.core._
import com.ambiata.saws.s3.{S3, S3Address}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream, FSDataInputStream}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Job, Counter}

import DistCopyJob._

import scalaz._, Scalaz._, effect._, effect.Effect._

object DistCopyJob {
  val PartSize = "distcopy.part.size"
  val ReadLimit = "distcopy.read.limit"
  val MultipartUploadThreshold = "distcopy.multipart.upload.threshold"
  val RetryCount = "distcopy.retry.count"

  def run(mappings: Mappings, conf: DistCopyConfiguration): ResultTIO[Unit] = for {
    job <- ResultT.safe[IO, Job](Job.getInstance(conf.hdfs))
    ctx <- ResultT.safe[IO, MrContext](MrContext.newContext("notion-distcopy-sync", job))
    _   <- ResultT.safe[IO, Unit]({
      job.setJobName(ctx.id.value)
      job.setInputFormatClass(classOf[DistCopyInputFormat])
      job.getConfiguration.setBoolean("mapreduce.map.speculative", false)
      job.setNumReduceTasks(0)
      job.getConfiguration.setLong(PartSize, conf.partSize.toBytes.value)
      job.getConfiguration.setInt(ReadLimit, conf.readLimit.toBytes.value.toInt)
      job.getConfiguration.setLong(MultipartUploadThreshold, conf.multipartUploadThreshold.toBytes.value)
    })
    _   <- DistCopyInputFormat.setMappings(job, ctx, conf.client, mappings, conf.mappersNumber)
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
  var totalBytesUploaded: Counter = null
  var totalFilesUploaded: Counter = null
  var totalBytesDownloaded: Counter = null
  var totalFilesDownloaded: Counter = null
  var retryCounter: Counter = null
  var partSize: Long = 0
  var readLimit: Int = 0
  var multipartUploadThreshold: Long = 0
  var retryCount: Int = 0

  override def setup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    partSize =
      context.getConfiguration.getLong(PartSize, DistCopyConfiguration.Default.partSize.toBytes.value)
    readLimit =
      context.getConfiguration.getInt(ReadLimit, DistCopyConfiguration.Default.readLimit.toBytes.value.toInt)
    multipartUploadThreshold =
      context.getConfiguration.getLong(MultipartUploadThreshold, DistCopyConfiguration.Default.multipartUploadThreshold.toBytes.value)
    retryCount =
      context.getConfiguration.getInt(RetryCount, DistCopyConfiguration.Default.retryCount)

    // create a transfer manager for all uploads to spare resources
    // don't shut it down after the upload because that shuts down the client
    val configuration = new TransferManagerConfiguration
    configuration.setMinimumUploadPartSize(partSize)
    configuration.setMultipartUploadThreshold(multipartUploadThreshold)
    transferManager = new TransferManager(client)
    transferManager.setConfiguration(configuration)
    totalBytesUploaded = context.getCounter("notion", "total.bytes.uploaded")
    totalFilesUploaded = context.getCounter("notion", "total.files.uploaded")
    totalBytesDownloaded = context.getCounter("notion", "total.bytes.downloaded")
    totalFilesDownloaded = context.getCounter("notion", "total.files.downloaded")
    retryCounter = context.getCounter("notion", "total.files.retried")
  }

  override def map(key: NullWritable, value: Mapping, context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    val action: S3Action[Unit] = value match {
      case DownloadMapping(from, destination) => for {
        _               <- validateDownload(from, destination, client, context.getConfiguration)
        tmpOutput       = FileOutputFormat.getWorkOutputPath(context)
        tmpDestination  = new Path(tmpOutput.toString + "/" + UUID.randomUUID())
        _               <- Aws.using(S3Action.safe[FSDataOutputStream](FileSystem.get(context.getConfiguration).create(tmpDestination))) {
          outputStream => from.withStreamMultipart(
              Bytes(partSize)
            , in => {
                val counted = DownloadInputStream(in, i => totalBytesDownloaded.increment(i))
                Streams.pipe(counted, outputStream)
              }
            , () => context.progress()
          )
        }
        _               <- S3Action.fromResultT(Hdfs.mv(tmpDestination, destination).run(context.getConfiguration))
        _               = totalFilesDownloaded.increment(1)
      } yield ()

      case UploadMapping(from, destination)   => for {
        _        <- validateUpload(from, destination, client, context.getConfiguration)
        fs       = FileSystem.get(context.getConfiguration)
        length   <- S3Action.safe[Long]({
          fs.getFileStatus(from).getLen
        })
        metadata = S3.ServerSideEncryption
        _        = metadata.setContentLength(length)

        _        <- Aws.using(S3Action.safe[FSDataInputStream](fs.open(from))) {
          inputStream =>
          (if (length > partSize) {
          // This should really be handled by `saws`
            destination.putStreamMultiPartWithTransferManager(
                transferManager
              , inputStream
              , readLimit
              , (i: Long) => {
                  totalBytesUploaded.increment(i)
                  context.progress()
                }
              , metadata
              ).map( upload => upload()
            )
          } else {
            destination.putStreamWithMetadata(inputStream, metadata)
          }).void
        }
        _       = totalFilesUploaded.increment(1)
      } yield ()
    }

    val retryHandler = (n: Int, e: String \&/ Throwable) => {
      retryCounter.increment(1)
      context.progress()
      Vector()
    }

    action.retry(retryCount, retryHandler).execute(client).unsafePerformIO() match {
      case Error(e) =>
        sys.error(Result.asString(e))
      case Ok(_) =>
        ()
    }
  }

  override def cleanup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    super.cleanup(context)
    transferManager.shutdownNow()
  }

  def validateDownload(from: S3Address, to: Path, client: AmazonS3Client, conf: Configuration): S3Action[Unit] = {
    // Check source file exists
    S3Action.fromResultT(from.exists.executeT(client).flatMap(
      ResultT.unless(_,
        ResultT.failIO[Unit](s"notion dist-copy download failed - no source file. ( ${from.render} )")
      )) >>
    // Check no file in target location
    Hdfs.exists(to).run(conf).flatMap(
      ResultT.when(_,
        ResultT.failIO[Unit](s"notion dist-copy download failed - target file exists. ( ${to.toString} )")
      )))
  }

  def validateUpload(from: Path, to: S3Address, client: AmazonS3Client, conf: Configuration): S3Action[Unit] = {
    // Check source file exists
    S3Action.fromResultT(Hdfs.exists(from).run(conf).flatMap(
      ResultT.unless(_,
        ResultT.failIO[Unit](s"notion dist-copy upload failed - no source file. ( ${from.toString} )")
      )) >>
    // Check no file in target location
    to.exists.executeT(client).flatMap(
      ResultT.when(_,
        ResultT.failIO[Unit](s"notion dist-copy upload failed - target file exists. ( ${to.render} )")
      )))
  }


}
