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

import scala.collection.JavaConverters._

import scalaz._, Scalaz._, effect.Effect._

object DistCopyJob {
  val PartSize = "distcopy.part.size"
  val ReadLimit = "distcopy.read.limit"
  val MultipartUploadThreshold = "distcopy.multipart.upload.threshold"
  val RetryCount = "distcopy.retry.count"

  def run(mappings: Mappings, conf: DistCopyConfiguration): RIO[DistCopyStats] = for {
    job <- RIO.safe[Job](Job.getInstance(conf.hdfs))
    ctx <- RIO.safe[MrContext](MrContext.newContext("notion-distcopy-sync", job))
    _   <- RIO.safe[Unit]({
      job.setJobName(ctx.id.value)
      job.setInputFormatClass(classOf[DistCopyInputFormat])
      job.getConfiguration.setBoolean("mapreduce.map.speculative", false)
      job.getConfiguration.setInt("mapreduce.map.maxattempts", 1)
      job.setNumReduceTasks(0)
      job.getConfiguration.setLong(PartSize, conf.partSize.toBytes.value)
      job.getConfiguration.setInt(ReadLimit, conf.readLimit.toBytes.value.toInt)
      job.getConfiguration.setLong(MultipartUploadThreshold, conf.multipartUploadThreshold.toBytes.value)
    })
    _   <- DistCopyInputFormat.setMappings(job, ctx, conf.client, mappings, conf.mappersNumber)
    _   <- RIO.safe[Unit]({
      job.setJarByClass(classOf[DistCopyMapper])
      job.setMapperClass(classOf[DistCopyMapper])
      job.setMapOutputKeyClass(classOf[NullWritable])
      job.setMapOutputValueClass(classOf[NullWritable])
      val tmpout = new Path(ctx.output, "distcopy")
      job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
      FileOutputFormat.setOutputPath(job, tmpout)
    })
    b   <- RIO.safe[Boolean] (job.waitForCompletion(true))
    _   <- RIO.unless(b, RIO.fail("notion dist-copy failed."))
    cnt <- RIO.safe(job.getCounters.getGroup("notion"))
    stats = cnt.iterator().asScala.map(c => c.getName -> c.getValue).toMap
  } yield DistCopyStats(ctx.id.value, stats)

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
  var totalMegabytesDownloaded: Counter = null
  var totalGigabytesDownloaded: Counter = null
  var totalMegabytesUploaded: Counter = null
  var totalGigabytesUploaded: Counter = null
  var totalFilesDownloaded: Counter = null
  var retryCounter: Counter = null
  var partSize: Long = 0
  var readLimit: Int = 0
  var multipartUploadThreshold: Long = 0
  var retryCount: Int = 0

  override def setup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    // get the default mapper parameter values
    val parameters = DistCopyMapperParameters.Default

    partSize =
      context.getConfiguration.getLong(PartSize, parameters.partSize.toBytes.value)
    readLimit =
      context.getConfiguration.getInt(ReadLimit, parameters.readLimit.toBytes.value.toInt)
    multipartUploadThreshold =
      context.getConfiguration.getLong(MultipartUploadThreshold, parameters.multipartUploadThreshold.toBytes.value)
    retryCount =
      context.getConfiguration.getInt(RetryCount, parameters.retryCount)

    // create a transfer manager for all uploads to spare resources
    // don't shut it down after the upload because that shuts down the client
    val configuration = new TransferManagerConfiguration
    configuration.setMinimumUploadPartSize(partSize)
    configuration.setMultipartUploadThreshold(multipartUploadThreshold)
    transferManager = new TransferManager(client)
    transferManager.setConfiguration(configuration)
    totalBytesUploaded = context.getCounter("notion", "total.bytes.uploaded")
    totalMegabytesUploaded = context.getCounter("notion", "total.megabytes.uploaded")
    totalGigabytesUploaded = context.getCounter("notion", "total.gigabytes.uploaded")
    totalFilesUploaded = context.getCounter("notion", "total.files.uploaded")
    totalBytesDownloaded = context.getCounter("notion", "total.bytes.downloaded")
    totalMegabytesDownloaded = context.getCounter("notion", "total.megabytes.downloaded")
    totalGigabytesDownloaded = context.getCounter("notion", "total.gigabytes.downloaded")
    totalFilesDownloaded = context.getCounter("notion", "total.files.downloaded")
    retryCounter = context.getCounter("notion", "total.files.retried")
  }

  override def map(key: NullWritable, value: Mapping, context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    val retryHandler: ((Int, String \&/ Throwable) => S3Action[Unit]) = (n: Int, e: String \&/ Throwable) => S3Action.safe({
      println(s"Retrying ...")
      println(Result.asString(e))
      retryCounter.increment(1)
      context.progress()
      ()
    })

    val action: S3Action[Unit] = value match {
      case DownloadMapping(from, destination) => for {
        _               <- validateDownload(destination, client, context.getConfiguration)
        _               = println(s"Downloading: ${from.render} ===> $destination")
        tmpOutput       = FileOutputFormat.getWorkOutputPath(context)
        tmpDestination  = new Path(tmpOutput.toString + "/" + UUID.randomUUID())
        _               <- (Aws.using(S3Action.safe[FSDataOutputStream](FileSystem.get(context.getConfiguration).create(tmpDestination))) {
          outputStream => from.withStreamMultipart(
              Bytes(partSize)
            , in => {
                val counted = DownloadInputStream(in, i => {
                  totalBytesDownloaded.increment(i)
                  val bytes = totalBytesDownloaded.getValue
                  totalMegabytesDownloaded.setValue(bytes / 1024 / 1024)
                  totalGigabytesDownloaded.setValue(bytes / 1024 / 1024 / 1024)
                })
                Streams.pipe(counted, outputStream)
              }
            , () => context.progress()
          )
        }).retry(retryCount, retryHandler)
        _               = println(s"Moving: $tmpDestination ===> $destination")
        _               <- S3Action.fromRIO(Hdfs.mkdir(destination.getParent).run(context.getConfiguration))
        _               <- S3Action.fromRIO(Hdfs.mv(tmpDestination, destination).run(context.getConfiguration))
        _               = totalFilesDownloaded.increment(1)
      } yield ()

      case UploadMapping(from, destination)   => for {
        _        <- validateUpload(destination, client, context.getConfiguration)
        fs       = FileSystem.get(context.getConfiguration)
        length   <- S3Action.safe[Long]({
          fs.getFileStatus(from).getLen
        })
        metadata = S3.ServerSideEncryption
        _        = metadata.setContentLength(length)
        _        = println(s"Uploading: $from ===> ${destination.render}")
        _        = println(s"\tFile size: ${length / 1024 / 1024}mb")
        // Wrapping FSDataInputStream in BufferedInputStream to fix overflows on reset of the stream
        _        <- (Aws.using(S3Action.safe(new BufferedInputStream(fs.open(from)))) {
          inputStream => {
            def update() = {
              val bytes = totalBytesUploaded.getValue
              totalMegabytesUploaded.setValue(bytes / 1024 / 1024)
              totalGigabytesUploaded.setValue(bytes / 1024 / 1024 / 1024)
            }

            (if (length > partSize) {
              // This should really be handled by `saws`
              println(s"\tRunning multi-part upload")
              destination.putStreamMultiPartWithTransferManager(
                  transferManager
                , inputStream
                , readLimit
                , (i: Long) => {
                    totalBytesUploaded.increment(i)
                    update()
                    context.progress()
                  }
                , metadata
              ).flatMap(upload => S3Action.safe(upload()))
            } else {
              println(s"\tRunning stream upload")
              totalBytesUploaded.increment(length)
              update()
              destination.putStreamWithMetadata(inputStream, readLimit, metadata)
            }).void
          }
        }).retry(retryCount, retryHandler)
        _       = totalFilesUploaded.increment(1)

        } yield ()
    }

    action.execute(client).unsafePerformIO match {
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

  def validateDownload(to: Path, client: AmazonS3Client, conf: Configuration): S3Action[Unit] = {
    // Check no file in target location
    S3Action.fromRIO(Hdfs.exists(to).run(conf).flatMap(
      RIO.when(_,
        RIO.failIO[Unit](s"notion dist-copy download failed - target file exists. ( ${to.toString} )")
      )))
  }

  def validateUpload(to: S3Address, client: AmazonS3Client, conf: Configuration): S3Action[Unit] = {
    // Check no file in target location
    S3Action.fromRIO(to.exists.execute(client).flatMap(
      RIO.when(_,
        RIO.failIO[Unit](s"notion dist-copy upload failed - target file exists. ( ${to.render} )")
      )))
  }


}
