package com.ambiata.notion.distcopy

import java.io._
import java.util.UUID

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.com.amazonaws.services.s3.transfer.{TransferManagerConfiguration, TransferManager}
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.HdfsPath
import com.ambiata.poacher.mr._
import com.ambiata.saws.core._
import com.ambiata.saws.s3.{S3, S3Address}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Job, Counter}
import DistCopyStats._
import DistCopyJob._
import org.joda.time._
import scala.collection.JavaConverters._
import scalaz._, Scalaz._, effect.Effect._

object DistCopyJob {
  val PartSize = "distcopy.part.size"
  val ReadLimit = "distcopy.read.limit"
  val MultipartUploadThreshold = "distcopy.multipart.upload.threshold"
  val RetryCount = "distcopy.retry.count"
  val CrossValidate = "distcopy.cross.validate"

  // additional counters for reading
  // values on the hadoop console interactively
  val UPLOADED_MBYTES   = "uploaded.mb"
  val UPLOADED_GBYTES   = "uploaded.gb"
  val DOWNLOADED_MBYTES = "downloaded.mb"
  val DOWNLOADED_GBYTES = "downloaded.gb"


  def run(mappings: Mappings, conf: DistCopyConfiguration): RIO[Option[DistCopyStats]] =
    if (mappings.mappings.length == 0) RIO.ok(None)
    else for {
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
        job.getConfiguration.setBoolean(CrossValidate, conf.parameters.crossValidate)
      })
      n   = Math.min(mappings.mappings.length, conf.mappersNumber)
      _   <- DistCopyInputFormat.setMappings(job, ctx, conf.client, mappings, n)
      _   <- RIO.safe[Unit]({
        job.setJarByClass(classOf[DistCopyMapper])
        job.setMapperClass(classOf[DistCopyMapper])
        job.setMapOutputKeyClass(classOf[NullWritable])
        job.setMapOutputValueClass(classOf[NullWritable])
        val tmpout = new Path(ctx.output.toHPath, "distcopy")
        job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
        FileOutputFormat.setOutputPath(job, tmpout)
      })
      b   <- RIO.safe[Boolean] (job.waitForCompletion(true))
      _   <- RIO.unless(b, RIO.fail("notion dist-copy failed."))
      cnt <- RIO.safe(job.getCounters.getGroup("notion"))
      stats = cnt.iterator().asScala.map(c => c.getName -> c.getValue).toMap
    } yield Some(DistCopyStats(ctx.id.value, stats, DateTime.now))
}

/*
  Validation is delayed until the mappers so that the client is not required to pre-compute the
  validity of all workloads. A faster-fail check can be added in downstream projects when possible
 */
class DistCopyMapper extends Mapper[NullWritable, Mapping, NullWritable, NullWritable] {
  private var transferManager: TransferManager = null
  val client = Clients.s3
  var totalBytesUploaded: Counter = null
  var totalMegaBytesUploaded: Counter = null
  var totalGigaBytesUploaded: Counter = null
  var totalBytesDownloaded: Counter = null
  var totalMegaBytesDownloaded: Counter = null
  var totalGigaBytesDownloaded: Counter = null
  var totalFilesUploaded: Counter = null
  var totalFilesDownloaded: Counter = null
  var retryCounter: Counter = null
  var partSize: Long = 0
  var readLimit: Int = 0
  var multipartUploadThreshold: Long = 0
  var retryCount: Int = 0
  var crossValidate: Boolean = true

  override def setup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    // get the default mapper parameter values
    val parameters = DistCopyMapperParameters.Default

    crossValidate =
      context.getConfiguration.getBoolean(CrossValidate, true)
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
    totalBytesUploaded        = context.getCounter("notion", UPLOADED_BYTES)
    totalMegaBytesUploaded    = context.getCounter("notion", UPLOADED_MBYTES)
    totalGigaBytesUploaded    = context.getCounter("notion", UPLOADED_GBYTES)
    totalBytesDownloaded      = context.getCounter("notion", DOWNLOADED_BYTES)
    totalMegaBytesDownloaded  = context.getCounter("notion", DOWNLOADED_MBYTES)
    totalGigaBytesDownloaded  = context.getCounter("notion", DOWNLOADED_GBYTES)
    totalFilesUploaded        = context.getCounter("notion", UPLOADED_FILES)
    totalFilesDownloaded      = context.getCounter("notion", DOWNLOADED_FILES)
    retryCounter              = context.getCounter("notion", RETRIED_FILES)
  }

  override def map(key: NullWritable, value: Mapping, context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    val retryHandler: ((Int, String \&/ Throwable) => S3Action[Unit]) = (n: Int, e: String \&/ Throwable) => S3Action.safe({
      println(s"Retrying ...")
      println(Result.asString(e))
      retryCounter.increment(1)
      context.progress()
      Thread.sleep(200 * (math.pow(2, retryCount - n)))
      ()
    })

    val action: S3Action[Unit] = value match {
      case DownloadMapping(from, destination) => for {
        _               <- if (crossValidate) validateDownload(destination, client, context.getConfiguration) else ().pure[S3Action]
        _               = println(s"Downloading: ${from.render} ===> $destination")
        tmpOutput       = FileOutputFormat.getWorkOutputPath(context)
        tmpDestination  = new Path(tmpOutput.toString + "/" + UUID.randomUUID())
        _               <- Aws.using(S3Action.safe[FSDataOutputStream](FileSystem.get(context.getConfiguration).create(tmpDestination))) {
          outputStream => from.withStreamMultipart(
            Bytes(partSize)
            , in => {
              val counted = DownloadInputStream(in, i => incrementCounters(totalBytesDownloaded, totalMegaBytesDownloaded, totalGigaBytesDownloaded, i))
              Streams.pipe(counted, outputStream)
            }
            , () => context.progress()
          )
        }.retry(retryCount, retryHandler)
        _               = println(s"Moving: $tmpDestination ===> $destination")
        _               <- S3Action.fromRIO((destination.dirname.mkdirs >> HdfsPath.fromPath(tmpDestination).move(destination)).run(context.getConfiguration))
        _               = totalFilesDownloaded.increment(1)
      } yield ()

      case UploadMapping(from, destination)   => for {
        _        <- if (crossValidate) validateUpload(destination, client, context.getConfiguration) else ().pure[S3Action]
        fs       = FileSystem.get(context.getConfiguration)
        length   <- S3Action.safe[Long]({
          fs.getFileStatus(from.toHPath).getLen
        })
        metadata = S3.ServerSideEncryption
        _        = metadata.setContentLength(length)
        _        = println(s"Uploading: $from ===> ${destination.render}")
        _        = println(s"\tFile size: ${length / 1024 / 1024}mb")
        // Wrapping FSDataInputStream in BufferedInputStream to fix overflows on reset of the stream
        _        <- (Aws.using(S3Action.safe(new BufferedInputStream(fs.open(from.toHPath)))) {
          inputStream => {
            (if (length > partSize) {
              // This should really be handled by `saws`
              println(s"\tRunning multi-part upload")
              destination.putStreamMultiPartWithTransferManager(
                transferManager
                , inputStream
                , readLimit
                , (i: Long) => {
                  incrementCounters(totalBytesUploaded, totalMegaBytesUploaded, totalGigaBytesUploaded, i)
                  context.progress()
                }
                , metadata
              ).flatMap(upload => S3Action.safe(upload()))
            } else {
              println(s"\tRunning stream upload")
              incrementCounters(totalBytesUploaded, totalMegaBytesUploaded, totalGigaBytesUploaded, length)
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

  def incrementCounters(bytesCounter: Counter, mbCounter: Counter, gbCounter: Counter, increment: Long): Unit = {
    bytesCounter.increment(increment)
    val bytes = bytesCounter.getValue
    mbCounter.setValue(bytes / 1024 / 1024)
    gbCounter.setValue(bytes / 1024 / 1024 / 1024)
  }

  override def cleanup(context: Mapper[NullWritable, Mapping, NullWritable, NullWritable]#Context): Unit = {
    super.cleanup(context)
    transferManager.shutdownNow()
  }

  def validateDownload(to: HdfsPath, client: AmazonS3Client, conf: Configuration): S3Action[Unit] = {
    // Check no file in target location
    S3Action.fromRIO(to.exists.run(conf).flatMap(
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
