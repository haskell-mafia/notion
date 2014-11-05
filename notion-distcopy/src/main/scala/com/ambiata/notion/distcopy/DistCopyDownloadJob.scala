package com.ambiata.notion.distcopy

import java.util.UUID

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.error.Throwables
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr._
import com.ambiata.saws.core.Clients
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Job}
import MemoryConversions._

import scalaz.effect.IO

object DistCopyDownloadJob {
  def run(conf: Configuration, mappings: DownloadMappings, mappersNumber: Int): ResultTIO[Unit] = for {
    job <- ResultT.safe[IO, Job](Job.getInstance(conf))
    ctx <- ResultT.safe[IO, MrContext](MrContext.newContext("notion-distcopy-download", job))
    _   <- ResultT.safe[IO, Unit]({
      job.setJobName(ctx.id.value)
      job.setInputFormatClass(classOf[S3InputFormat])
      job.getConfiguration.setBoolean("mapreduce.map.speculative", false)
    })

    _   <- S3InputFormat.setMappings(job, ctx, mappings, mappersNumber)

    _   <- ResultT.safe[IO, Unit]({
      job.setJarByClass(classOf[DistCopyDownloadMapper])
      job.setMapperClass(classOf[DistCopyDownloadMapper])
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
class DistCopyDownloadMapper extends Mapper[NullWritable, DownloadMapping, NullWritable, NullWritable] {
  override def map(key: NullWritable, value: DownloadMapping, context: Mapper[NullWritable, DownloadMapping, NullWritable, NullWritable]#Context): Unit = {
    val tmpOutput: Path = FileOutputFormat.getWorkOutputPath(context)

    val tmpDestination: Path = new Path(tmpOutput.toString + "/" + UUID.randomUUID())
    val destination = value.to

    val client = Clients.s3

    // Check source file exists
    value.from.s3.exists.execute(client).unsafePerformIO() match {
      case Error(e) => sys.error(Result.asString(e))
      case Ok(false) => sys.error("notion dist-copy failed - no source file.")
      case Ok(true) => ()
    }

    // Check no file in target location
    Hdfs.exists(value.to).run(context.getConfiguration).run.unsafePerformIO() match {
      case Error(e) => sys.error(Result.asString(e))
      case Ok(true) => sys.error("notion dist-copy failed - target file exists.")
      case Ok(false) => ()
    }

    val outputStream = FileSystem.get(context.getConfiguration).create(tmpDestination)
    try {

      val action = value.from.s3.withStreamMultipart(32.mb, in => Streams.pipe(in, outputStream.getWrappedStream), () => context.progress())

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
  }
}
