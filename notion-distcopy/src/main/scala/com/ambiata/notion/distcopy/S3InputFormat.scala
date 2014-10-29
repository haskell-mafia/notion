package com.ambiata.notion.distcopy


import com.ambiata.mundane.control.{ResultT, ResultTIO}
import com.ambiata.poacher.mr.{DistCache, MrContext}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._

import argonaut.{Context => _, _}, Argonaut._

import scalaz._, effect._

import scala.collection.JavaConverters._


/**
 * Create splits for a list of files to download by create FilesSplits of roughly equal sizes
 */
class S3InputFormat extends InputFormat[NullWritable, DownloadMapping] {
  def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val workloads = context.getConfiguration.get("notion.dist-copy.workloads").decodeOption[Workloads].getOrElse(
      sys.error("notion dist-copy failed - `notion.dist-copy.workloads` not set"))

    workloads.workloads.map(workload =>
      new S3Split(workload): InputSplit
    ).asJava
  }

  /**
   * A RecordReader iterates through the files of a FilesSplit and enumerates them as FilePathInfo objects
   */
  def createRecordReader(inputSplit: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, DownloadMapping] = {
    val split: S3Split = inputSplit.asInstanceOf[S3Split]
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val mappings = ctx.distCache.pop(context.getConfiguration, S3InputFormat.MappingKey, bytes => new String(bytes, "UTF-8").decodeEither[DownloadMappings])
    new RecordReader[NullWritable, DownloadMapping] {
      var index = -1

      def initialize(split: InputSplit, context: TaskAttemptContext) = {
      }

      def nextKeyValue: Boolean = {
        index += 1
        index < split.workload.indexes.size
      }

      def getCurrentKey: NullWritable =
        NullWritable.get

      def getCurrentValue: DownloadMapping =
        mappings.mappings(split.workload.indexes(index))

      def getProgress: Float =
        index / split.workload.indexes.size

      def close {}

    }
  }
}

object S3InputFormat {
  val MappingKey = DistCache.Key("notion.dist-copy.download.mappings")

  def setMappings(j: Job, ctx: MrContext, m: DownloadMappings, z: Int): ResultTIO[Unit] = for {
    w <- calc(m, z)
    _ <- ResultT.safe[IO, Unit] {
      ctx.distCache.push(j, MappingKey, m.asJson.nospaces.getBytes("UTF-8"))
    }
    _ <- ResultT.safe[IO, Unit] {
      j.getConfiguration.set("notion.dist-copy.workloads", w.asJson.nospaces)
    }
  } yield ()

  def calc(mappings: DownloadMappings, mappers: Int): ResultTIO[Workloads] = {
    val work: Vector[Workload] =
      Partition.partitionGreedily(mappings.mappings.zipWithIndex, mappers, (z: (DownloadMapping, Int)) => z._1.from.size)
        .map(_.map(_._2)).map(z => Workload(z.toVector)).toVector
    ResultT.safe(Workloads(work))
  }
}
