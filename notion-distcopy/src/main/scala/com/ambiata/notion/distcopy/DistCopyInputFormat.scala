package com.ambiata.notion.distcopy


import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control.{ResultT, ResultTIO}
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr.{DistCache, MrContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._

import argonaut.{Context => _, _}, Argonaut._

import scalaz._, Scalaz._, effect._

import scala.collection.JavaConverters._


/**
 * Create splits for a list of files to sync of roughly equal sizes
 */
class DistCopyInputFormat extends InputFormat[NullWritable, Mapping] {
  def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val workloads = context.getConfiguration.get("notion.dist-copy.workloads").decodeOption[Workloads].getOrElse(
      sys.error("notion dist-copy failed - `notion.dist-copy.workloads` not set"))

    workloads.workloads.map(workload =>
      new S3Split(workload): InputSplit
    ).asJava
  }

  def createRecordReader(inputSplit: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, Mapping] = {
    val split: S3Split = inputSplit.asInstanceOf[S3Split]
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val mappings = ctx.distCache.pop(context.getConfiguration, DistCopyInputFormat.MappingKey, bytes => new String(bytes, "UTF-8").decodeEither[Mappings])
    new RecordReader[NullWritable, Mapping] {
      var index = -1

      def initialize(split: InputSplit, context: TaskAttemptContext) = {
      }

      def nextKeyValue: Boolean = {
        index += 1
        index < split.workload.indexes.size
      }

      def getCurrentKey: NullWritable =
        NullWritable.get

      def getCurrentValue: Mapping =
        mappings.mappings(split.workload.indexes(index))

      def getProgress: Float =
        index / split.workload.indexes.size

      def close {}

    }
  }
}

object DistCopyInputFormat {
  val MappingKey = DistCache.Key("notion.dist-copy.sync.mappings")

  def setMappings(j: Job, ctx: MrContext, client: AmazonS3Client, mappings: Mappings, splitNumber: Int): ResultTIO[Unit] = for {
    w <- calc(mappings, splitNumber, client, j.getConfiguration)
    _ <- ResultT.safe[IO, Unit] {
      ctx.distCache.push(j, MappingKey, mappings.asJson.nospaces.getBytes("UTF-8"))
    }
    _ <- ResultT.safe[IO, Unit] {
      j.getConfiguration.set("notion.dist-copy.workloads", w.asJson.nospaces)
    }
  } yield ()

  def size(z: (Mapping, Int), client: AmazonS3Client, conf: Configuration): ResultTIO[Long] = z._1 match {
    case DownloadMapping(from, _) => from.size.executeT(client)
    case UploadMapping(from, _)   => Hdfs.size(from).run(conf).map(_.value)
  }

  def calc(mappings: Mappings, mappers: Int, client: AmazonS3Client, conf: Configuration): ResultTIO[Workloads] = for {
    s <- mappings.mappings.zipWithIndex.traverse[ResultTIO, (Mapping, Int, Long)]({
      case (a, b) => size((a, b), client, conf).map(lon => (a, b, lon))
    })
    getSize = { p : (Mapping, Int, Long) => p._3 }
    partitions = Partition.partitionGreedily[(Mapping, Int, Long)](s, mappers, getSize)
  } yield Workloads(partitions.map(_.map(_._2)).map(z => Workload(z)))
}
