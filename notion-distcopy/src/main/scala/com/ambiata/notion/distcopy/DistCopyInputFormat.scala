package com.ambiata.notion.distcopy


import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control.RIO
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr.{DistCache, MrContext, ThriftCache}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._

import scala.collection.JavaConverters._


/**
 * Create splits for a list of files to sync of roughly equal sizes
 */
class DistCopyInputFormat extends InputFormat[NullWritable, Mapping] {
  def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val workloads = DistCopyInputFormat.take(ctx).getOrElse(sys.error("notion dist-copy failed - `notion.dist-copy.workloads` not set"))

    workloads.workloads.map(workload =>
      new S3Split(workload): InputSplit
    ).asJava
  }

  def createRecordReader(inputSplit: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, Mapping] = {
    val split: S3Split = inputSplit.asInstanceOf[S3Split]
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val thrift: MappingsLookup = new MappingsLookup
    ctx.thriftCache.pop(context.getConfiguration, DistCopyInputFormat.MappingKey, thrift)
    val mappings: Mappings = Mappings.fromThrift(thrift)
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

      def getProgress: Float = {
        val i = split.workload.indexes.size
        if (i > 0) index.toFloat / i
        else 0
      }

      def close {}

    }
  }
}

object DistCopyInputFormat {
  val MappingKey = ThriftCache.Key("notion.dist-copy.sync.mappings")

  /*
   This code is terrible, but a necessary evil in this instance. With the memory
   constraints on the client, its cheaper to hold the Workloads im memory rather
   than serializer and de-serializer them.
   */
  private val local = new java.util.concurrent.ConcurrentHashMap[String, Workloads]

  def take(context: MrContext): Option[Workloads] =
    Option(local.remove(context.id.value))

  def setMappings(j: Job, ctx: MrContext, client: AmazonS3Client, mappings: Mappings, splitNumber: Int): RIO[Unit] = for {
    w <- calc(mappings, splitNumber, client, j.getConfiguration)
    _ <- RIO.safe[Unit](ctx.thriftCache.push(j, MappingKey, mappings.toThrift))
    _ = local.put(ctx.id.value, w)
  } yield ()

  def size(z: (Mapping, Int), client: AmazonS3Client, conf: Configuration): RIO[Long] = z._1 match {
    case DownloadMapping(from, _) => from.size.execute(client)
    case UploadMapping(from, _)   => Hdfs.size(from).run(conf).map(_.value)
  }

  def calc(mappings: Mappings, mappers: Int, client: AmazonS3Client, conf: Configuration): RIO[Workloads] = for {
    s <- mappings.mappings.zipWithIndex.traverse[RIO, (Mapping, Int, Long)]({
      case (a, b) =>
        RIO.when(b % 1000 == 0, println(s"File size calculated: $b of ${mappings.mappings.size}").pure[RIO]) >>
        size((a, b), client, conf).map(lon => (a, b, lon))
    })
    _ = println(s"File size calculated: ${mappings.mappings.size} of ${mappings.mappings.size}")
    getSize = { p : (Mapping, Int, Long) => p._3 }
    _ = println(s"Total file size to copy: ( ${s.map(getSize).sum}b ) - ${Bytes(s.map(getSize).sum).toMegabytes.value}mb")
    partitions = Partition.partitionGreedily[(Mapping, Int, Long)](s, mappers, getSize)
  } yield Workloads(partitions.map(_.map(_._2)).map(z => Workload(z)))
}
