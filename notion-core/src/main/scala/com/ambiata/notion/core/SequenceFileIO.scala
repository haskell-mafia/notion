package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Writable, BytesWritable, NullWritable, IntWritable, SequenceFile}
import org.apache.hadoop.fs.{Hdfs => _, _}

import java.io._

import scalaz._, Scalaz._, effect.Effect._

/**
 * This is based off SequenceUtil, but can read/write sequence files of any key/value type
 * 
 * WARNING: Not suitable for reading/writing large amounts of data as everything is kept in memory
 */
case class SequenceFileIO[K <: Writable, V <: Writable, A](conf: Configuration, emptyKey: K, emptyValue: V, fromKeyValue: (K, V) => A, toKeyValue: A => (K, V))
                                                          (implicit km: Manifest[K], vm: Manifest[V]) {

  def readLocation(location: Location, client: AmazonS3Client): RIO[List[A]] = for {
    in <- (location match {
      case HdfsLocation(p) =>
        RIO.safe[InputStream](FileSystem.get(conf).open(new Path(p)))
      case LocalLocation(p) =>
        RIO.safe[InputStream](new LocalInputStream(p))
      case S3Location(bucket, key) =>
        S3InputStream.stream(S3Address(bucket, key), client)
    })
    as <- readInputStream(in)
  } yield as

  def readInputStream(input: InputStream): RIO[List[A]] =
    RIO.using(RIO.safe(new FSDataInputStream(input))) {
      in => {
        RIO.using(RIO.safe(new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in)))) {
          reader => for {
            _ <- compatibleClassOrFail(reader.getKeyClass, km.runtimeClass, "key")
            _ <- compatibleClassOrFail(reader.getValueClass, vm.runtimeClass, "value")
            r <- RIO.safe[List[A]] {
              val buffer = scala.collection.mutable.ListBuffer[A]()
              while (SequenceUtil.nextSafe(reader, emptyKey, emptyValue)) {
                val a: A  = fromKeyValue(emptyKey, emptyValue)
                buffer += a
              }
              buffer.toList
            }
          } yield r
        }
      }
    }

  def writeLocation(location: Location, client: AmazonS3Client, codec: Option[CompressionCodec], as: List[A]): RIO[Unit] = for {
    out <- (location match {
      case HdfsLocation(p) =>
        Hdfs.mkdir(new Path(p).getParent).run(conf) >>
          RIO.safe[OutputStream](FileSystem.get(conf).create(new Path(p)))
      case LocalLocation(p) =>
        Directories.mkdirs(FilePath.unsafe(p).dirname) >>
          RIO.safe[OutputStream](new BufferedOutputStream(new FileOutputStream(p)))
      case S3Location(bucket, key) =>
        S3OutputStream.stream(S3Address(bucket, key), client)
    })
    _ <- writeOutputStream(out, codec, as)
  } yield ()

  def writeOutputStream(output: OutputStream, codec: Option[CompressionCodec], as: List[A]): RIO[Unit] = for {
    _    <- RIO.using(RIO.safe(new FSDataOutputStream(output, new FileSystem.Statistics("SequenceFile")))) {
      out => {
        val opts = List(
          SequenceFile.Writer.stream(out),
          SequenceFile.Writer.keyClass(km.runtimeClass),
          SequenceFile.Writer.valueClass(vm.runtimeClass)
        ) ++ codec.map {
          SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, _)
        }
        RIO.using(RIO.safe(SequenceFile.createWriter(conf, opts: _*))) {
          writer => RIO.safe(as.map(toKeyValue).foreach((writer.append _).tupled))
        }
      }
    }
  } yield ()

  def compatibleClassOrFail(fileClass: Class[_], expectedClass: Class[_], label: String): RIO[Unit] =
    if(!expectedClass.isAssignableFrom(fileClass))
      RIO.fail(s"Expecting ${label} class in SequenceFile to be '${expectedClass.getSimpleName}', but found '${fileClass.getSimpleName}'")
    else
      RIO.ok(())
}

object SequenceFileIO {

  def nullBytes[A](conf: Configuration, from: Array[Byte] => A, to: A => Array[Byte]): SequenceFileIO[NullWritable, BytesWritable, A] = {
    val fromWritables: (NullWritable, BytesWritable) => A = (_, bw) =>
      from(bw.copyBytes)

    val toWritables: A => (NullWritable, BytesWritable) = a =>
      (NullWritable.get, new BytesWritable(to(a)))

    SequenceFileIO(conf, NullWritable.get, new BytesWritable, fromWritables, toWritables)
  }

  def intBytes[A](conf: Configuration, from: (Int, Array[Byte]) => A, to: A => (Int, Array[Byte])): SequenceFileIO[IntWritable, BytesWritable, A] = {
    val fromWritables: (IntWritable, BytesWritable) => A = (iw, bw) =>
      from(iw.get, bw.copyBytes)

    val toWritables: A => (IntWritable, BytesWritable) = a => {
      val (k, v) = to(a)
      (new IntWritable(k), new BytesWritable(v))
    }
    SequenceFileIO(conf, new IntWritable, new BytesWritable, fromWritables, toWritables)
  }
}
