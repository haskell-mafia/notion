package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.poacher.scoobi.ScoobiAction
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Writable, BytesWritable, NullWritable, SequenceFile}
import org.apache.hadoop.fs.{Hdfs => _, _}

import java.io._
import java.util.UUID

import scalaz._, effect._, effect.Effect._

object SequenceUtil {
  def writeBytes(location: Location, conf: Configuration, client: AmazonS3Client, codec: Option[CompressionCodec])(f: (Array[Byte] => Unit) => ResultTIO[Unit]): ResultTIO[Unit] ={
    val out = (location match {
      case HdfsLocation(p) =>
        ResultT.safe[IO, OutputStream](FileSystem.get(conf).create(new Path(p)))
      case LocalLocation(p) =>
        ResultT.safe[IO, OutputStream](new BufferedOutputStream(new FileOutputStream(p)))
      case S3Location(bucket, key) =>
        S3OutputStream.stream(S3Address(bucket, key), client)
    })
    writeBytesX(out, conf, codec)(f)
  }

  def writeHdfsBytes(location: HdfsLocation, conf: Configuration, codec: Option[CompressionCodec])(f: (Array[Byte] => Unit) => ResultTIO[Unit]): Hdfs[Unit] = {
    val out = ResultT.safe[IO, OutputStream](FileSystem.get(conf).create(new Path(location.path)))
    Hdfs.fromResultTIO(writeBytesX(out, conf, codec)(f))
  }

  def writeBytesX(location: ResultTIO[OutputStream], conf: Configuration, codec: Option[CompressionCodec])(f: (Array[Byte] => Unit) => ResultTIO[Unit]): ResultTIO[Unit] = for {
    out  <- location
    _    <- ResultT.using(ResultT.safe(new FSDataOutputStream(out, new FileSystem.Statistics("SequenceUtil")))) {
      out => {
        val opts = List(
          SequenceFile.Writer.stream(out),
          SequenceFile.Writer.keyClass(classOf[NullWritable]),
          SequenceFile.Writer.valueClass(classOf[BytesWritable])
        ) ++ codec.map {
          SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, _)
        }
        ResultT.using(ResultT.safe(SequenceFile.createWriter(conf, opts: _*))) {
          writer => f(bytes => writer.append(NullWritable.get, new BytesWritable(bytes)))
        }
      }
    }
  } yield ()

  def readThrift[A](location: Location, conf: Configuration, client: AmazonS3Client)(forRow: Array[Byte] => A): ResultTIO[List[A]] = for {
    in <- (location match {
      case HdfsLocation(p) =>
        ResultT.safe[IO, InputStream](FileSystem.get(conf).open(new Path(p)))
      case LocalLocation(p) =>
        ResultT.safe[IO, InputStream](new LocalInputStream(p))
      case S3Location(bucket, key) =>
        S3InputStream.stream(S3Address(bucket, key), client)
    })
    r <- readThriftX(in, conf)(forRow)
  } yield r

  /* This code is for testing purposes only.
     It does not have any tests around it at this stage, it is also unsafe
     and the performance is unknown                                          */
  def readThriftX[A](location: InputStream, conf: Configuration)(forRow: Array[Byte] => A): ResultTIO[List[A]] = for {
    r <- ResultT.using(ResultT.safe(new FSDataInputStream(location))) {
      in => {
        ResultT.using(ResultT.safe(new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in)))) {
          reader => ResultT.safe[IO, List[A]] {
            val key = NullWritable.get
            val value = new BytesWritable()
            val buffer = scala.collection.mutable.ListBuffer[A]()
            while (nextSafe(reader, key, value)) {
              val bytes = value.copyBytes
              val a: A  = forRow(bytes)
              buffer += a
            }
            buffer.toList
          }
        }
      }
    }
  } yield r

  /* SequenceFile.Reader will throw an EOFException after reading all the data, if it
     doesn't know the length. Since we are using an InputStream, we hit this case      */
  def nextSafe(reader: SequenceFile.Reader, key: Writable, value: Writable): Boolean = {
    try {
      reader.next(key, value)
    } catch {
      case e: EOFException =>
        false
    }
  }

}
