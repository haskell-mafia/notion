package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.saws.s3._
import com.ambiata.poacher.hdfs.Hdfs

import org.apache.hadoop.io.{Writable, NullWritable, SequenceFile}
import org.apache.hadoop.fs.{Hdfs => _, _}
import org.apache.hadoop.conf.Configuration

import java.io.{InputStream, OutputStream, BufferedOutputStream, FileOutputStream, EOFException}
import scala.collection.mutable.ListBuffer

import scalaz._, Scalaz._, effect.Effect._

/**
 * Helpers for reading from and writing to sequence files in tests.
 *
 * Note: Only use this for testing, its not designed for production.
 */
object SequenceFileIO {

  def readKeys[K <: Writable : Manifest, A](location: Location, locationIO: LocationIO, emptyKey: => K)(f: K => String \/ A): RIO[String \/ List[A]] =
    readKeyValues(location, locationIO, emptyKey, NullWritable.get)({ case (k, _) => f(k) })

  def readValues[V <: Writable : Manifest, A](location: Location, locationIO: LocationIO, emptyValue: => V)(f: V => String \/ A): RIO[String \/ List[A]] =
    readKeyValues(location, locationIO, NullWritable.get, emptyValue)({ case (_, v) => f(v) })

  def readKeyValues[K <: Writable, V <: Writable, A](location: Location, locationIO: LocationIO, emptyKey: => K, emptyValue: => V)
                                                    (f: (K, V) => String \/ A)(implicit km: Manifest[K], vm: Manifest[V]): RIO[String \/ List[A]] = for {
    stream <- location match {
      case HdfsLocation(p)         => RIO.safe[InputStream](FileSystem.get(locationIO.configuration).open(new Path(p)))
      case LocalLocation(p)        => RIO.safe[InputStream](new LocalInputStream(p))
      case S3Location(bucket, key) => S3InputStream.stream(S3Address(bucket, key), locationIO.s3Client)
    }
    r <- readKeyValuesX(stream, locationIO.configuration, emptyKey, emptyValue)(f)
  } yield r

  def readKeyValuesX[K <: Writable, V <: Writable, A](stream: InputStream, conf: Configuration, emptyKey: => K, emptyValue: => V)
                                                     (f: (K, V) => String \/ A)(implicit km: Manifest[K], vm: Manifest[V]): RIO[String \/ List[A]] =

    RIO.io((emptyKey, emptyValue)).flatMap({ case (key, value) =>
      RIO.using(RIO.safe(new FSDataInputStream(stream)))(in => {
        RIO.using(RIO.safe(new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in))))(reader => RIO.safe({
          var state: String \/ ListBuffer[A] = for {
            _ <- compatibleClass(reader.getKeyClass, km.runtimeClass, "key").toLeftDisjunction(())
            _ <- compatibleClass(reader.getValueClass, vm.runtimeClass, "value").toLeftDisjunction(())
          } yield ListBuffer()

          while (state.isRight && nextSafe(reader, key, value)) {
            state = state.flatMap(buffer => f(key, value).map(a => buffer += a))
          }
          state.map(_.toList)
        }))
      })
    })

  def writeKeys[K <: Writable : Manifest, A](location: Location, locationIO: LocationIO, items: List[A])(f: A => K): RIO[Unit] =
    writeKeyValues[K, NullWritable, A](location, locationIO, items)(a => (f(a), NullWritable.get))

  def writeValues[V <: Writable : Manifest, A](location: Location, locationIO: LocationIO, items: List[A])(f: A => V): RIO[Unit] =
    writeKeyValues[NullWritable, V, A](location, locationIO, items)(a => (NullWritable.get, f(a)))

  def writeKeyValues[K <: Writable, V <: Writable, A](location: Location, locationIO: LocationIO, items: List[A])
                                                     (f: A => (K, V))(implicit km: Manifest[K], vm: Manifest[V]): RIO[Unit] = for {
    stream <- location match {
      case HdfsLocation(p) =>
        Hdfs.mkdir(new Path(p).getParent).run(locationIO.configuration) >>
          RIO.safe[OutputStream](FileSystem.get(locationIO.configuration).create(new Path(p)))
      case LocalLocation(p) =>
        Directories.mkdirs(FilePath.unsafe(p).dirname) >>
          RIO.safe[OutputStream](new BufferedOutputStream(new FileOutputStream(p)))
      case S3Location(bucket, key) =>
        S3OutputStream.stream(S3Address(bucket, key), locationIO.s3Client)
    }
    _ <- writeKeyValuesX(stream, locationIO.configuration, items)(f)
  } yield ()

  def writeKeyValuesX[K <: Writable, V <: Writable, A](stream: OutputStream, conf: Configuration, items: List[A])
                                                      (f: A => (K, V))(implicit km: Manifest[K], vm: Manifest[V]): RIO[Unit] =
    RIO.using(RIO.safe(new FSDataOutputStream(stream, new FileSystem.Statistics("SequenceUtil"))))(out => {
      val opts = List(
        SequenceFile.Writer.stream(out),
        SequenceFile.Writer.keyClass(km.runtimeClass),
        SequenceFile.Writer.valueClass(vm.runtimeClass)
      )
      RIO.using(RIO.safe(SequenceFile.createWriter(conf, opts: _*)))(writer => RIO.safe(
        items.foreach(a => {
          val (k, v) = f(a)
          writer.append(k, v)
        })
      ))
    })

  def compatibleClass(fileClass: Class[_], expectedClass: Class[_], label: String): Option[String] =
    (!expectedClass.isAssignableFrom(fileClass)).option(s"Expecting ${label} class in SequenceFile to be '${expectedClass.getSimpleName}', but found '${fileClass.getSimpleName}'")

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
