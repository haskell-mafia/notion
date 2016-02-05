package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.saws.core._
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

  def readKeys[K <: Writable : Manifest, A](location: Location, conf: Configuration, emptyKey: => K)(f: K => String \/ A): LocationIO[String \/ List[A]] =
    readKeyValues(location, conf, emptyKey, NullWritable.get)({ case (k, _) => f(k) })

  def readValues[V <: Writable : Manifest, A](location: Location, conf: Configuration, emptyValue: => V)(f: V => String \/ A): LocationIO[String \/ List[A]] =
    readKeyValues(location, conf, NullWritable.get, emptyValue)({ case (_, v) => f(v) })

  def readKeyValues[K <: Writable, V <: Writable, A](location: Location, conf: Configuration, emptyKey: => K, emptyValue: => V)
                                                    (f: (K, V) => String \/ A)(implicit km: Manifest[K], vm: Manifest[V]): LocationIO[String \/ List[A]] = for {
    stream <- LocationIO.withLocationContext(location) {
      case LocalLocationContext(p)      => LocationIO.fromRIO(p.determineFile.flatMap(lf => RIO.safe[InputStream](new LocalInputStream(lf.path.path))))
      case HdfsLocationContext(conf, p) => LocationIO.fromHdfs(conf, p.toInputStream)
      case S3LocationContext(client, p) => LocationIO.fromS3(client, p.determineAddress.flatMap(a => S3Action.fromRIO(S3InputStream.stream(a, client))))
    }
    ioc    <- LocationIO.ioContext
    result <- LocationIO.fromRIO(readKeyValuesX(stream, conf, emptyKey, emptyValue)(f))
  } yield result

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

  def writeKeys[K <: Writable : Manifest, A](location: Location, conf: Configuration, items: List[A])(f: A => K): LocationIO[Unit] =
    writeKeyValues[K, NullWritable, A](location, conf, items)(a => (f(a), NullWritable.get))

  def writeValues[V <: Writable : Manifest, A](location: Location, conf: Configuration, items: List[A])(f: A => V): LocationIO[Unit] =
    writeKeyValues[NullWritable, V, A](location, conf, items)(a => (NullWritable.get, f(a)))

  def writeKeyValues[K <: Writable, V <: Writable, A](location: Location, conf: Configuration, items: List[A])
                                                     (f: A => (K, V))(implicit km: Manifest[K], vm: Manifest[V]): LocationIO[Unit] = for {
    stream <- LocationIO.withLocationContext(location) {
      case LocalLocationContext(p) =>
        LocationIO.fromRIO(p.doesNotExist(s"${p} already exists", p.dirname.mkdirs >> p.path.toOutputStream.map[OutputStream](new BufferedOutputStream(_))))
      case HdfsLocationContext(conf, p) =>
        LocationIO.fromHdfs(conf, p.doesNotExist(s"${p} already exists", p.dirname.mkdirs >> p.toOutputStream))
      case S3LocationContext(client, p) =>
        LocationIO.fromS3(client, S3Action.fromRIO(S3OutputStream.stream(p, client)))
    }
    _      <- LocationIO.fromRIO(writeKeyValuesX(stream, conf, items)(f))
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
