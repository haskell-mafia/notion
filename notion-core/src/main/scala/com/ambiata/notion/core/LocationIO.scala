package com.ambiata.notion.core

import com.ambiata.saws.core.{S3Action, Clients}
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.data.Lists
import com.ambiata.mundane.io._
import com.ambiata.mundane.bytes.Buffer
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.{S3Prefix, S3Address, S3Pattern}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.File
import Reducer._
import scalaz._, Scalaz._, effect.IO, effect.Effect._

/**
 * This module provides file system-like functions on HDFS, S3 and locally
 */
case class LocationIO(configuration: Configuration, s3Client: AmazonS3Client) {

  /** @return the (recursive) list of all the locations contained in `location` */
  def list(location: Location): RIO[List[Location]] = location match {
    case l @ LocalLocation(path) =>
      Directories.list(l.dirPath).map(_.map(f => LocalLocation(f.path)))

    case s @ S3Location(bucket, key) =>
      S3Pattern(bucket, key).listKeys.execute(s3Client).map(_.map(k => S3Location(bucket,  k)))

    case h @ HdfsLocation(path) =>
      Hdfs.globFilesRecursively(new Path(path)).run(configuration).map(_.map(p => HdfsLocation(p.toString)))
  }

  /** @return true if the location exists */
  def exists(location: Location): RIO[Boolean] = location match {
    case l @ LocalLocation(path)            =>
      Files.exists(FilePath.unsafe(path)).flatMap(e => if (e) RIO.ok[Boolean](e) else Directories.exists(l.dirPath))

    case s @ S3Location(bucket, key) => S3Pattern(bucket, key).exists.execute(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.exists(new Path(path)).run(configuration)
  }

  /** @return true if the location can contain other locations */
  def isDirectory(location: Location): RIO[Boolean] = location match {
    case l @ LocalLocation(path)     => RIO.safe[Boolean](new File(path).isDirectory)
    case s @ S3Location(bucket, key) => S3Pattern(bucket, key + "/").determine.map(_.exists(_.isRight)).execute(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.isDirectory(new Path(path)).run(configuration)
  }

  /** @return the lines of the file present at `location` if there is one */
  def readLines(location: Location): RIO[List[String]] = location match {
    case l @ LocalLocation(path)     => Files.readLines(l.filePath).map(_.toList)
    case s @ S3Location(bucket, key) =>
      S3Pattern(bucket, key).determine.flatMap { prefixOrAddress =>
        val asAddress = prefixOrAddress.flatMap(_.swap.toOption)
        val lines = asAddress.map(_.getLines).getOrElse(S3Action.fail(s"There is no file at ${location.render}"))
        lines
      }.execute(s3Client)

    case h @ HdfsLocation(path)      =>
      Hdfs.isDirectory(new Path(path)).flatMap { isDirectory =>
        if (isDirectory)
          Hdfs.globFilesRecursively(new Path(path)).filterHidden
            .flatMap(_.traverseU(Hdfs.readLines)).map(_.toList.flatten)
        else
          Hdfs.readLines(new Path(path)).map(_.toList)
      }.run(configuration)
  }

  def readUnsafe(location: Location)(f: java.io.InputStream => RIO[Unit]): RIO[Unit] =
    location match {
      case l @ LocalLocation(_) =>
        RIO.using(l.filePath.toInputStream)(f)
      case S3Location(bucket, key) =>
        RIO.using(S3Address(bucket, key).getObject.map(_.getObjectContent).execute(s3Client))(f)
      case HdfsLocation(path) =>
        Hdfs.readWith(new Path(path), f).run(configuration)
    }

  /** write to a given location, using an OutputStream */
  def writeUnsafe(location: Location)(f: java.io.OutputStream => RIO[Unit]): RIO[Unit] =
    location match {
      case l @ LocalLocation(_) =>
        RIO.safe(l.dirPath.dirname.toFile.mkdirs) >>
        RIO.using(l.filePath.toOutputStream)(f)

      case HdfsLocation(path) =>
        Hdfs.writeWith(new Path(path), f).run(configuration)

      case S3Location(bucket, key) =>
        for {
          tmpFile <- LocalTemporary.random.fileWithParent
          out     <- tmpFile.toOutputStream
          _       <- RIO.using(RIO.ok(out))(f)
          _       <- S3Address(bucket, key).putFile(tmpFile).execute(s3Client)
        } yield ()
    }

  def streamLinesUTF8[A](location: Location, empty: => A)(f: (String, A) => A): RIO[A] =
    RIO.io(empty).flatMap { s =>
      var state = s
      readUnsafe(location) { in => RIO.io {
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(in, "UTF-8"))
        var line = reader.readLine
        while (line != null) {
          state = f(line, state)
          line = reader.readLine
        }
      }}.as(state)
    }

  /** read a file as a stream of bytes and compute some state value */
  def streamBytes[A](location: Location, empty: => A, bufferSize: Int = 4096)(f: (Buffer, A) => A): RIO[A] =
    RIO.io(empty).flatMap { s =>
      var state = s
      readUnsafe(location) { in => RIO.io {
        val buffer = Buffer.wrapArray(Array.ofDim[Byte](bufferSize), 0, bufferSize)
        var length = 0
        while ({ length = in.read(buffer.bytes, buffer.offset, bufferSize); length != -1 }) {
          state = f(Buffer.allocate(buffer, length), state)
        }
      }}.as(state)
    }

  def writeUtf8Lines(location: Location, lines: List[String]): RIO[Unit] =
    writeUtf8(location, Lists.prepareForFile(lines))

  def writeUtf8(location: Location, string: String): RIO[Unit] = location match {
    case l @ LocalLocation(path)     => Files.write(l.filePath, string)
    case s @ S3Location(bucket, key) => S3Address(bucket, key).put(string).execute(s3Client).void
    case h @ HdfsLocation(path)      => Hdfs.writeWith(new Path(path), out => Streams.write(out, string)).run(configuration)
  }

  def deleteAll(location: Location): RIO[Unit] = location match {
    case l @ LocalLocation(path)     => Directories.delete(l.dirPath).void
    case s @ S3Location(bucket, key) => S3Prefix(bucket, key).delete.execute(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.deleteAll(new Path(path)).run(configuration)
  }

  def delete(location: Location): RIO[Unit] = location match {
    case l @ LocalLocation(path)     => Files.delete(l.filePath)
    case s @ S3Location(bucket, key) => S3Address(bucket, key).delete.execute(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.delete(new Path(path)).run(configuration)
  }

  /** get the first lines of a file */
  def head(location: Location, numberOfLines: Int): RIO[List[String]] = {
    val lines: collection.mutable.ListBuffer[String] = new collection.mutable.ListBuffer[String]
    readUnsafe(location) { in =>
      RIO.io(new java.io.BufferedReader(new java.io.InputStreamReader(in, "UTF-8"))).map { reader =>
        var line = reader.readLine
        while (line != null && lines.size < numberOfLines) {
          lines.append(line)
          line = reader.readLine
        }
      }
    }.map(_ => lines.toList)
  }

  /** get the first line of a file */
  def firstLine(location: Location): RIO[Option[String]] =
    head(location, numberOfLines = 1).map(_.headOption)

  /**
   * count the number of lines in a file and get the last one
   * This is done in one pass to avoid parsing the file twice when both the lines number and
   * the tail are required
   */
  def tailAndLinesNumber(location: Location, numberOfLines: Int): RIO[(List[String], Int)] = {
    def addLine(lines: collection.mutable.Queue[String], line: String): collection.mutable.Queue[String] = {
      lines.enqueue(line)
      if (lines.size > numberOfLines && !lines.isEmpty) lines.dequeue()
      lines
    }

    streamLinesUTF8[(collection.mutable.Queue[String], Int)](location, (new collection.mutable.Queue[String], 0)) {
      case (line, (lines, number)) => (addLine(lines, line), number + 1)
    }.map(_.leftMap(_.toList))
  }

  def reduceLinesUTF8[T](location: Location, reducer: LineReducer[T]): RIO[T] =
    streamLinesUTF8[reducer.S](location, reducer.init)((line, s) => reducer.reduce(line, s)).map(s => reducer.finalise(s))

  def reduceBytes[T](location: Location, reducer: BytesReducer[T]): RIO[T] =
    streamBytes[reducer.S](location, reducer.init)((bytes, s) => reducer.reduce(bytes, s)).map(s => reducer.finalise(s))

  /** get the last line of a file */
  def tail(location: Location, numberOfLines: Int): RIO[List[String]] =
    reduceLinesUTF8(location, LineReducer.tail(numberOfLines))

  /** get the last line of a file */
  def lastLine(location: Location): RIO[Option[String]] =
    tail(location, numberOfLines = 1).map(_.lastOption)
}