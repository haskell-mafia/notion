package com.ambiata.notion.core

import com.ambiata.com.amazonaws.AmazonServiceException
import com.ambiata.com.amazonaws.services.s3.model.ListObjectsRequest
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
    case l @ LocalLocation(_) => listLocal(l).map(identity)
    case s @ S3Location(_, _) => listS3(s).map(identity)
    case h @ HdfsLocation(_)  => listHdfs(h).map(identity)
  }

  def listLocal(local: LocalLocation): RIO[List[LocalLocation]] =
    Directories.list(local.dirPath).map(_.map(f => LocalLocation(f.path)))

  def listHdfs(hdfs: HdfsLocation): RIO[List[HdfsLocation]] =
    Hdfs.globFilesRecursively(new Path(hdfs.path)).run(configuration).map(_.map(p => HdfsLocation(p.toString)))

  def listS3(s3: S3Location): RIO[List[S3Location]] =
    S3Pattern(s3.bucket, s3.key).listKeys.execute(s3Client).map(_.map(k => S3Location(s3.bucket,  k)))

  /** @return true if the location exists */
  def exists(location: Location): RIO[Boolean] = location match {
    case l @ LocalLocation(path)            =>
      Files.exists(FilePath.unsafe(path)).flatMap(e => if (e) RIO.ok[Boolean](e) else Directories.exists(l.dirPath))

    case s @ S3Location(bucket, key) => S3Pattern(bucket, key).exists.execute(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.exists(new Path(path)).run(configuration)
  }

  /**
   * @return true if the location can contain other locations
   *         Note that a S3Location can both be a directory, meaning that it is an existing S3Prefix
   *         and a file, meaning that it is an existing S3Address
   */
  def isDirectory(location: Location): RIO[Boolean] = location match {
    case l @ LocalLocation(path)     => RIO.safe[Boolean](new File(path).isDirectory)
    case s @ S3Location(bucket, key) => S3Prefix(bucket, key).exists.execute(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.isDirectory(new Path(path)).run(configuration)
  }

  /**
   * @return true if the location references a file.
   */
  def isFile(location: Location): RIO[Boolean] = location match {
    case l @ LocalLocation(path)     => isDirectory(location).map(!_)
    case h @ HdfsLocation(path)      => isDirectory(location).map(!_)
    case s @ S3Location(bucket, key) => S3Address(bucket, key).exists.execute(s3Client)
  }

  /** @return the lines of the file present at `location` if there is one */
  def readLines(location: Location): RIO[List[String]] = location match {
    case l @ LocalLocation(path)     => Files.readLines(l.filePath).map(_.toList)
    case s @ S3Location(bucket, key) =>
      S3Pattern(bucket, key).determine.flatMap { prefixOrAddress =>
        val asAddress = prefixOrAddress.flatMap(_.swap.toOption)
        asAddress.map(_.getLines).getOrElse(S3Action.fail(s"There is no file at ${location.render}"))
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

  /** @return the content of the file present at `location` as a UTF-8 string if there is one */
  def readUtf8(location: Location): RIO[String] =
    readBytes(location).map(new String(_, "UTF-8"))

  /** @return the content of the file present at `location` as an array of bytes if there is one */
  def readBytes(location: Location): RIO[Array[Byte]] = location match {
    case l @ LocalLocation(path) => Files.readBytes(l.filePath)
    case s @ S3Location(bucket, key) =>
      S3Pattern(bucket, key).determine.flatMap { prefixOrAddress =>
        val asAddress = prefixOrAddress.flatMap(_.swap.toOption)
        asAddress.map(_.getBytes).getOrElse(S3Action.fail(s"There is no file at ${location.render}"))
      }.execute(s3Client)

    case h @ HdfsLocation(path) => Hdfs.readBytes(new Path(path)).run(configuration)
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
        var buffer = Buffer.wrapArray(Array.ofDim[Byte](bufferSize), 0, bufferSize)
        var length = 0
        while ({ length = in.read(buffer.bytes, buffer.offset, bufferSize); length != -1 }) {
          buffer = Buffer.allocate(buffer, length)
          state = f(buffer, state)
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

  def writeBytes(location: Location, bytes: Array[Byte]): RIO[Unit] = location match {
    case l @ LocalLocation(path)     => Files.writeBytes(l.filePath, bytes)
    case s @ S3Location(bucket, key) => S3Address(bucket, key).putBytes(bytes).execute(s3Client).void
    case h @ HdfsLocation(path)      => Hdfs.writeWith(new Path(path), out => Streams.writeBytes(out, bytes)).run(configuration)
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

  /**
   * copy a file by simply piping lines from one location to the other
   *
   * if overwrite is false and the location file already exists, return an Error
   */
  def copyFile(from: Location, to: Location, overwrite: Boolean): RIO[Unit] = {
    for {
      exists <- exists(to)
      _      <-
      if (exists)
        if (overwrite) delete(to) >> unsafeCopyFile(from, to)
        else           RIO.fail(s"There is already a file at ${to.render}")
      else unsafeCopyFile(from, to)
    } yield ()
  }

  /**
   * copy a file by simply piping lines from one location to the other
   *
   * if the file exist at destination
   *  - if the destination is a sync location do nothing
   *  - otherwise fail
   */
  def syncFile(from: Location, to: Location): RIO[Unit] =
    exists(to).flatMap { exist =>
      if (!exist) unsafeCopyFile(from, to)
      else (from, to) match {
        case (f: S3Location, t: LocalLocation)   => RIO.putStrLn(s"a file already exists at ${to.render}, ${from.render} won't be downloaded again")
        case (f: S3Location, t: HdfsLocation)    => RIO.putStrLn(s"a file already exists at ${to.render}, ${from.render} won't be downloaded again")
        case (f: LocalLocation, t: HdfsLocation) => RIO.putStrLn(s"a file already exists at ${to.render}, ${from.render} won't be downloaded again")
        case other                               => RIO.fail("can not overwrite an existing result file at "+to.render)
      }
    }

  /** copy file from one location to another, always overwriting the destination */
  def unsafeCopyFile(from: Location, to: Location): RIO[Unit] =
    (from, to) match {
      case (l1 @LocalLocation(_),  l2 @ LocalLocation(_)) => com.ambiata.mundane.io.Files.copy(l1.filePath, l2.filePath)
      case (HdfsLocation(p1),      HdfsLocation(p2))      => Hdfs.cp(new Path(p1), new Path(p2), overwrite = true).run(configuration)
      case (S3Location(b1, k1),    S3Location(b2, k2))    => S3Address(b1, k1).copy(S3Address(b2, k2)).execute(s3Client).void
      case _                                              => pipe(from, to)
    }

  /** pipe bytes from one location to another */
  def pipe(from: Location, to: Location): RIO[Unit] =
    readUnsafe(from)(in => writeUnsafe(to)(out => Streams.pipe(in, out)))

  /**
   * synchronize files from one location to another.
   *
   * if the first location is a directory, copy all the files in that directory to the other location (must be a directory as well)
   *
   * files existing at destination are not being copied
   *
   * Note: in the case of a large copy from S3 to Hdfs or from Hdfs to S3, a distcopy should be done instead
   * see SynchronizedInputsOutputs
   *
   * @return the list of copied locations
   */
  def syncFiles(from: Location, to: Location): RIO[List[Location]] =
    isDirectory(from) >>= { isDirectory =>
      if (isDirectory)
        list(from).flatMap(_.traverseU {
          case f @ LocalLocation(_) => syncFile(f, to </> f.filePath).as(f: Location)
          case f @ HdfsLocation(_)  => syncFile(f, to </> f.filePath).as(f: Location)
          case s @ S3Location(_, k) => syncFile(s, to </> FilePath.unsafe(k)).as(s: Location)
        })
      else
        syncFile(from, to).as(List(from))
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
