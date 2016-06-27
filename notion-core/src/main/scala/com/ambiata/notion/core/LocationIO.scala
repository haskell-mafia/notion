package com.ambiata.notion.core

import com.ambiata.saws.core.{S3Action, Clients}
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.data.Lists
import com.ambiata.mundane.io._
import com.ambiata.mundane.bytes.Buffer
import com.ambiata.mundane.path._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.{S3Prefix, S3Address, S3Pattern}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.File
import Reducer._
import scalaz._, Scalaz._, effect.IO, effect.Effect._


case class LocationIO[A](run: IOContext => RIO[LocationIOResult[A]]) {

  def map[B](f: A => B): LocationIO[B] =
    LocationIO(c => run(c).map(_.map(f)))

  def flatMap[B](f: A => LocationIO[B]): LocationIO[B] =
    LocationIO(c => run(c).flatMap(_.result match {
      case -\/(e) => RIO.ok(LocationIOResult(e.left))
      case \/-(a) => f(a).run(c)
    }))
}

object LocationIO {

  def ioContext: LocationIO[IOContext] =
    LocationIO(c => RIO.ok(LocationIOResult.ok(c)))

  def ok[A](a: A): LocationIO[A] =
    LocationIO(_ => RIO.ok(LocationIOResult.ok(a)))

  def safe[A](a: => A): LocationIO[A] =
    LocationIO(_ => RIO.safe(LocationIOResult.ok(a)))

  def io[A](a: => A): LocationIO[A] =
    LocationIO(_ => RIO.io(LocationIOResult.ok(a)))

  def fail[A](e: LocationIOError): LocationIO[A] =
    LocationIO(_ => RIO.ok(LocationIOResult.fail(e)))

  def fromRIO[A](res: RIO[A]): LocationIO[A] =
    LocationIO(_ => res.map(LocationIOResult.ok))

  def fromHdfs[A](conf: Configuration, hdfs: Hdfs[A]): LocationIO[A] =
    fromRIO(hdfs.run(conf))

  def fromS3[A](client: AmazonS3Client, s3: S3Action[A]): LocationIO[A] =
    fromRIO(s3.run(client).map(_._2))

  def when[A](condition: Boolean)(action: LocationIO[A]): LocationIO[Unit] =
    if (condition) action.map(_ => ()) else ok(())

  def unless[A](condition: Boolean)(action: LocationIO[A]): LocationIO[Unit] =
    when(!condition)(action)

  def withLocationContext[A](location: Location)(f: LocationContext => LocationIO[A]): LocationIO[A] =
    ioContext.flatMap(context => LocationContext.fromContextLocation(context, location).fold(fail, f))

  def withConfiguration: LocationIO[Configuration] =
    ioContext.flatMap(ioc => ioc match {
      case HdfsIOContext(conf)      => ok(conf)
      case HdfsS3IOContext(conf, _) => ok(conf)
      case S3IOContext(_)           => fail(InvalidContext(ioc, "hdfs"))
      case NoneIOContext            => fail(InvalidContext(ioc, "hdfs"))
    })

  def withS3Client: LocationIO[AmazonS3Client] =
    ioContext.flatMap(ioc => ioc match {
      case HdfsIOContext(_)           => fail(InvalidContext(ioc, "s3"))
      case HdfsS3IOContext(_, client) => ok(client)
      case S3IOContext(client)        => ok(client)
      case NoneIOContext              => fail(InvalidContext(ioc, "s3"))
    })

  /* the (recursive) list of all the locations contained in `location` */
  def list(location: Location): LocationIO[List[Location]] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.listFilesRecursively.map(_.map(_.toLocalPath).map(LocalLocation.apply)))
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.listFilesRecursively.map(_.map(_.toHdfsPath).map(HdfsLocation.apply)))
      case S3LocationContext(client, p) => fromS3(client, p.resolve.map(_.map(_.toS3Pattern).map(S3Location.apply)))
    }

  def exists(location: Location): LocationIO[Boolean] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.exists)
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.exists)
      case S3LocationContext(client, p) => fromS3(client, p.exists)
    }

  /* the lines of the file present at `location` if there is one
   * Note: This use to be inconsistent - local/s3 would read a single file/object,
   *       but hdfs would read all files recursively
   */
  def readLines(location: Location): LocationIO[List[String]] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.readLines.flatMap(_.cata(RIO.ok, RIO.fail(s"Failed to read file - LocalPath(${p.path}) does not exist"))))
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.readLines.flatMap(_.cata(Hdfs.ok, Hdfs.fail(s"Failed to read file - HdfsPath(${p.path}) does not exist"))))
      case S3LocationContext(client, p) => fromS3(client, p.getLines)
    }

  def readLinesAll(location: Location): LocationIO[List[String]] =
    list(location).flatMap(_.traverse(readLines)).map(_.flatten)

  /** @return the content of the file present at `location` as a UTF-8 string if there is one */
  def readUtf8(location: Location): LocationIO[String] =
    readBytes(location).map(new String(_, "UTF-8"))

  def readUtf8All(location: Location): LocationIO[String] =
    list(location).flatMap(_.traverse(readUtf8)).map(_.mkString)

  /** @return the content of the file present at `location` as an array of bytes if there is one */
  def readBytes(location: Location): LocationIO[Array[Byte]] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.readBytes.flatMap(_.cata(RIO.ok, RIO.fail(s"Failed to read file - LocalPath(${p.path}) does not exist"))))
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.readBytes.flatMap(_.cata(Hdfs.ok, Hdfs.fail(s"Failed to read file - HdfsPath(${p.path}) does not exist"))))
      case S3LocationContext(client, p) => fromS3(client, p.getBytes)
    }

  def readUnsafe(location: Location)(f: java.io.InputStream => LocationIO[Unit]): LocationIO[Unit] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => LocationIO(ioc => p.determineFile.flatMap(l => RIO.using(l.toInputStream)(i => f(i).run(ioc))))
      case HdfsLocationContext(conf, p) => LocationIO(ioc => p.readWith(i => Hdfs.fromRIO(f(i).run(ioc))).run(conf))
      case S3LocationContext(client, p) => LocationIO(ioc => p.withStream(i => f(i).run(ioc)).run(client).map(_._2)) // TODO drop AwsLog?
    }

  /** write to a given location, using an OutputStream */
  def writeUnsafe(location: Location)(f: java.io.OutputStream => LocationIO[Unit]): LocationIO[Unit] =
    withLocationContext(location) {
      case LocalLocationContext(p) => LocationIO(ioc => for {
        _ <- p.doesNotExist(s"Can not overwrite location ${location}", p.dirname.mkdirs)
        r <- RIO.using(p.path.toOutputStream)(o => f(o).run(ioc))
      } yield r)
      case HdfsLocationContext(conf, p) => LocationIO(ioc => p.writeWith(o => Hdfs.fromRIO(f(o).run(ioc))).run(conf))
      case S3LocationContext(client, p) => LocationIO(ioc => for {
        // TODO should we add something like this to saws?
        tmpPath <- LocalTemporary.random.pathWithParent
        res     <- RIO.using(tmpPath.path.toOutputStream)(o => f(o).run(ioc))
        tmpFile <- tmpPath.determineFile
        _       <- (for {
          e <- p.exists
          _ <- S3Action.when(e, S3Action.fail(s"Can not overwrite location ${location}"))
          _ <- p.putFile(tmpFile)
        } yield ()).execute(client)
      } yield res)
    }

  def streamLinesUTF8[A](location: Location, empty: => A)(f: (String, A) => A): LocationIO[A] =
    io(empty).flatMap(s => {
      var state = s
      readUnsafe(location)(in =>
        io({
          val reader = new java.io.BufferedReader(new java.io.InputStreamReader(in, "UTF-8"))
          var line = reader.readLine
          while (line != null) {
            state = f(line, state)
            line = reader.readLine
          }
        })).as(state)
    })

  /** read a file as a stream of bytes and compute some state value */
  def streamBytes[A](location: Location, empty: => A, bufferSize: Int = 4096)(f: (Buffer, A) => A): LocationIO[A] =
    io(empty).flatMap(s => {
      var state = s
      readUnsafe(location)(in =>
        io({
          var buffer = Buffer.wrapArray(Array.ofDim[Byte](bufferSize), 0, bufferSize)
          var length = 0
          while ({ length = in.read(buffer.bytes, buffer.offset, bufferSize); length != -1 }) {
            buffer = Buffer.allocate(buffer, length)
            state = f(buffer, state)
          }
        })).as(state)
    })

  /** get the first lines of a file */
  def head(location: Location, numberOfLines: Int): LocationIO[List[String]] = {
    val lines: collection.mutable.ListBuffer[String] = new collection.mutable.ListBuffer[String]
    readUnsafe(location) { in =>
      io(new java.io.BufferedReader(new java.io.InputStreamReader(in, "UTF-8"))).map { reader =>
        var line = reader.readLine
        while (line != null && lines.size < numberOfLines) {
          lines.append(line)
          line = reader.readLine
        }
      }
    }.map(_ => lines.toList)
  }

  /** get the first line of a file */
  def firstLine(location: Location): LocationIO[Option[String]] =
    head(location, numberOfLines = 1).map(_.headOption)

  /**
   * count the number of lines in a file and get the last one
   * This is done in one pass to avoid parsing the file twice when both the lines number and
   * the tail are required
   */
  def tailAndLinesNumber(location: Location, numberOfLines: Int): LocationIO[(List[String], Int)] = {
    def addLine(lines: collection.mutable.Queue[String], line: String): collection.mutable.Queue[String] = {
      lines.enqueue(line)
      if (lines.size > numberOfLines && !lines.isEmpty) lines.dequeue()
      lines
    }

    streamLinesUTF8[(collection.mutable.Queue[String], Int)](location, (new collection.mutable.Queue[String], 0)) {
      case (line, (lines, number)) => (addLine(lines, line), number + 1)
    }.map(_.leftMap(_.toList))
  }

  def reduceLinesUTF8[T](location: Location, reducer: LineReducer[T]): LocationIO[T] =
    streamLinesUTF8[reducer.S](location, reducer.init)((line, s) => reducer.reduce(line, s)).map(s => reducer.finalise(s))

  def reduceBytes[T](location: Location, reducer: BytesReducer[T]): LocationIO[T] =
    streamBytes[reducer.S](location, reducer.init)((bytes, s) => reducer.reduce(bytes, s)).map(s => reducer.finalise(s))

  /** get the last line of a file */
  def tail(location: Location, numberOfLines: Int): LocationIO[List[String]] =
    reduceLinesUTF8(location, LineReducer.tail(numberOfLines))

  /** get the last line of a file */
  def lastLine(location: Location): LocationIO[Option[String]] =
    tail(location, numberOfLines = 1).map(_.lastOption)

  def writeUtf8Lines(location: Location, lines: List[String]): LocationIO[Unit] =
    writeUtf8(location, Lists.prepareForFile(lines))

  def writeUtf8(location: Location, string: String): LocationIO[Unit] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.write(string)).void
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.write(string)).void
      case S3LocationContext(client, p) => fromS3(client, for {
        e <- p.exists
        _ <- S3Action.when(e, S3Action.fail(s"Can not overwrite location ${location}"))
        _ <- p.put(string)
      } yield ())
    }

  def writeBytes(location: Location, bytes: Array[Byte]): LocationIO[Unit] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.writeBytes(bytes)).void
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.writeBytes(bytes)).void
      case S3LocationContext(client, p) => fromS3(client, for {
        e <- p.exists
        _ <- S3Action.when(e, S3Action.fail(s"Can not overwrite location ${location}"))
        _ <- p.putBytes(bytes)
      } yield ())
    }

  def deleteAll(location: Location): LocationIO[Unit] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.delete)
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.delete)
      case S3LocationContext(client, p) => fromS3(client, for {
        e <- p.exists
        _ <- S3Action.unless(e, S3Action.fail(s"Location does not exist! ${location}"))
        _ <- p.delete
      } yield ())
    }

  def delete(location: Location): LocationIO[Unit] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.determineFile.flatMap(_.delete))
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.determineFile.flatMap(_.delete))
      case S3LocationContext(client, p) => fromS3(client, p.determineAddress.flatMap(_.delete))
    }

  def size(location: Location): LocationIO[BytesQuantity] =
    withLocationContext(location) {
      case LocalLocationContext(p)      => fromRIO(p.size)
      case HdfsLocationContext(conf, p) => fromHdfs(conf, p.sizeOrFail)
      case S3LocationContext(client, p) => fromS3(client, for {
        s <- p.size
        b <- s.cata(l => S3Action.ok(Bytes(l)), S3Action.fail(s"Location does not exist! ${location}"))
      } yield b)
    }

  /**
   * copy a file by simply piping lines from one location to the other
   *
   * if overwrite is false and the location file already exists, return an Error
   */
  def copyFile(from: Location, to: Location, overwrite: Boolean): LocationIO[Unit] = {
    val targetMode = overwrite.option(TargetMode.Overwrite).getOrElse(TargetMode.Fail)
    withLocationContext(from)(f => withLocationContext(to)(t => (f -> t) match {
      case (LocalLocationContext(p1), LocalLocationContext(p2)) =>
        LocationIO.fromRIO(p1.determineFile.flatMap(_.copyWithMode(p2, targetMode)).void)
      case (HdfsLocationContext(conf1, p1), HdfsLocationContext(conf2, p2)) =>
        LocationIO.fromHdfs(conf1, p1.determineFile.flatMap(_.copyWithMode(p2, targetMode)).void)
      case (S3LocationContext(client1, p1), S3LocationContext(client2, p2)) =>
        LocationIO.fromS3(client1, (for {
          e  <- p2.exists
          _  <- S3Action.when(e && !overwrite, S3Action.fail(s"Can not overwrite location ${to}"))
          a1 <- p1.determineAddress
          _  <- a1.copy(S3Address(p2.bucket, p2.unknown))
        } yield ()))
      case _ => for {
        e <- exists(to)
        _ <- when(e) {
          if(overwrite) delete(to)
          else LocationIO(_ => RIO.fail[LocationIOResult[Unit]](s"Can not overwrite location ${to}"))
        }
        _ <- pipe(from, to)
      } yield ()
    }))
  }

  /** pipe bytes from one location to another */
  def pipe(from: Location, to: Location): LocationIO[Unit] =
    readUnsafe(from)(in => writeUnsafe(to)(out => fromRIO(Streams.pipe(in, out))))

  implicit def LocationIOMonad: Monad[LocationIO] = new Monad[LocationIO] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: LocationIO[A])(f: A => LocationIO[B]) = m.flatMap(f)
  }
}
