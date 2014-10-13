package com.ambiata.notion

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import com.ambiata.poacher.hdfs._
import java.io.{InputStream, OutputStream}
import java.io.{PipedInputStream, PipedOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, scalaz.stream._, scalaz.concurrent._, effect.IO
import scodec.bits.ByteVector

case class HdfsStore(conf: Configuration, root: DirPath) extends Store[ResultTIO] with ReadOnlyStore[ResultTIO] {
  def readOnly: ReadOnlyStore[ResultTIO] =
    this

  def basePath: Path =
    new Path(root.path)

  def list(prefix: Key): ResultT[IO, List[Key]] =
    hdfs { Hdfs.filesystem.flatMap { fs =>
      Hdfs.globFilesRecursively(root </> keyToDirPath(prefix)).map { paths =>
        paths.map { path =>
          filePathToKey(FilePath.unsafe(path.toString).relativeTo(root </> keyToDirPath(prefix)))
        }
      }
    }}

  def listHeads(prefix: Key): ResultT[IO, List[Key]] =
    hdfs { Hdfs.filesystem.flatMap { fs =>
      Hdfs.globPaths(root </> keyToDirPath(prefix)).map { paths =>
        paths.map(path => Key.unsafe(path.getName))
      }
    }}

  def filter(prefix: Key, predicate: Key => Boolean): ResultT[IO, List[Key]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: Key, predicate: Key => Boolean): ResultT[IO, Option[Key]] =
    list(prefix).map(_.find(predicate))

  def exists(key: Key): ResultT[IO, Boolean] =
    hdfs { Hdfs.exists(root </> keyToFilePath(key)) }

  def existsPrefix(prefix: Key): ResultT[IO, Boolean] =
    hdfs { Hdfs.exists(root </> keyToFilePath(prefix)) }

  def delete(key: Key): ResultT[IO, Unit] =
    hdfs { Hdfs.delete(root </> keyToFilePath(key)) }

  def deleteAll(prefix: Key): ResultT[IO, Unit] =
    hdfs { Hdfs.deleteAll(root </> keyToDirPath(prefix)) }

  def move(in: Key, out: Key): ResultT[IO, Unit] =
    copy(in, out) >> delete(in)

  def copy(in: Key, out: Key): ResultT[IO, Unit] =
    hdfs { Hdfs.cp(root </> keyToFilePath(in), root </> keyToFilePath(out), false) }

  def mirror(in: Key, out: Key): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU(source => copy(source, out / source))
  } yield ()

  def moveTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copyTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    unsafe.withInputStream(src) { in =>
      store.unsafe.withOutputStream(dest) { out =>
        Streams.pipe(in, out) }}

  def mirrorTo(store: Store[ResultTIO], in: Key, out: Key): ResultT[IO, Unit] = for {
    keys <- list(in)
    _    <- keys.traverseU(source => copyTo(store, source, out / source))
  } yield ()

  def checksum(key: Key, algorithm: ChecksumAlgorithm): ResultT[IO, Checksum] =
    withInputStreamValue[Checksum](key)(in => Checksum.stream(in, algorithm))

  val bytes: StoreBytes[ResultTIO] = new StoreBytes[ResultTIO] {
    def read(key: Key): ResultT[IO, ByteVector] =
      withInputStreamValue[Array[Byte]](key)(Streams.readBytes(_, 4 * 1024 * 1024)).map(ByteVector.view)

    def write(key: Key, data: ByteVector): ResultT[IO, Unit] =
      unsafe.withOutputStream(key)(Streams.writeBytes(_, data.toArray))

    def source(key: Key): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(FileSystem.get(conf).open(root </> keyToFilePath(key))).evalMap(_(1024 * 1024))

    def sink(key: Key): Sink[Task, ByteVector] =
      io.resource(Task.delay(new PipedOutputStream))(out => Task.delay(out.close))(
        out => io.resource(Task.delay(new PipedInputStream))(in => Task.delay(in.close))(
          in => Task.now((bytes: ByteVector) => Task.delay(out.write(bytes.toArray)))).toTask)
  }

  val strings: StoreStrings[ResultTIO] = new StoreStrings[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, String] =
      bytes.read(key).map(b => new String(b.toArray, codec.name))

    def write(key: Key, data: String, codec: Codec): ResultT[IO, Unit] =
      bytes.write(key, ByteVector.view(data.getBytes(codec.name)))
  }

  val utf8: StoreUtf8[ResultTIO] = new StoreUtf8[ResultTIO] {
    def read(key: Key): ResultT[IO, String] =
      strings.read(key, Codec.UTF8)

    def write(key: Key, data: String): ResultT[IO, Unit] =
      strings.write(key, data, Codec.UTF8)

    def source(key: Key): Process[Task, String] =
      bytes.source(key) |> scalaz.stream.text.utf8Decode

    def sink(key: Key): Sink[Task, String] =
      bytes.sink(key).map(_.contramap(s => ByteVector.view(s.getBytes("UTF-8"))))
  }

  val lines: StoreLines[ResultTIO] = new StoreLines[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, List[String]] =
      strings.read(key, codec).map(_.lines.toList)

    def write(key: Key, data: List[String], codec: Codec): ResultT[IO, Unit] =
      strings.write(key, Lists.prepareForFile(data), codec)

    def source(key: Key, codec: Codec): Process[Task, String] =
      scalaz.stream.io.linesR(FileSystem.get(conf).open(root </> keyToFilePath(key)))(codec)

    def sink(key: Key, codec: Codec): Sink[Task, String] =
      bytes.sink(key).map(_.contramap(s => ByteVector.view(s"$s\n".getBytes(codec.name))))
  }

  val linesUtf8: StoreLinesUtf8[ResultTIO] = new StoreLinesUtf8[ResultTIO] {
    def read(key: Key): ResultT[IO, List[String]] =
      lines.read(key, Codec.UTF8)

    def write(key: Key, data: List[String]): ResultT[IO, Unit] =
      lines.write(key, data, Codec.UTF8)

    def source(key: Key): Process[Task, String] =
      lines.source(key, Codec.UTF8)

    def sink(key: Key): Sink[Task, String] =
      lines.sink(key, Codec.UTF8)
  }

  def withInputStreamValue[A](key: Key)(f: InputStream => ResultT[IO, A]): ResultT[IO, A] =
    hdfs { Hdfs.readWith(root </> keyToFilePath(key), f) }

  val unsafe: StoreUnsafe[ResultTIO] = new StoreUnsafe[ResultTIO] {
    def withInputStream(key: Key)(f: InputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      withInputStreamValue[Unit](key)(f)

    def withOutputStream(key: Key)(f: OutputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      hdfs { Hdfs.writeWith(root </> keyToFilePath(key), f) }
  }

  def hdfs[A](thunk: => Hdfs[A]): ResultT[IO, A] =
    thunk.run(conf)

  private implicit def filePathToPath(f: FilePath): Path = new Path(f.path)
  private implicit def dirPathToPath(d: DirPath): Path = new Path(d.path)

  private def keyToFilePath(key: Key): FilePath =
    FilePath.unsafe(key.name)

  private def keyToDirPath(key: Key): DirPath =
    DirPath.unsafe(key.name)

  private def filePathToKey(path: FilePath): Key =
    Key(path.names.map(fn => KeyName.unsafe(fn.name)).toVector)

}
