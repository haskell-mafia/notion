package com.ambiata.notion.core

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}

import com.ambiata.mundane.control._
import com.ambiata.mundane.data._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import scodec.bits.ByteVector

import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

case class HdfsStore(conf: Configuration, root: HdfsPath) extends Store[RIO] with ReadOnlyStore[RIO] {
  def readOnly: ReadOnlyStore[RIO] =
    this

  def basePath: HPath =
    root.toHPath

  def list(prefix: Key): RIO[List[Key]] =
    hdfs {
      keyToHdfsPath(prefix).listFilesRecursively.map(files =>
        files.flatMap(_.toHdfsPath.rebaseTo(root)).map(hdfsPathToKey))
    }

  def filter(prefix: Key, predicate: Key => Boolean): RIO[List[Key]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: Key, predicate: Key => Boolean): RIO[Option[Key]] =
    list(prefix).map(_.find(predicate))

  /* Note: This was broken before, it would return true if Key was a directory */
  def exists(key: Key): RIO[Boolean] =
    hdfs { keyToHdfsPath(key).determine.map(_.cata(_.isLeft, false)) }

  def delete(key: Key): RIO[Unit] =
    hdfs { keyToHdfsPath(key).determineFile.flatMap(_.delete) }

  def deleteAll(prefix: Key): RIO[Unit] =
    hdfs { keyToHdfsPath(prefix).delete }

  /* Note: This use to work on dirs also, but it shouldn't have. */
  def move(in: Key, out: Key): RIO[Unit] =
    hdfs {
      keyToHdfsPath(in).determinef(
        f => f.move(keyToHdfsPath(out)).void
      , d => Hdfs.fail(s"Can not move key, not an object. ${d}").void)
    }

  /* Note: This use to work on dirs also, but it shouldn't have. */
  def copy(in: Key, out: Key): RIO[Unit] =
    hdfs {
      keyToHdfsPath(in).determinef(
        f => f.copy(keyToHdfsPath(out)).void
      , d => Hdfs.fail(s"Can not copy key, not an object. ${d}").void)
    }

  def mirror(in: Key, out: Key): RIO[Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU(source => copy(source, out / source))
  } yield ()

  def moveTo(store: Store[RIO], src: Key, dest: Key): RIO[Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copyTo(store: Store[RIO], src: Key, dest: Key): RIO[Unit] =
    unsafe.withInputStream(src) { in =>
      store.unsafe.withOutputStream(dest) { out =>
        Streams.pipe(in, out) }}

  def mirrorTo(store: Store[RIO], in: Key, out: Key): RIO[Unit] = for {
    keys <- list(in)
    _    <- keys.traverseU(source => copyTo(store, source, out / source))
  } yield ()

  def checksum(key: Key, algorithm: ChecksumAlgorithm): RIO[Checksum] =
    hdfs {
      val p = keyToHdfsPath(key)
      p.checksum(algorithm).flatMap(_.cata(
        Hdfs.ok
      , Hdfs.fail(s"Key does not exist. HdfsPath(${p.path})")))
    }

  val bytes: StoreBytes[RIO] = new StoreBytes[RIO] {
    def read(key: Key): RIO[ByteVector] =
      withInputStreamValue[Array[Byte]](key)(Streams.readBytes(_, 4 * 1024 * 1024)).map(ByteVector.view)

    def write(key: Key, data: ByteVector): RIO[Unit] =
      unsafe.withOutputStream(key)(Streams.writeBytes(_, data.toArray))

    def source(key: Key): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(FileSystem.get(conf).open(keyToHdfsPath(key).toHPath)).evalMap(_(1024 * 1024))

    def sink(key: Key): Sink[Task, ByteVector] =
      io.resource(Task.delay(new PipedOutputStream))(out => Task.delay(out.close))(
        out => io.resource(Task.delay(new PipedInputStream))(in => Task.delay(in.close))(
          in => Task.now((bytes: ByteVector) => Task.delay(out.write(bytes.toArray)))).toTask)
  }

  val strings: StoreStrings[RIO] = new StoreStrings[RIO] {
    def read(key: Key, codec: Codec): RIO[String] =
      bytes.read(key).map(b => new String(b.toArray, codec.name))

    def write(key: Key, data: String, codec: Codec): RIO[Unit] =
      bytes.write(key, ByteVector.view(data.getBytes(codec.name)))
  }

  val utf8: StoreUtf8[RIO] = new StoreUtf8[RIO] {
    def read(key: Key): RIO[String] =
      strings.read(key, Codec.UTF8)

    def write(key: Key, data: String): RIO[Unit] =
      strings.write(key, data, Codec.UTF8)

    def source(key: Key): Process[Task, String] =
      bytes.source(key) |> scalaz.stream.text.utf8Decode

    def sink(key: Key): Sink[Task, String] =
      bytes.sink(key).map(_.contramap(s => ByteVector.view(s.getBytes("UTF-8"))))
  }

  val lines: StoreLines[RIO] = new StoreLines[RIO] {
    def read(key: Key, codec: Codec): RIO[List[String]] =
      strings.read(key, codec).map(_.lines.toList)

    def write(key: Key, data: List[String], codec: Codec): RIO[Unit] =
      strings.write(key, Lists.prepareForFile(data), codec)

    def source(key: Key, codec: Codec): Process[Task, String] =
      scalaz.stream.io.linesR(FileSystem.get(conf).open(keyToHdfsPath(key).toHPath))(codec)

    def sink(key: Key, codec: Codec): Sink[Task, String] =
      bytes.sink(key).map(_.contramap(s => ByteVector.view(s"$s\n".getBytes(codec.name))))
  }

  val linesUtf8: StoreLinesUtf8[RIO] = new StoreLinesUtf8[RIO] {
    def read(key: Key): RIO[List[String]] =
      lines.read(key, Codec.UTF8)

    def write(key: Key, data: List[String]): RIO[Unit] =
      lines.write(key, data, Codec.UTF8)

    def source(key: Key): Process[Task, String] =
      lines.source(key, Codec.UTF8)

    def sink(key: Key): Sink[Task, String] =
      lines.sink(key, Codec.UTF8)
  }

  def withInputStreamValue[A](key: Key)(f: InputStream => RIO[A]): RIO[A] =
    hdfs { keyToHdfsPath(key).readWith(i => Hdfs.fromRIO(f(i))) }

  val unsafe: StoreUnsafe[RIO] = new StoreUnsafe[RIO] {
    def withInputStream(key: Key)(f: InputStream => RIO[Unit]): RIO[Unit] =
      withInputStreamValue[Unit](key)(f)

    def withOutputStream(key: Key)(f: OutputStream => RIO[Unit]): RIO[Unit] =
      hdfs { keyToHdfsPath(key).writeWith(o => Hdfs.fromRIO(f(o))) }
  }

  def hdfs[A](thunk: => Hdfs[A]): RIO[A] =
    thunk.run(conf)

  /* WARNING: This is a lossy operation, empty components will be dropped */
  private def keyToHdfsPath(key: Key): HdfsPath =
    root / HdfsPath.fromString(key.name).path

  /* WARNING: This is a lossy operation */
  private def hdfsPathToKey(p: HdfsPath): Key =
    Key(p.path.names.map(KeyName.fromComponent).toVector)
}
