package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import java.io.{InputStream, OutputStream}

import scala.io.Codec
import scalaz._, Scalaz._, scalaz.stream._, scalaz.concurrent._, effect.IO, effect.Effect._
import scodec.bits.ByteVector

// FIX pull out "derived" functions so the implementation can be shared with s3/hdfs impls.
case class PosixStore(root: DirPath) extends Store[RIO] with ReadOnlyStore[RIO] {
  def readOnly: ReadOnlyStore[RIO] =
    this

  def list(prefix: Key): RIO[List[Key]] = {
    val dirPrefix = root </> toDirPath(prefix)
    Directories.list(dirPrefix).map(_.map(_.relativeTo(root)).map(filePathToKey))
  }

  def listHeads(prefix: Key): RIO[List[Key]] = {
    val dirPrefix = root </> toDirPath(prefix)
    Directories.listFirstFileNames(dirPrefix).map(_.map(name => Key.unsafe(name.name)))
  }

  def filter(prefix: Key, predicate: Key => Boolean): RIO[List[Key]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: Key, predicate: Key => Boolean): RIO[Option[Key]] =
    list(prefix).map(_.find(predicate))

  def exists(key: Key): RIO[Boolean] =
    Files.exists(root </> toFilePath(key))

  def existsPrefix(key: Key): RIO[Boolean] =
    exists(key).flatMap(e => if (e) RIO.ok[Boolean](e) else Directories.exists(root </> toDirPath(key)))

  def delete(prefix: Key): RIO[Unit] =
    Files.delete(root </> toFilePath(prefix))

  def deleteAll(prefix: Key): RIO[Unit] =
    Directories.delete(root </> toDirPath(prefix)).void

  def move(in: Key, out: Key): RIO[Unit] =
    Files.move(root </> toFilePath(in), root </> toFilePath(out))

  def copy(in: Key, out: Key): RIO[Unit] =
    Files.copy(root </> toFilePath(in), root </> toFilePath(out))

  def mirror(in: Key, out: Key): RIO[Unit] = for {
    keys <- list(in)
    _    <- keys.traverseU { source => copy(source, out / source) }
  } yield ()

  def moveTo(store: Store[RIO], src: Key, dest: Key): RIO[Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copyTo(store: Store[RIO], src: Key, dest: Key): RIO[Unit] =
    unsafe.withInputStream(src) { in =>
      store.unsafe.withOutputStream(dest) { out =>
        Streams.pipe(in, out) }}

  def mirrorTo(store: Store[RIO], in: Key, out: Key): RIO[Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU { source => copyTo(store, source, out / source) }
  } yield ()

  def checksum(key: Key, algorithm: ChecksumAlgorithm): RIO[Checksum] =
    Checksum.file(root </> toFilePath(key), algorithm)

  val bytes: StoreBytes[RIO] = new StoreBytes[RIO] {
    def read(key: Key): RIO[ByteVector] =
      Files.readBytes(root </> toFilePath(key)).map(ByteVector.apply)

    def write(key: Key, data: ByteVector): RIO[Unit] =
      Files.writeBytes(root </> toFilePath(key), data.toArray)

    def source(key: Key): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(new java.io.FileInputStream((root </> toFilePath(key)).path)).evalMap(_(1024 * 1024))

    def sink(key: Key): Sink[Task, ByteVector] =
      scalaz.stream.io.chunkW(new java.io.FileOutputStream((root </> toFilePath(key)).path))
  }

  val strings: StoreStrings[RIO] = new StoreStrings[RIO] {
    def read(key: Key, codec: Codec): RIO[String] =
      Files.read(root </> toFilePath(key), codec.name)

    def write(key: Key, data: String, codec: Codec): RIO[Unit] =
      Files.write(root </> toFilePath(key), data, codec.name)
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
      scalaz.stream.io.linesR(new java.io.FileInputStream((root </> toFilePath(key)).path))(codec)

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

  val unsafe: StoreUnsafe[RIO] = new StoreUnsafe[RIO] {
    def withInputStream(key: Key)(f: InputStream => RIO[Unit]): RIO[Unit] =
      RIO.using((root </> toFilePath(key)).toInputStream)(f)

    def withOutputStream(key: Key)(f: OutputStream => RIO[Unit]): RIO[Unit] =
      Directories.mkdirs((root </> toFilePath(key)).dirname) >> RIO.using((root </> toFilePath(key)).toOutputStream)(f)
  }

  def toDirPath(key: Key): DirPath =
    DirPath.unsafe(key.components.map(_.name).mkString("/"))

  def toFilePath(key: Key): FilePath =
    toDirPath(key).toFilePath

  def filePathToKey(path: FilePath): Key =
    Key(path.names.map(fn => KeyName.unsafe(fn.name)).toVector)

  def dirPathToKey(path: DirPath): Key =
    Key(path.names.map(fn => KeyName.unsafe(fn.name)).toVector)
}
