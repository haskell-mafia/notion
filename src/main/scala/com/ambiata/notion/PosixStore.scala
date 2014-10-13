package com.ambiata.notion

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import java.io.{InputStream, OutputStream}
import scala.io.Codec
import scalaz._, Scalaz._, scalaz.stream._, scalaz.concurrent._, effect.IO, effect.Effect._
import scodec.bits.ByteVector

// FIX pull out "derived" functions so the implementation can be shared with s3/hdfs impls.
case class PosixStore(root: DirPath) extends Store[ResultTIO] with ReadOnlyStore[ResultTIO] {
  def readOnly: ReadOnlyStore[ResultTIO] =
    this

  def list(prefix: Key): ResultT[IO, List[Key]] = {
    val dirPrefix = root </> toDirPath(prefix)
    Directories.list(dirPrefix).map(_.map(_.relativeTo(dirPrefix)).map(filePathToKey))
  }

  def listHeads(prefix: Key): ResultT[IO, List[Key]] = {
    val dirPrefix = root </> toDirPath(prefix)
    Directories.listFirstFileNames(dirPrefix).map(_.map(name => Key.unsafe(name.name)))
  }

  def filter(prefix: Key, predicate: Key => Boolean): ResultT[IO, List[Key]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: Key, predicate: Key => Boolean): ResultT[IO, Option[Key]] =
    list(prefix).map(_.find(predicate))

  def exists(key: Key): ResultT[IO, Boolean] =
    Files.exists(root </> toFilePath(key))

  def existsPrefix(key: Key): ResultT[IO, Boolean] =
    exists(key).flatMap(e => if (e) ResultT.ok[IO, Boolean](e) else Directories.exists(root </> toDirPath(key)))

  def delete(prefix: Key): ResultT[IO, Unit] =
    Files.delete(root </> toFilePath(prefix))

  def deleteAll(prefix: Key): ResultT[IO, Unit] =
    Directories.delete(root </> toDirPath(prefix)).void

  def move(in: Key, out: Key): ResultT[IO, Unit] =
    Files.move(root </> toFilePath(in), root </> toFilePath(out))

  def copy(in: Key, out: Key): ResultT[IO, Unit] =
    Files.copy(root </> toFilePath(in), root </> toFilePath(out))

  def mirror(in: Key, out: Key): ResultT[IO, Unit] = for {
    keys <- list(in)
    _    <- keys.traverseU { source => copy(source, out / source) }
  } yield ()

  def moveTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copyTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    unsafe.withInputStream(src) { in =>
      store.unsafe.withOutputStream(dest) { out =>
        Streams.pipe(in, out) }}

  def mirrorTo(store: Store[ResultTIO], in: Key, out: Key): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU { source => copyTo(store, source, out / source) }
  } yield ()

  def checksum(key: Key, algorithm: ChecksumAlgorithm): ResultT[IO, Checksum] =
    Checksum.file(root </> toFilePath(key), algorithm)

  val bytes: StoreBytes[ResultTIO] = new StoreBytes[ResultTIO] {
    def read(key: Key): ResultT[IO, ByteVector] =
      Files.readBytes(root </> toFilePath(key)).map(ByteVector.apply)

    def write(key: Key, data: ByteVector): ResultT[IO, Unit] =
      Files.writeBytes(root </> toFilePath(key), data.toArray)

    def source(key: Key): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(new java.io.FileInputStream((root </> toFilePath(key)).path)).evalMap(_(1024 * 1024))

    def sink(key: Key): Sink[Task, ByteVector] =
      scalaz.stream.io.chunkW(new java.io.FileOutputStream((root </> toFilePath(key)).path))
  }

  val strings: StoreStrings[ResultTIO] = new StoreStrings[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, String] =
      Files.read(root </> toFilePath(key), codec.name)

    def write(key: Key, data: String, codec: Codec): ResultT[IO, Unit] =
      Files.write(root </> toFilePath(key), data, codec.name)
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
      scalaz.stream.io.linesR(new java.io.FileInputStream((root </> toFilePath(key)).path))(codec)

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

  val unsafe: StoreUnsafe[ResultTIO] = new StoreUnsafe[ResultTIO] {
    def withInputStream(key: Key)(f: InputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      ResultT.using((root </> toFilePath(key)).toInputStream)(f)

    def withOutputStream(key: Key)(f: OutputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      Directories.mkdirs((root </> toFilePath(key)).dirname) >> ResultT.using((root </> toFilePath(key)).toOutputStream)(f)
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
