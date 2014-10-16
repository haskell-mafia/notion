package com.ambiata.notion.core

import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.saws.core._
import com.ambiata.saws.s3._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import java.util.UUID
import java.io.{InputStream, OutputStream}
import java.io.{PipedInputStream, PipedOutputStream}
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, scalaz.stream._, scalaz.concurrent._, effect.IO, effect.Effect._
import scodec.bits.ByteVector

case class S3ReadOnlyStore(bucket: String, base: DirPath, client: AmazonS3Client) extends ReadOnlyStore[ResultTIO] {
  def list(prefix: Key): ResultT[IO, List[Key]] =
    S3.listKeysx(addressx(prefix)).executeT(client)
      .map(_.map(path => filePathToKey(FilePath.unsafe(path).relativeTo(base </> keyToDirPath(prefix)))))

  def listHeads(prefix: Key): ResultT[IO, List[Key]] =
    S3.listKeysHeadx(addressx(prefix)).executeT(client)
      .map(_.map(Key.unsafe))

  def filter(prefix: Key, predicate: Key => Boolean): ResultT[IO, List[Key]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: Key, predicate: Key => Boolean): ResultT[IO, Option[Key]] =
    list(prefix).map(_.find(predicate))

  def exists(key: Key): ResultT[IO, Boolean] =
    S3.exists(address(key)).executeT(client)

  def existsPrefix(prefix: Key): ResultT[IO, Boolean] =
    S3.existsPrefixx(addressx(prefix)).executeT(client)

  def checksum(key: Key, algorithm: ChecksumAlgorithm): ResultT[IO, Checksum] =
    S3.withStream(address(key), in => Checksum.stream(in, algorithm)).executeT(client)

  def copyTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    unsafe.withInputStream(src) { in =>
      store.unsafe.withOutputStream(dest) { out =>
        Streams.pipe(in, out) }}

  def mirrorTo(store: Store[ResultTIO], in: Key, out: Key): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU { source => copyTo(store, source, out / source) }
  } yield ()

  val bytes: StoreBytesRead[ResultTIO] = new StoreBytesRead[ResultTIO] {
    def read(key: Key): ResultT[IO, ByteVector] =
      s3 { S3.getBytes(address(key)).map(ByteVector.apply) }

    def source(key: Key): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(client.getObject(bucket, (base </> keyToDirPath(key)).path).getObjectContent).evalMap(_(1024 * 1024))
  }

  val strings: StoreStringsRead[ResultTIO] = new StoreStringsRead[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, String] =
      s3 { S3.getString(address(key), codec.name) }
  }

  val utf8: StoreUtf8Read[ResultTIO] = new StoreUtf8Read[ResultTIO] {
    def read(key: Key): ResultT[IO, String] =
      strings.read(key, Codec.UTF8)

    def source(key: Key): Process[Task, String] =
      bytes.source(key) |> scalaz.stream.text.utf8Decode
  }

  val lines: StoreLinesRead[ResultTIO] = new StoreLinesRead[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, List[String]] =
      strings.read(key, codec).map(_.lines.toList)

    def source(key: Key, codec: Codec): Process[Task, String] =
      scalaz.stream.io.linesR(client.getObject(bucket, (base </> keyToDirPath(key)).path).getObjectContent)(codec)
  }

  val linesUtf8: StoreLinesUtf8Read[ResultTIO] = new StoreLinesUtf8Read[ResultTIO] {
    def read(key: Key): ResultT[IO, List[String]] =
      lines.read(key, Codec.UTF8)

    def source(key: Key): Process[Task, String] =
      lines.source(key, Codec.UTF8)
  }

  val unsafe: StoreUnsafeRead[ResultTIO] = new StoreUnsafeRead[ResultTIO] {
    def withInputStream(key: Key)(f: InputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      ResultT.using(S3.getObject(address(key)).executeT(client).map(_.getObjectContent: InputStream))(f)
  }

  def s3[A](thunk: => S3Action[A]): ResultT[IO, A] =
    thunk.executeT(client)

  def address(prefix: Key): S3Address =
    S3Address(bucket, (base </> keyToDirPath(prefix)).path)

  /** Only to be used by deprecated Saws methods - removed when they are no longer used */
  private def addressx(prefix: Key): S3Address =
    S3Address(bucket, (base </> keyToDirPath(prefix)).path + "/")

  private def keyToDirPath(key: Key): DirPath =
    DirPath.unsafe(key.name)

  private def filePathToKey(filePath: FilePath): Key =
    new Key(filePath.names.map(n => KeyName.unsafe(n.name)).toVector)

}

case class S3Store(bucket: String, base: DirPath, client: AmazonS3Client, cache: DirPath) extends Store[ResultTIO] with ReadOnlyStore[ResultTIO] {
  val root: DirPath =
    DirPath.unsafe(bucket) </> base

  def local =
    PosixStore(cache)

  val readOnly: ReadOnlyStore[ResultTIO] =
    S3ReadOnlyStore(bucket, base, client)

  def list(prefix: Key): ResultT[IO, List[Key]] =
    readOnly.list(prefix)

  def listHeads(prefix: Key): ResultT[IO, List[Key]] =
    readOnly.listHeads(prefix)

  def filter(prefix: Key, predicate: Key => Boolean): ResultT[IO, List[Key]] =
    readOnly.filter(prefix, predicate)

  def find(prefix: Key, predicate: Key => Boolean): ResultT[IO, Option[Key]] =
    readOnly.find(prefix, predicate)

  def exists(key: Key): ResultT[IO, Boolean] =
    readOnly.exists(key)

  def existsPrefix(key: Key): ResultT[IO, Boolean] =
    readOnly.existsPrefix(key)

  def checksum(key: Key, algorithm: ChecksumAlgorithm): ResultT[IO, Checksum] =
    readOnly.checksum(key, algorithm)

  def delete(key: Key): ResultT[IO, Unit] =
    S3.deleteObject(S3Address(bucket, (base </> keyToDirPath(key)).path)).executeT(client)

  def deleteAll(prefix: Key): ResultT[IO, Unit] =
    list(prefix).flatMap(_.traverseU(delete)).void

  def move(in: Key, out: Key): ResultT[IO, Unit] =
    copy(in, out) >> delete(in)

  def moveTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copy(in: Key, out: Key): ResultT[IO, Unit] =
    S3.copyFile(S3Address(bucket, (base </> keyToFilePath(in)).path), S3Address(bucket, (base </> keyToFilePath(out)).path)).executeT(client).void

  def mirror(in: Key, out: Key): ResultT[IO, Unit] = for {
    paths <- list(in)
    _     <- paths.traverseU { source => copy(source, out / source) }
  } yield ()

  def copyTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    readOnly.copyTo(store, src, dest)

  def mirrorTo(store: Store[ResultTIO], in: Key, out: Key): ResultT[IO, Unit] =
    readOnly.mirrorTo(store, in, out)

  val bytes: StoreBytes[ResultTIO] = new StoreBytes[ResultTIO] {
    def read(key: Key): ResultT[IO, ByteVector] =
      readOnly.bytes.read(key)

    def source(key: Key): Process[Task, ByteVector] =
      readOnly.bytes.source(key)

    def write(key: Key, data: ByteVector): ResultT[IO, Unit] =
      s3 { S3.putBytes(S3Address(bucket, (base </> keyToDirPath(key)).path), data.toArray).void }

    def sink(key: Key): Sink[Task, ByteVector] =
      io.resource(Task.delay(new PipedOutputStream))(out => Task.delay(out.close))(
        out => io.resource(Task.delay(new PipedInputStream))(in => Task.delay(in.close))(
          in => Task.now((bytes: ByteVector) => Task.delay(out.write(bytes.toArray)))).toTask)
  }

  val strings: StoreStrings[ResultTIO] = new StoreStrings[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, String] =
      readOnly.strings.read(key, codec)

    def write(key: Key, data: String, codec: Codec): ResultT[IO, Unit] =
      s3 { S3.putString(S3Address(bucket, (base </> keyToDirPath(key)).path), data, codec.name).void }
  }

  val utf8: StoreUtf8[ResultTIO] = new StoreUtf8[ResultTIO] {
    def read(key: Key): ResultT[IO, String] =
      readOnly.utf8.read(key)

    def source(key: Key): Process[Task, String] =
      readOnly.utf8.source(key)

    def write(key: Key, data: String): ResultT[IO, Unit] =
      strings.write(key, data, Codec.UTF8)

    def sink(key: Key): Sink[Task, String] =
      bytes.sink(key).map(_.contramap(s => ByteVector.view(s.getBytes("UTF-8"))))
  }

  val lines: StoreLines[ResultTIO] = new StoreLines[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, List[String]] =
      readOnly.lines.read(key, codec)

    def source(key: Key, codec: Codec): Process[Task, String] =
      readOnly.lines.source(key, codec)

    def write(key: Key, data: List[String], codec: Codec): ResultT[IO, Unit] =
      strings.write(key, Lists.prepareForFile(data), codec)

    def sink(key: Key, codec: Codec): Sink[Task, String] =
      bytes.sink(key).map(_.contramap(s => ByteVector.view(s"$s\n".getBytes(codec.name))))
  }

  val linesUtf8: StoreLinesUtf8[ResultTIO] = new StoreLinesUtf8[ResultTIO] {
    def read(key: Key): ResultT[IO, List[String]] =
      readOnly.linesUtf8.read(key)

    def source(key: Key): Process[Task, String] =
      readOnly.linesUtf8.source(key)

    def write(key: Key, data: List[String]): ResultT[IO, Unit] =
      lines.write(key, data, Codec.UTF8)

    def sink(key: Key): Sink[Task, String] =
      lines.sink(key, Codec.UTF8)
  }

  val unsafe: StoreUnsafe[ResultTIO] = new StoreUnsafe[ResultTIO] {
    def withInputStream(key: Key)(f: InputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      readOnly.unsafe.withInputStream(key)(f)

    def withOutputStream(key: Key)(f: OutputStream => ResultT[IO, Unit]): ResultT[IO, Unit] = {
      val unique = Key.unsafe(UUID.randomUUID.toString)
      local.unsafe.withOutputStream(unique)(f) >> local.unsafe.withInputStream(unique)(in =>
        S3.putStream(S3Address(bucket, (base </> keyToDirPath(key)).path), in, {
          val metadata = S3.ServerSideEncryption
          metadata.setContentLength((local.root </> keyToFilePath(unique)).toFile.length)
          metadata
        }).executeT(client).void)
    }
  }

  def s3[A](thunk: => S3Action[A]): ResultT[IO, A] =
    thunk.executeT(client)

  private def keyToFilePath(key: Key): FilePath =
    FilePath.unsafe(key.name)

  private def keyToDirPath(key: Key): DirPath =
    DirPath.unsafe(key.name)
}

object S3Store {
  def createReadOnly(path: DirPath): S3Action[S3ReadOnlyStore] =
    createReadOnly(path.rootname.path, path.fromRoot)

  def createReadOnly(bucket: String, base: DirPath): S3Action[S3ReadOnlyStore] =
    S3Action.client.map(c => S3ReadOnlyStore(bucket, base, c))

  def create(path: DirPath, cache: DirPath): S3Action[S3Store] =
    create(path.rootname.path, path.fromRoot, cache)

  def create(bucket: String, base: DirPath, cache: DirPath): S3Action[S3Store] =
    S3Action.client.map(c => S3Store(bucket, base, c, cache))

}