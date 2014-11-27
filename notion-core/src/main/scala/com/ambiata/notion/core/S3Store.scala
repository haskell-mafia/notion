package com.ambiata.notion.core

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
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

case class S3ReadOnlyStore(s3: S3Prefix, client: AmazonS3Client) extends ReadOnlyStore[ResultTIO] {
  def list(prefix: Key): ResultT[IO, List[Key]] =
    run { (s3 / prefix.name).listPrefix.map(_.map( p => {
      new Key(p.removeCommonPrefix(s3).cata(_.split(S3Operations.DELIMITER).toList, Nil).map(KeyName.unsafe).toVector)
    }))}

  def listHeads(prefix: Key): ResultT[IO, List[Key]] =
    run { (s3 / prefix.name).listKeysHead.map(_.map(Key.unsafe)) }

  def filter(prefix: Key, predicate: Key => Boolean): ResultT[IO, List[Key]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: Key, predicate: Key => Boolean): ResultT[IO, Option[Key]] =
    list(prefix).map(_.find(predicate))

  def exists(key: Key): ResultT[IO, Boolean] =
    run { (s3 | key.name).exists }

  def existsPrefix(prefix: Key): ResultT[IO, Boolean] =
    run { (s3 / prefix.name).exists }

  def checksum(key: Key, algorithm: ChecksumAlgorithm): ResultT[IO, Checksum] =
    run { (s3 | key.name).withStream(in => Checksum.stream(in, algorithm)) }

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
      run { (s3 | key.name).getBytes.map(ByteVector.apply) }

    def source(key: Key): Process[Task, ByteVector] =
      scalaz.stream.io.chunkR(client.getObject(s3.bucket, (s3 | key.name).key).getObjectContent).evalMap(_(1024 * 1024))
  }

  val strings: StoreStringsRead[ResultTIO] = new StoreStringsRead[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, String] =
      run { (s3 | key.name).getWithEncoding(codec) }
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
      scalaz.stream.io.linesR(client.getObject(s3.bucket, (s3 | key.name).key).getObjectContent)(codec)
  }

  val linesUtf8: StoreLinesUtf8Read[ResultTIO] = new StoreLinesUtf8Read[ResultTIO] {
    def read(key: Key): ResultT[IO, List[String]] =
      lines.read(key, Codec.UTF8)

    def source(key: Key): Process[Task, String] =
      lines.source(key, Codec.UTF8)
  }

  val unsafe: StoreUnsafeRead[ResultTIO] = new StoreUnsafeRead[ResultTIO] {
    def withInputStream(key: Key)(f: InputStream => ResultT[IO, Unit]): ResultT[IO, Unit] =
      ResultT.using(run { (s3 | key.name).getObject.map(_.getObjectContent: InputStream) })(f)
  }

  def run[A](thunk: => S3Action[A]): ResultT[IO, A] =
    thunk.executeT(client)

}

case class S3Store(s3: S3Prefix, client: AmazonS3Client, cache: DirPath) extends Store[ResultTIO] with ReadOnlyStore[ResultTIO] {
  val root: S3Prefix =
    s3

  def local =
    PosixStore(cache)

  val readOnly: ReadOnlyStore[ResultTIO] =
    S3ReadOnlyStore(s3, client)

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
    run { (s3 | key.name).delete }

  def deleteAll(prefix: Key): ResultT[IO, Unit] =
    list(prefix).flatMap(_.traverseU(delete)).void

  def move(in: Key, out: Key): ResultT[IO, Unit] =
    copy(in, out) >> delete(in)

  def moveTo(store: Store[ResultTIO], src: Key, dest: Key): ResultT[IO, Unit] =
    copyTo(store, src, dest) >> delete(src)

  def copy(in: Key, out: Key): ResultT[IO, Unit] =
    run { (s3 | in.name).copy(s3 | out.name).void }

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
      run { (s3 | key.name).putBytes(data.toArray).void }

    def sink(key: Key): Sink[Task, ByteVector] =
      io.resource(Task.delay(new PipedOutputStream))(out => Task.delay(out.close))(
        out => io.resource(Task.delay(new PipedInputStream))(in => Task.delay(in.close))(
          in => Task.now((bytes: ByteVector) => Task.delay(out.write(bytes.toArray)))).toTask)
  }

  val strings: StoreStrings[ResultTIO] = new StoreStrings[ResultTIO] {
    def read(key: Key, codec: Codec): ResultT[IO, String] =
      readOnly.strings.read(key, codec)

    def write(key: Key, data: String, codec: Codec): ResultT[IO, Unit] =
      run { (s3 | key.name).putWithEncoding(data, codec).void }
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
        run { (s3 | key.name).putStreamWithMetadata(in, S3Address.ReadLimitDefault, {
          val metadata = S3.ServerSideEncryption
          metadata.setContentLength((local.root </> keyToFilePath(unique)).toFile.length)
          metadata
        }).void })
    }
  }

  def run[A](thunk: => S3Action[A]): ResultT[IO, A] =
    thunk.executeT(client)

  private def keyToFilePath(key: Key): FilePath =
    FilePath.unsafe(key.name)
}

object S3Store {
  def createReadOnly(s3: S3Prefix): S3Action[ReadOnlyStore[ResultTIO]] =
    S3Action.client.map(c => S3ReadOnlyStore(s3, c))

  def create(s3: S3Prefix, cache: DirPath): S3Action[Store[ResultTIO]] =
    S3Action.client.map(c => S3Store(s3, c, cache))

}
