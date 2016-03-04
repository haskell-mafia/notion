package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.data._
import com.ambiata.mundane.path._
import java.io.{InputStream, OutputStream}

import scala.io.Codec
import scalaz._, Scalaz._, effect.IO, effect.Effect._
import scodec.bits.ByteVector

// FIX pull out "derived" functions so the implementation can be shared with s3/hdfs impls.
case class PosixStore(root: LocalPath) extends Store[RIO] with ReadOnlyStore[RIO] {
  def readOnly: ReadOnlyStore[RIO] =
    this

  def list(prefix: Key): RIO[List[Key]] = {
    val p = keyToLocalPath(prefix)
    p.onExists(p.listFilesRecursively.map(files =>
      files.flatMap(_.toLocalPath.rebaseTo(root)).map(p => Key(p.path.names.toVector))), RIO.ok(Nil))
  }

  def filter(prefix: Key, predicate: Key => Boolean): RIO[List[Key]] =
    list(prefix).map(_.filter(predicate))

  def find(prefix: Key, predicate: Key => Boolean): RIO[Option[Key]] =
    list(prefix).map(_.find(predicate))

  /* Note: This was broken before, it would return true if Key was a directory */
  def exists(key: Key): RIO[Boolean] =
    keyToLocalPath(key).determine.map(_.cata(_.isLeft, false))

  def delete(key: Key): RIO[Unit] =
    keyToLocalPath(key).determineFile.flatMap(_.delete)

  def deleteAll(prefix: Key): RIO[Unit] =
    keyToLocalPath(prefix).delete

  def move(in: Key, out: Key): RIO[Unit] =
    keyToLocalPath(in).determinef(
      f => f.move(keyToLocalPath(out)).void
    , d => RIO.failIO(s"Can not move key, not an object. LocalDirectory(${d.path})").void)

  def copy(in: Key, out: Key): RIO[Unit] =
    keyToLocalPath(in).determinef(
      f => f.copy(keyToLocalPath(out)).void
    , d => RIO.failIO(s"Can not copy key, not an object. LocalDirectory(${d.path})").void)

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

  def checksum(key: Key, algorithm: ChecksumAlgorithm): RIO[Checksum] = {
    val p = keyToLocalPath(key)
    p.checksum(algorithm).flatMap(_.cata(
      RIO.ok
    , RIO.failIO(s"Key does not exist. LocalPath(${p.path})")))
  }

  val bytes: StoreBytes[RIO] = new StoreBytes[RIO] {
    def read(key: Key): RIO[ByteVector] = {
      val p = keyToLocalPath(key)
      p.readBytes.flatMap(_.cata(
        bs => RIO.ok(ByteVector(bs))
      , RIO.failIO(s"Key does not exist. LocalPath(${p.path})")))
    }

    def write(key: Key, data: ByteVector): RIO[Unit] =
      keyToLocalPath(key).writeBytes(data.toArray).void
  }

  val strings: StoreStrings[RIO] = new StoreStrings[RIO] {
    def read(key: Key, codec: Codec): RIO[String] = {
      val p = keyToLocalPath(key)
      p.readWithEncoding(codec).flatMap(_.cata(
        RIO.ok
      , RIO.failIO(s"Key does not exist. LocalPath(${p.path})")))
    }

    def write(key: Key, data: String, codec: Codec): RIO[Unit] =
      keyToLocalPath(key).writeWithEncoding(data, codec).void
  }

  val utf8: StoreUtf8[RIO] = new StoreUtf8[RIO] {
    def read(key: Key): RIO[String] =
      strings.read(key, Codec.UTF8)

    def write(key: Key, data: String): RIO[Unit] =
      strings.write(key, data, Codec.UTF8)
  }

  val lines: StoreLines[RIO] = new StoreLines[RIO] {
    def read(key: Key, codec: Codec): RIO[List[String]] =
      strings.read(key, codec).map(_.lines.toList)

    def write(key: Key, data: List[String], codec: Codec): RIO[Unit] =
      strings.write(key, Lists.prepareForFile(data), codec)
  }

  val linesUtf8: StoreLinesUtf8[RIO] = new StoreLinesUtf8[RIO] {
    def read(key: Key): RIO[List[String]] =
      lines.read(key, Codec.UTF8)

    def write(key: Key, data: List[String]): RIO[Unit] =
      lines.write(key, data, Codec.UTF8)
  }

  val unsafe: StoreUnsafe[RIO] = new StoreUnsafe[RIO] {
    def withInputStream(key: Key)(f: InputStream => RIO[Unit]): RIO[Unit] =
      keyToLocalPath(key).readUnsafe(f)

    /* TODO Should we add writeWith to LocalPath? */
    def withOutputStream(key: Key)(f: OutputStream => RIO[Unit]): RIO[Unit] = for {
      e <- exists(key)
      _ <- RIO.when(e, RIO.fail(s"Can not overwrite key ${key}"))
      p  = keyToLocalPath(key)
      _ <- p.dirname.mkdirs
      _ <- RIO.using(p.path.toOutputStream)(f)
    } yield ()
  }

  def keyToLocalPath(key: Key): LocalPath =
    LocalPath(Path.fromList(root.path, key.components.toList))
}
