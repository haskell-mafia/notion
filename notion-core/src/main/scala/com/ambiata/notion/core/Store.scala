package com.ambiata.notion.core

import com.ambiata.mundane.io._
import java.io.{InputStream, OutputStream}

import scala.io.Codec
import scodec.bits.ByteVector

trait Store[F[_]] extends WriteOnlyStore[F] with ReadOnlyStore[F] {
  val bytes: StoreBytes[F]
  val strings: StoreStrings[F]
  val utf8: StoreUtf8[F]
  val lines: StoreLines[F]
  val linesUtf8: StoreLinesUtf8[F]
  val unsafe: StoreUnsafe[F]
}

trait WriteOnlyStore[F[_]] {
  def delete(key: Key): F[Unit]
  def deleteAll(prefix: Key): F[Unit]
  def deleteAllFromRoot: F[Unit] = deleteAll(Key.Root)

  def move(in: Key, out: Key): F[Unit]
  def moveTo(store: Store[F], in: Key, out: Key): F[Unit]

  def copy(in: Key, out: Key): F[Unit]

  val bytes: StoreBytesWrite[F]
  val strings: StoreStringsWrite[F]
  val utf8: StoreUtf8Write[F]
  val lines: StoreLinesWrite[F]
  val linesUtf8: StoreLinesUtf8Write[F]
  val unsafe: StoreUnsafeWrite[F]
}

trait ReadOnlyStore[F[_]] {
  def listAll: F[List[Key]] = list(Key.Root)
  def list(prefix: Key): F[List[Key]]

  def filterAll(predicate: Key => Boolean): F[List[Key]] = filter(Key.Root, predicate)
  def filter(prefix: Key, predicate: Key => Boolean): F[List[Key]]

  def findAll(predicate: Key => Boolean): F[Option[Key]] = find(Key.Root, predicate)
  def find(prefix: Key, predicate: Key => Boolean): F[Option[Key]]

  def exists(key: Key): F[Boolean]

  def copyTo(store: Store[F], in: Key, out: Key): F[Unit]

  def checksum(key: Key, algorithm: ChecksumAlgorithm): F[Checksum]

  val bytes: StoreBytesRead[F]
  val strings: StoreStringsRead[F]
  val utf8: StoreUtf8Read[F]
  val lines: StoreLinesRead[F]
  val linesUtf8: StoreLinesUtf8Read[F]
  val unsafe: StoreUnsafeRead[F]
}

trait StoreBytes[F[_]] extends StoreBytesRead[F] with StoreBytesWrite[F]

trait StoreBytesRead[F[_]] {
  def read(key: Key): F[ByteVector]
}

trait StoreBytesWrite[F[_]] {
  def write(key: Key, data: ByteVector): F[Unit]
}

trait StoreStrings[F[_]] extends StoreStringsRead[F] with StoreStringsWrite[F]

trait StoreStringsRead[F[_]] {
  def read(key: Key, codec: Codec): F[String]
}

trait StoreStringsWrite[F[_]] {
  def write(key: Key, data: String, codec: Codec): F[Unit]
}

trait StoreUtf8[F[_]] extends StoreUtf8Read[F] with StoreUtf8Write[F]

trait StoreUtf8Read[F[_]] {
  def read(key: Key): F[String]
}

trait StoreUtf8Write[F[_]] {
  def write(key: Key, data: String): F[Unit]
}

trait StoreLines[F[_]] extends StoreLinesRead[F] with StoreLinesWrite[F]

trait StoreLinesRead[F[_]] {
  def read(key: Key, codec: Codec): F[List[String]]
}

trait StoreLinesWrite[F[_]] {
  def write(key: Key, data: List[String], codec: Codec): F[Unit]
}

trait StoreLinesUtf8[F[_]] extends StoreLinesUtf8Read[F] with StoreLinesUtf8Write[F]

trait StoreLinesUtf8Read[F[_]]  {
  def read(key: Key): F[List[String]]
}

trait StoreLinesUtf8Write[F[_]]  {
  def write(key: Key, data: List[String]): F[Unit]
}

trait StoreUnsafe[F[_]] extends StoreUnsafeRead[F] with StoreUnsafeWrite[F]

trait StoreUnsafeRead[F[_]] {
  def withInputStream(key: Key)(f: InputStream => F[Unit]): F[Unit]
}

trait StoreUnsafeWrite[F[_]] {
  def withOutputStream(key: Key)(f: OutputStream => F[Unit]): F[Unit]
}
