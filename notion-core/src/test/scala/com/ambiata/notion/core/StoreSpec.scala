package com.ambiata.notion.core

import com.ambiata.mundane.control.{Result => _, _}
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing._
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._
import org.specs2.matcher.Parameters
import scodec.bits.ByteVector
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._

class StoreSpec extends Specification with ScalaCheck { def is = s2"""
  Store Usage
  ===========

  list all keys                                   $list
  list all keys from a key prefix                 $listFromPrefix
  filter listed paths                             $filter
  find path in root (thirdish)                    $find
  find path in root (first)                       $findfirst
  find path in root (last)                        $findlast

  exists                                          $exists
  not exists                                      $notExists

  delete                                          $delete
  deleteAll                                       $deleteAll

  move                                            $move
  move and read                                   $moveRead
  move fails for non file/object                  $moveFail
  copy                                            $copy
  copy and read                                   $copyRead
  copy fails for non file/object                  $copyFail
  mirror                                          $mirror

  moveTo                                          $moveTo
  copyTo                                          $copyTo
  mirrorTo                                        $mirrorTo

  checksum                                        $checksum
  checksum fails for non file/object              $checksumFail
  read / write bytes                              $bytes
  read / write strings                            $strings
  read / write utf8 strings                       $utf8Strings
  read / write lines                              $lines
  read / write utf8 lines                         $utf8Lines

  input stream                                    $inputStream
  output stream                                   $outputStream
  """

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 15, workers = 1, maxDiscardRatio = 100)

  def full(keys: Keys): List[Key] =
    keys.keys.map(_.full.toKey)

  def writeKeys(keys: Keys, store: Store[RIO]): RIO[Unit] =
    keys.keys.traverseU(e => store.utf8.write((e.path + "/" + e.value).toKey, e.value.toString)).void

  def list = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    l <- s.listAll
  } yield l.sorted ==== full(keys).sorted)

  def listFromPrefix = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys.map(_ prepend "sub"), s)
    l <- s.list(Key.Root / "sub")
  } yield l.sorted ==== full(keys.map(_ prepend "sub")).sorted)

  def filter = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    f = full(keys)
    l <- s.filterAll(x => x == f.head || x == f.last)
    e = if (f.head == f.last) List(f.head) else List(f.head, f.last)
  } yield l.sorted ==== e.sorted)

  def find = propNoShrink((keys: Keys, st: StoreTemporary) => keys.keys.length >= 3 ==> (for {
    s <- st.store
    _ <- writeKeys(keys, s)
    l <- s.findAll(_ == full(keys).drop(2).head)
  } yield l ==== full(keys).drop(2).head.some))

  def findfirst = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    l <- s.findAll(_ == full(keys).head)
  } yield l ==== full(keys).head.some)

  def findlast = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    l <- s.findAll(_ == full(keys).last)
  } yield l ==== full(keys).last.some)

  def exists = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    l <- full(keys).traverseU(s.exists)
  } yield l ==== l.map(_ => true))

  def notExists = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    e <- s.exists("root" / "missing")
  } yield e ==== false)

  def delete = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    _ <- s.delete(full(keys).head)
    l <- full(keys).traverseU(s.exists)
  } yield l ==== false :: l.drop(1).map(_ => true))

  def deleteAll = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    _ <- s.deleteAllFromRoot
    l <- full(keys).traverseU(s.exists)
  } yield l ==== l.map(_ => false))

  def move = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.move(m.full.toKey, n.full.toKey)
    e <- s.exists(m.full.toKey).zip(s.exists(n.full.toKey))
  } yield e ==== false -> true)

  def moveRead = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.move(m.full.toKey, n.full.toKey)
    d <- s.utf8.read(n.full.toKey)
  } yield d ==== m.value.toString)

  def moveFail = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary) => (for {
    s <- st.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.move(m.path.toKey, n.path.toKey)
  } yield ()) must beFail)

  def copy = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.copy(m.full.toKey, n.full.toKey)
    e <- s.exists(m.full.toKey).zip(s.exists(n.full.toKey))
  } yield e ==== true -> true)

  def copyRead = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.copy(m.full.toKey, n.full.toKey)
    b <- s.utf8.read(m.full.toKey)
    a <- s.utf8.read(n.full.toKey)
  } yield b ==== a)

  def copyFail = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary) => (for {
    s <- st.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.copy(m.path.toKey, n.path.toKey)
  } yield ()) must beFail)

  def mirror = propNoShrink((keys: Keys, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(keys, s)
    _ <- s.mirror(Key.Root, Key("mirror"))
    l <- s.list(Key("mirror"))
  } yield l.sorted ==== full(keys).map(_.prepend("mirror")).sorted)

  def moveTo = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary, alt: StoreTemporary) => for {
    s <- st.store
    t <- alt.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.moveTo(t, m.full.toKey, n.full.toKey)
    e <- s.exists(m.full.toKey)
    a <- t.exists(n.full.toKey)
  } yield e -> a ==== false -> true)

  def copyTo = propNoShrink((m: KeyEntry, n: KeyEntry, st: StoreTemporary, alt: StoreTemporary) => for {
    s <- st.store
    t <- alt.store
    _ <- writeKeys(Keys(m :: Nil), s)
    _ <- s.copyTo(t, m.full.toKey, n.full.toKey)
    e <- s.exists(m.full.toKey)
    a <- t.exists(n.full.toKey)
  } yield e -> a ==== true -> true)

  def mirrorTo = propNoShrink((keys: Keys, st: StoreTemporary, alt: StoreTemporary) => for {
    s <- st.store
    t <- alt.store
    _ <- writeKeys(keys, s)
    _ <- s.mirrorTo(t, Key.Root, Key("mirror"))
    l <- t.list(Key("mirror"))
  } yield l.sorted ==== full(keys).map(_.prepend("mirror")).sorted)

  def checksum = propNoShrink((m: KeyEntry, st: StoreTemporary) => for {
    s <- st.store: RIO[Store[RIO]]
    _ <- writeKeys(Keys(m :: Nil), s): RIO[Unit]
    r <- s.checksum(m.full.toKey, MD5): RIO[Checksum]
  } yield r ==== Checksum.string(m.value.toString, MD5))

  def checksumFail = propNoShrink((m: KeyEntry, st: StoreTemporary) => (for {
    s <- st.store: RIO[Store[RIO]]
    _ <- writeKeys(Keys(m :: Nil), s): RIO[Unit]
    r <- s.checksum(m.path.toKey, MD5): RIO[Checksum]
  } yield ()) must beFail)

  def bytes = propNoShrink((m: KeyEntry, bytes: Array[Byte], st: StoreTemporary) => for {
    s <- st.store
    _ <- s.bytes.write(m.full.toKey, ByteVector(bytes))
    r <- s.bytes.read(m.full.toKey)
  } yield r ==== ByteVector(bytes))

  def strings = propNoShrink((m: KeyEntry, str: String, st: StoreTemporary) => for {
    s <- st.store
    _ <- s.strings.write(m.full.toKey, str, Codec.UTF8)
    r <- s.strings.read(m.full.toKey, Codec.UTF8)
  } yield r ==== str)

  def utf8Strings = propNoShrink((m: KeyEntry, str: String, st: StoreTemporary) => for {
    s <- st.store
    _ <- s.utf8.write(m.full.toKey, str)
    r <- s.utf8.read(m.full.toKey)
  } yield r ==== str)

  def lines = propNoShrink((m: KeyEntry, v: List[Int], st: StoreTemporary) => for {
    s <- st.store
    _ <- s.lines.write(m.full.toKey, v.map(_.toString), Codec.UTF8)
    r <- s.lines.read(m.full.toKey, Codec.UTF8)
  } yield r ==== v.map(_.toString))

  def utf8Lines = propNoShrink((m: KeyEntry, v: List[Int], st: StoreTemporary) => for {
    s <- st.store
    _ <- s.linesUtf8.write(m.full.toKey, v.map(_.toString))
    r <- s.linesUtf8.read(m.full.toKey)
  } yield r ==== v.map(_.toString))

  def inputStream = propNoShrink((m: KeyEntry, st: StoreTemporary) => for {
    s <- st.store
    _ <- writeKeys(Keys(m :: Nil), s)
    b = new StringBuilder
    _ <- s.unsafe.withInputStream(m.full.toKey)(Streams.read(_).map(b.append(_)).void)
  } yield b.toString ==== m.value.toString)

  def outputStream = propNoShrink((m: KeyEntry, data: String, st: StoreTemporary) => for {
    s <- st.store
    _ <- s.unsafe.withOutputStream(m.full.toKey)(o => Streams.write(o, data))
    d <- s.utf8.read(m.full.toKey)
  } yield d ==== data)

  implicit class ToKey(s: String) {
    def toKey: Key = Key.unsafe(s)
  }
}
