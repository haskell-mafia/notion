package com.ambiata.notion.store

import com.ambiata.notion.store.Key

import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, effect.IO
import scodec.bits.ByteVector
import org.specs2._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing._, ResultTIOMatcher._
import java.util.UUID

// FIX Workout how this test can be pulled out and shared with posix/s3/hdfs.
class PosixStoreSpec extends Specification with ScalaCheck { def is = isolated ^ s2"""
  Posix Store Usage
  =================

  list all keys                                   $list
  list all keys from a key prefix                 $listFromPrefix
  list all keys from a key prefix when empty      $listFromPrefixEmpty
  list all direct prefixes from a key prefix      $listHeadPrefixes
  filter listed keys                              $filter
  find path in root (thirdish)                    $find
  find path in root (first)                       $findfirst
  find path in root (last)                        $findlast

  exists                                          $exists
  existsPrefix                                    $existsPrefix
  not exists                                      $notExists

  delete                                          $delete
  deleteAll                                       $deleteAll

  move                                            $move
  move and read                                   $moveRead
  copy                                            $copy
  copy and read                                   $copyRead
  mirror                                          $mirror

  moveTo                                          $moveTo
  copyTo                                          $copyTo
  mirrorTo                                        $mirrorTo

  checksum                                        $checksum
  read / write bytes                              $bytes
  read / write strings                            $strings
  read / write utf8 strings                       $utf8Strings
  read / write lines                              $lines
  read / write utf8 lines                         $utf8Lines

  """

  val tmp1 = DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> FileName.unsafe(s"StoreSpec.${UUID.randomUUID}")
  val tmp2 = DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> FileName.unsafe(s"StoreSpec.${UUID.randomUUID}")
  val store = PosixStore(tmp1)
  val alternate = PosixStore(tmp2)

  def list =
    prop((keys: Keys) => clean(keys) { keys =>
       store.listAll must beOkLike((_:List[Key]).toSet must_== keys.toSet) })

  def listFromPrefix =
    prop((keys: Keys) => clean(keys.map(_ prepend "sub")) { keys =>
      store.list(Key.Root / "sub") must beOkLike((_:List[Key]).toSet must_== keys.map(_.fromRoot).toSet) })

  def listFromPrefixEmpty =
    prop { keys: Keys =>
      val action =
        Directories.mkdirs(store.root </> "sub") >>
        store.list(Key.Root / "sub")
      action must beOkLike((_:List[Key]) must beEmpty)
    }

  def listHeadPrefixes =
    prop((keys: Keys) => clean(keys.map(_ prepend "sub")) { keys =>
      store.listHeads(Key.Root / "sub") must beOkLike((_:List[Key]).toSet must_== keys.map(_.fromRoot.head).toSet) })

  def filter =
    prop((keys: Keys) => clean(keys) { keys =>
      val first = keys.head
      val last = keys.last
      val expected = if (first == last) List(first) else List(first, last)
      store.filterAll(x => x == first || x == last) must beOkLike(keys => keys must contain(allOf(expected:_*))) })

  def find =
    prop((keys: Keys) => keys.keys.length >= 3 ==> { clean(keys) { keys =>
      val third = keys.drop(2).head
      store.findAll(_ == third) must beOkValue(Some(third)) } })

  def findfirst =
    prop((keys: Keys) => clean(keys) { keys =>
      store.findAll(x => x == keys.head) must beOkValue(Some(keys.head)) })

  def findlast =
    prop((keys: Keys) => clean(keys) { keys =>
      store.findAll(x => x == keys.last) must beOkValue(Some(keys.last)) })

  def exists =
    prop((keys: Keys) => clean(keys) { keys =>
      keys.traverseU(store.exists) must beOkLike(_.forall(identity)) })

  def existsPrefix =
    prop((keys: Keys) => clean(keys) { keys =>
      keys.traverseU(store.existsPrefix) must beOkLike(_.forall(identity)) })

  def notExists =
    prop((keys: Keys) => store.exists("root" / "missing") must beOkValue(false))

  def delete =
    prop((keys: Keys) => clean(keys) { keys =>
      val first = keys.head
      (store.delete(first) >> keys.traverseU(store.exists)) must beOkLike(x => !x.head && x.tail.forall(identity)) })

  def deleteAll =
    prop((keys: Keys) => clean(keys) { keys =>
      (store.deleteAllFromRoot >> keys.traverseU(store.exists)) must beOkLike(x => !x.tail.exists(identity)) })

  def move =
    prop((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.move(m.full.toKey, n.full.toKey) >>
       store.exists(m.full.toKey).zip(store.exists(n.full.toKey))) must beOkValue(false -> true) })

  def moveRead =
    prop((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.move(m.full.toKey, n.full.toKey) >>
       store.utf8.read(n.full.toKey)) must beOkValue(m.value.toString) })

  def copy =
    prop((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.copy(m.full.toKey, n.full.toKey) >>
       store.exists(m.full.toKey).zip(store.exists(n.full.toKey))) must beOkValue(true -> true) })

  def copyRead =
    prop((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.copy(m.full.toKey, n.full.toKey) >>
       store.utf8.read(m.full.toKey).zip(store.utf8.read(n.full.toKey))) must beOkLike({ case (in, out) => in must_== out }) })

  def mirror =
    prop((keys: Keys) => clean(keys) { keys =>
      store.mirror(Key.Root, Key("mirror")) >> store.list(Key("mirror")) must
        beOkLike((_:List[Key]).toSet must_== keys.toSet) })

  def moveTo =
    prop((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.moveTo(alternate, m.full.toKey, n.full.toKey) >>
       store.exists(m.full.toKey).zip(alternate.exists(n.full.toKey))) must beOkValue(false -> true) })

  def copyTo =
    prop((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.copyTo(alternate, m.full.toKey, n.full.toKey) >>
       store.exists(m.full.toKey).zip(alternate.exists(n.full.toKey))) must beOkValue(true -> true) })

  def mirrorTo =
    prop((keys: Keys) => clean(keys) { keys =>
      store.mirrorTo(alternate, Key.Root, Key("mirror")) >> alternate.list(Key("mirror")) must
        beOkLike((_:List[Key]).toSet must_== keys.toSet) })

  def checksum =
    prop((m: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      store.checksum(m.full.toKey, MD5) must beOkValue(Checksum.string(m.value.toString, MD5)) })

  def bytes =
    prop((m: KeyEntry, bytes: Array[Byte]) => clean(Keys(m :: Nil)) { _ =>
      (store.bytes.write(m.full.toKey, ByteVector(bytes)) >> store.bytes.read(m.full.toKey)) must beOkValue(ByteVector(bytes)) })

  def strings =
    prop((m: KeyEntry, s: String) => clean(Keys(m :: Nil)) { _ =>
      (store.strings.write(m.full.toKey, s, Codec.UTF8) >> store.strings.read(m.full.toKey, Codec.UTF8)) must beOkValue(s) })

  def utf8Strings =
    prop((m: KeyEntry, s: String) => clean(Keys(m :: Nil)) { _ =>
      (store.utf8.write(m.full.toKey, s) >> store.utf8.read(m.full.toKey)) must beOkValue(s) })

  def lines =
    prop((m: KeyEntry, s: List[Int]) => clean(Keys(m :: Nil)) { _ =>
      (store.lines.write(m.full.toKey, s.map(_.toString), Codec.UTF8) >> store.lines.read(m.full.toKey, Codec.UTF8)) must beOkValue(s.map(_.toString)) })

  def utf8Lines =
    prop((m: KeyEntry, s: List[Int]) => clean(Keys(m :: Nil)) { _ =>
      (store.linesUtf8.write(m.full.toKey, s.map(_.toString)) >> store.linesUtf8.read(m.full.toKey)) must beOkValue(s.map(_.toString)) })

  def create(keys: Keys): ResultT[IO, Unit] =
    keys.keys.traverseU(e =>
      Files.write(tmp1 </> FilePath.unsafe(e.full), e.value.toString)).void

  def clean[A](keys: Keys)(run: List[Key] => A): A = {
    create(keys).run.unsafePerformIO
    try run(keys.keys.map(e => e.full.toKey))
    finally ((Directories.delete(tmp1) >> Directories.delete(tmp2)).run.unsafePerformIO.void.toOption.get)
  }

  implicit class ToKey(s: String) {
    def toKey: Key = Key.unsafe(s)
  }

}
