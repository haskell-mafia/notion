package com.ambiata.notion

import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._, effect.IO
import scodec.bits.ByteVector
import org.specs2._, org.specs2.matcher._
import org.scalacheck.Arbitrary, Arbitrary._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing._, ResultTIOMatcher._
import com.ambiata.poacher.hdfs._
import Keys._
import java.util.UUID


// FIX Workout how this test can be pulled out and shared with posix/s3/hdfs.
class HdfsStoreSpec extends Specification with ScalaCheck { def is = sequential ^ s2"""
  Hdfs Store Usage
  ================

  list keys                                       $list
  list all keys from a key prefix                 $listFromPrefix
  list all direct prefixes from a key prefix      $listHeadPrefixes
  list an empty key prefix                        $listEmptyPrefix
  filter listed paths                             $filter
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

  implicit val params =
    Parameters(workers = 20, minTestsOk = 40, maxSize = 10)

  val conf = new Configuration

  implicit def HdfsStoreArbitary: Arbitrary[HdfsStore] =
    Arbitrary(arbitrary[Int].map(math.abs).map(n =>
      HdfsStore(conf, DirPath.Root </> "tmp" </> FileName.unsafe(s"HdfsStoreSpec.${UUID.randomUUID}.${n}"))))

  def list =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
       store.list(Key.Root) must beOkLike((_:List[Key]) must contain(exactly(keys:_*))) })

  def listFromPrefix =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys.map(_ prepend "sub")) { keys =>
      store.list(Key.Root / "sub") must beOkLike((_:List[Key]).toSet must_== keys.map(_.fromRoot).toSet) })

  def listHeadPrefixes =
  prop((store: HdfsStore, keys: Keys) => clean(store, keys.map(_ prepend "sub")) { keys =>
    store.listHeads(Key.Root / "sub") must beOkLike((_:List[Key]).toSet must_== keys.map(_.fromRoot.head).toSet) })

  def listEmptyPrefix =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
    val action =
      Hdfs.mkdir(new Path(store.basePath, "sub")).run(conf) >> // create an empty directory
      store.list(Key.Root / "sub")                             // list the prefix for keys --> should be empty
    action must beOkLike((_:List[Key]) must beEmpty)
  })

  def filter =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      val first = keys.head
      val last = keys.last
      val expected = if (first == last) List(first) else List(first, last)
      store.filter(Key.Root, x => x == first || x == last) must beOkLike(ks => ks must contain(allOf(expected:_*))) })

  def find =
    prop((store: HdfsStore, keys: Keys) => keys.keys.length >= 3 ==> { clean(store, keys) { keys =>
      val third = keys.drop(2).head
      store.find(Key.Root, _ == third) must beOkValue(Some(third)) } })

  def findfirst =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      store.find(Key.Root, x => x == keys.head) must beOkValue(Some(keys.head)) })

  def findlast =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      store.find(Key.Root, x => x == keys.last) must beOkValue(Some(keys.last)) })

  def exists =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      keys.traverseU(store.exists) must beOkLike(_.forall(identity)) })

  def existsPrefix =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      keys.traverseU(key => store.existsPrefix(key.copy(components = key.components.dropRight(1)))) must beOkLike(_.forall(identity)) })

  def notExists =
    prop((store: HdfsStore, keys: Keys) => store.exists(Key.Root / "missing") must beOkValue(false))

  def delete =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      val first = keys.head
      (store.delete(first) >> keys.traverseU(store.exists)) must beOkLike(x => !x.head && x.tail.forall(identity)) })

  def deleteAll =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      (store.deleteAll(Key.Root) >> keys.traverseU(store.exists)) must beOkLike(x => !x.tail.exists(identity)) })

  def move =
    prop((store: HdfsStore, m: KeyEntry, n: KeyEntry) => clean(store, Keys(m :: Nil)) { _ =>
      (store.move(m, n) >>
       store.exists(m).zip(store.exists(n))) must beOkValue(false -> true) })

  def moveRead =
    prop((store: HdfsStore, m: KeyEntry, n: KeyEntry) => clean(store, Keys(m :: Nil)) { _ =>
      (store.move(m, n) >>
       store.utf8.read(n)) must beOkValue(m.value.toString) })

  def copy =
    prop((store: HdfsStore, m: KeyEntry, n: KeyEntry) => clean(store, Keys(m :: Nil)) { _ =>
      (store.copy(m, n) >>
       store.exists(m).zip(store.exists(n))) must beOkValue(true -> true) })

  def copyRead =
    prop((store: HdfsStore, m: KeyEntry, n: KeyEntry) => clean(store, Keys(m :: Nil)) { _ =>
      (store.copy(m, n) >>
       store.utf8.read(m).zip(store.utf8.read(n))) must beOkLike({ case (in, out) => in must_== out }) })

  def mirror =
    prop((store: HdfsStore, keys: Keys) => clean(store, keys) { keys =>
      store.mirror(Key.Root, Key("mirror")) >> store.list(Key("mirror")) must
        beOkLike((_:List[Key]) must contain(exactly(keys:_*))) })

  def moveTo =
    prop((store: HdfsStore, alternate: HdfsStore, m: KeyEntry, n: KeyEntry) => clean(store, alternate, Keys(m :: Nil)) { _ =>
      (store.moveTo(alternate, m, n) >>
       store.exists(m).zip(alternate.exists(n))) must beOkValue(false -> true) })

  def copyTo =
    prop((store: HdfsStore, alternate: HdfsStore, m: KeyEntry, n: KeyEntry) => clean(store, alternate, Keys(m :: Nil)) { _ =>
      (store.copyTo(alternate, m, n) >>
       store.exists(m).zip(alternate.exists(n))) must beOkValue(true -> true) })

  def mirrorTo =
    prop((store: HdfsStore, alternate: HdfsStore, keys: Keys) => clean(store, alternate, keys) { keys =>
      store.mirrorTo(alternate, Key.Root, Key("mirror")) >> alternate.list(Key("mirror")) must
        beOkLike((_:List[Key]) must contain(exactly(keys:_*))) })

  def checksum =
    prop((store: HdfsStore, m: KeyEntry) => clean(store, Keys(m :: Nil)) { _ =>
      store.checksum(m, MD5) must beOkValue(Checksum.string(m.value.toString, MD5)) })

  def bytes =
    prop((store: HdfsStore, m: KeyEntry, bytes: Array[Byte]) => clean(store, Keys(m :: Nil)) { _ =>
      (store.bytes.write(m, ByteVector(bytes)) >> store.bytes.read(m)) must beOkValue(ByteVector(bytes)) })

  def strings =
    prop((store: HdfsStore, m: KeyEntry, s: String) => clean(store, Keys(m :: Nil)) { _ =>
      (store.strings.write(m, s, Codec.UTF8) >> store.strings.read(m, Codec.UTF8)) must beOkValue(s) })

  def utf8Strings =
    prop((store: HdfsStore, m: KeyEntry, s: String) => clean(store, Keys(m :: Nil)) { _ =>
      (store.utf8.write(m, s) >> store.utf8.read(m)) must beOkValue(s) })

  def lines =
    prop((store: HdfsStore, m: KeyEntry, s: List[Int]) => clean(store, Keys(m :: Nil)) { _ =>
      (store.lines.write(m, s.map(_.toString), Codec.UTF8) >> store.lines.read(m, Codec.UTF8)) must beOkValue(s.map(_.toString)) })

  def utf8Lines =
    prop((store: HdfsStore, m: KeyEntry, s: List[Int]) => clean(store, Keys(m :: Nil)) { _ =>
      (store.linesUtf8.write(m, s.map(_.toString)) >> store.linesUtf8.read(m)) must beOkValue(s.map(_.toString)) })

  def files(keys: Keys): List[Key] =
    keys.keys.map(keyEntryToKey)

  def create(store: HdfsStore, keys: Keys): ResultT[IO, Unit] =
    keys.keys.traverseU(e =>
      Hdfs.writeWith[Unit](store.root </> FilePath.unsafe(e.full), out => ResultT.safe[IO, Unit] { out.write( e.value.toString.getBytes("UTF-8")) }).run(conf)).void

  def clean[A](store: HdfsStore, keys: Keys)(run: List[Key] => A): A = {
    create(store, keys).run.unsafePerformIO
    try run(files(keys))
    finally store.deleteAll(Key.Root).run.void.unsafePerformIO
  }

  def clean[A](store: HdfsStore, alternate: HdfsStore, keys: Keys)(run: List[Key] => A): A = {
    create(store, keys).run.unsafePerformIO
    try run(files(keys))
    finally ((store.deleteAll(Key.Root) >> alternate.deleteAll(Key.Root)).run.unsafePerformIO.toOption.get)
  }

  implicit def keyEntryToKey(entry: KeyEntry): Key =
    Key.unsafe(entry.full)

  private implicit def filePathToPath(f: FilePath): Path = new Path(f.path)
  private implicit def dirPathToPath(d: DirPath): Path = new Path(d.path)

}
