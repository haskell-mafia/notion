package com.ambiata.notion.core

import java.util.UUID

import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing._
import com.ambiata.mundane.testing.Keys._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.saws.core.S3Action
import com.ambiata.saws.s3._
import org.specs2._
import org.specs2.matcher.Parameters
import scodec.bits.ByteVector
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._
import scalaz.effect.IO
import org.specs2.specification._

// FIX Workout how this test can be pulled out and shared with posix/s3/hdfs.
class S3StoreSpec extends Specification with ScalaCheck { def is = isolated ^ s2"""
  S3 Store Usage
  ==============

  list all keys                                   $list
  list all keys from a key prefix                 $listFromPrefix
  list all direct prefixes from a key prefix      $listHeadPrefixes
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
  ${Step(client.shutdown)}
  """

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 5, workers = 1, maxDiscardRatio = 100)

  lazy val storeId = s"s3storespec/store.${UUID.randomUUID}"
  lazy val altId = s"s3storespec/alternate.${UUID.randomUUID}"
  lazy val tmp1 = DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> FileName.unsafe(s"StoreSpec.${UUID.randomUUID}")
  lazy val tmp2 = DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> FileName.unsafe(s"StoreSpec.${UUID.randomUUID}")
  lazy val store     = S3Store(S3Prefix("ambiata-test-view-exp", storeId), client, tmp1)
  lazy val alternate = S3Store(S3Prefix("ambiata-test-view-exp", altId), client, tmp2)
  lazy val client = new AmazonS3Client

  def list =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      store.listAll must beOkLike((_:List[Key]).toSet must_== keys.toSet) })

  def listFromPrefix =
    propNoShrink((keys: Keys) => clean(keys.map(_ prepend "sub")) { keys =>
      store.list(Key.Root /"sub") must beOkLike((_:List[Key]).toSet must_== keys.toSet) })

  def listHeadPrefixes =
    propNoShrink((keys: Keys) => clean(keys.map(_ prepend "sub")) { keys =>
      store.listHeads(Key.Root /"sub") must beOkLike((_:List[Key]).toSet must_== keys.map(_.fromRoot.head).toSet) }).set(workers=1)

  def filter =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      val first = keys.head
      val last = keys.last
      val expected = if (first == last) List(first) else List(first, last)
      store.filterAll(x => x == first || x == last) must beOkLike(ks => ks must contain(allOf(expected:_*))) })

  def find =
    propNoShrink((keys: Keys) => keys.keys.length >= 3 ==> { clean(keys) { keys =>
      val third = keys.drop(2).head
      store.findAll(_ == third) must beOkValue(Some(third)) } })

  def findfirst =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      store.findAll(x => x == keys.head) must beOkValue(Some(keys.head)) })

  def findlast =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      store.findAll(x => x == keys.last) must beOkValue(Some(keys.last)) })

  def exists =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      keys.traverseU(store.exists) must beOkLike(_.forall(identity)) })

  def existsPrefix =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      keys.traverseU(key => store.existsPrefix(key.copy(components = key.components.dropRight(1)))) must beOkLike(_.forall(identity)) })

  def notExists =
    propNoShrink((keys: Keys) => store.exists("root" / "missing") must beOkValue(false))

  def delete =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      val first = keys.head
      (store.delete(first) >> keys.traverseU(store.exists)) must beOkLike(x => !x.head && x.tail.forall(identity)) })

  def deleteAll =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      (store.deleteAllFromRoot >> keys.traverseU(store.exists)) must beOkLike(x => !x.tail.exists(identity)) })

  def move =
    propNoShrink((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.move(m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(store.exists(n.full.toKey))) must beOkValue(false -> true) })

  def moveRead =
    propNoShrink((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.move(m.full.toKey, n.full.toKey) >>
        store.utf8.read(n.full.toKey)) must beOkValue(m.value.toString) })

  def copy =
    propNoShrink((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.copy(m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(store.exists(n.full.toKey))) must beOkValue(true -> true) })

  def copyRead =
    propNoShrink((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.copy(m.full.toKey, n.full.toKey) >>
        store.utf8.read(m.full.toKey).zip(store.utf8.read(n.full.toKey))) must beOkLike({ case (in, out) => in must_== out }) })

  def mirror =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      store.mirror(Key.Root, Key("mirror")) >> store.list(Key("mirror")) must
        beOkLike((_: List[Key]).toSet must_== keys.map(_.prepend("mirror")).toSet) })

  def moveTo =
    propNoShrink((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.moveTo(alternate, m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(alternate.exists(n.full.toKey))) must beOkValue(false -> true) })

  def copyTo =
    propNoShrink((m: KeyEntry, n: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      (store.copyTo(alternate, m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(alternate.exists(n.full.toKey))) must beOkValue(true -> true) })

  def mirrorTo =
    propNoShrink((keys: Keys) => clean(keys) { keys =>
      store.mirrorTo(alternate, Key.Root, Key("mirror")) >> alternate.list(Key("mirror")) must
        beOkLike((_: List[Key]).toSet must_== keys.map(_.prepend("mirror")).toSet) })

  def checksum =
    propNoShrink((m: KeyEntry) => clean(Keys(m :: Nil)) { _ =>
      store.checksum(m.full.toKey, MD5) must beOkValue(Checksum.string(m.value.toString, MD5)) })

  def bytes =
    propNoShrink((m: KeyEntry, bytes: Array[Byte]) => clean(Keys(m :: Nil)) { _ =>
      (store.bytes.write(m.full.toKey, ByteVector(bytes)) >> store.bytes.read(m.full.toKey)) must beOkValue(ByteVector(bytes)) })

  def strings =
    propNoShrink((m: KeyEntry, s: String) => clean(Keys(m :: Nil)) { _ =>
      (store.strings.write(m.full.toKey, s, Codec.UTF8) >> store.strings.read(m.full.toKey, Codec.UTF8)) must beOkValue(s) })

  def utf8Strings =
    propNoShrink((m: KeyEntry, s: String) => clean(Keys(m :: Nil)) { _ =>
      (store.utf8.write(m.full.toKey, s) >> store.utf8.read(m.full.toKey)) must beOkValue(s) })

  def lines =
    propNoShrink((m: KeyEntry, s: List[Int]) => clean(Keys(m :: Nil)) { _ =>
      (store.lines.write(m.full.toKey, s.map(_.toString), Codec.UTF8) >> store.lines.read(m.full.toKey, Codec.UTF8)) must beOkValue(s.map(_.toString)) })

  def utf8Lines =
    propNoShrink((m: KeyEntry, s: List[Int]) => clean(Keys(m :: Nil)) { _ =>
      (store.linesUtf8.write(m.full.toKey, s.map(_.toString)) >> store.linesUtf8.read(m.full.toKey)) must beOkValue(s.map(_.toString)) })

  def create(keys: Keys): ResultT[IO, Unit] =
    keys.keys.traverseU(e =>
      S3Address("ambiata-test-view-exp", (e prepend storeId).path+"/"+e.value).put(e.value.toString)).evalT.void

  def clean[A](keys: Keys)(run: List[Key] => A): A = {
    try {
      create(keys).run.unsafePerformIO
      run(keys.keys.map(e => e.full.toKey))
    }
    finally {
      (S3Prefix("ambiata-test-view-exp", storeId).delete >>
        S3Prefix("ambiata-test-view-exp", altId).delete >>
        S3Action.fromResultT(Directories.delete(tmp1)) >>
        S3Action.fromResultT(Directories.delete(tmp2))).execute(client).void.unsafePerformIO
    }
  }

  implicit class ToDirPath(s: String) {
    def toDirPath: DirPath = DirPath.unsafe(s)
  }

  implicit class ToFilePath(s: String) {
    def toFilePath: FilePath = FilePath.unsafe(s)
  }

  def s3IsAccessible =
    S3.isS3Accessible.execute(new AmazonS3Client).unsafePerformIO.isOk

  implicit class ToKey(s: String) {
    def toKey: Key = Key.unsafe(s)
  }

}
