package com.ambiata.notion.core

import com.ambiata.mundane.control.{Result => _, _}
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing._
import com.ambiata.mundane.testing.Keys._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core.Arbitraries._
import org.specs2._
import org.specs2.matcher.Parameters
import scodec.bits.ByteVector
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._

abstract class StoreSpec extends Specification with ScalaCheck { def is = s2"""
  Store Usage
  ===========

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
  """

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 5, workers = 1, maxDiscardRatio = 100)

  def storeType: TemporaryType

  def run[A](keys: Keys)(f: (Store[RIO], List[Key]) => RIO[A]): RIO[A] =
    TemporaryStore.withStore(storeType)(store => for {
      _ <- keys.keys.traverseU(e => store.utf8.write((e.path +"/"+e.value).toKey, e.value.toString))
      r <- f(store, keys.keys.map(e => e.full.toKey))
    } yield r)

  def runR[A](keys: Keys, alternateType: TemporaryType)(f: (Store[RIO], Store[RIO], List[Key]) => RIO[A]): RIO[A] =
    TemporaryStore.withStore(storeType)(store => for {
      r <- TemporaryStore.withStore(alternateType)(alternate => for {
        _ <- keys.keys.traverseU(e => store.utf8.write((e.path +"/"+e.value).toKey, e.value.toString))
        r <- f(store, alternate, full(keys))
      } yield r)
    } yield r)

  def full(keys: Keys): List[Key] =
    keys.keys.map(_.full.toKey)

  def list =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      store.listAll } must beOkLike((_:List[Key]).toSet must_== full(keys).toSet) )

  def listFromPrefix =
    propNoShrink((keys: Keys) => run(keys.map(_ prepend "sub")) { (store, keys) =>
      store.list(Key.Root /"sub") } must beOkLike((_:List[Key]).toSet must_== full(keys.map(_ prepend "sub")).toSet) )

  def listHeadPrefixes =
    propNoShrink((keys: Keys) => run(keys.map(_ prepend "sub")) { (store, keys) =>
      store.listHeads(Key.Root /"sub") } must beOkLike((_:List[Key]).toSet
        must_== full(keys.map(_ prepend "sub")).map(_.fromRoot.head).toSet)).set(workers=1)

  def filter =
    propNoShrink((keys: Keys) => {
      val keyss = full(keys)
      val first = keyss.head
      val last = keyss.last
      val expected = if (first == last) List(first) else List(first, last)
      run(keys) { (store, keys)  =>
        store.filterAll(x => x == keys.head || x == keys.last)
      } must beOkLike(e => e must contain(allOf(expected:_*)))
    })

  def find =
    propNoShrink((keys: Keys) => keys.keys.length >= 3 ==> { run(keys) { (store, keys) =>
      store.findAll(_ == keys.drop(2).head) } must beOkValue(Some(full(keys).drop(2).head))
    })

  def findfirst =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      store.findAll(x => x == keys.head) } must beOkValue(Some(full(keys).head)) )

  def findlast =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      store.findAll(x => x == keys.last) } must beOkValue(Some(full(keys).last)) )

  def exists =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      keys.traverseU(store.exists) } must beOkLike(_.forall(identity)) )

  def existsPrefix =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      keys.traverseU(key => store.existsPrefix(key.copy(components = key.components.dropRight(1)))) } must beOkLike(_.forall(identity)) )

  def notExists =
    propNoShrink((keys: Keys) => TemporaryStore.withStore(storeType)( store =>
      store.exists("root" / "missing")) must beOkValue(false))

  def delete =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      val first = keys.head
      store.delete(first) >> keys.traverseU(store.exists) } must beOkLike(x => !x.head && x.tail.forall(identity)) )

  def deleteAll =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      store.deleteAllFromRoot >> keys.traverseU(store.exists) } must beOkLike(x => !x.tail.exists(identity)) )

  def move =
    propNoShrink((m: KeyEntry, n: KeyEntry) => run(Keys(m :: Nil)) { (store, _) =>
      store.move(m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(store.exists(n.full.toKey)) } must beOkValue(false -> true) )

  def moveRead =
    propNoShrink((m: KeyEntry, n: KeyEntry) => run(Keys(m :: Nil)) { (store, _) =>
      store.move(m.full.toKey, n.full.toKey) >>
        store.utf8.read(n.full.toKey) } must beOkValue(m.value.toString) )

  def copy =
    propNoShrink((m: KeyEntry, n: KeyEntry) => run(Keys(m :: Nil)) { (store, _) =>
      store.copy(m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(store.exists(n.full.toKey)) } must beOkValue(true -> true) )

  def copyRead =
    propNoShrink((m: KeyEntry, n: KeyEntry) => run(Keys(m :: Nil)) { (store, _) =>
      store.copy(m.full.toKey, n.full.toKey) >>
        store.utf8.read(m.full.toKey).zip(store.utf8.read(n.full.toKey)) } must beOkLike({ case (in, out) => in must_== out }) )

  def mirror =
    propNoShrink((keys: Keys) => run(keys) { (store, keys) =>
      store.mirror(Key.Root, Key("mirror")) >> store.list(Key("mirror")) } must
        beOkLike((_: List[Key]).toSet must_== full(keys).map(_.prepend("mirror")).toSet) )

  def moveTo =
    propNoShrink((m: KeyEntry, n: KeyEntry, t:TemporaryType) => runR(Keys(m :: Nil), t) { (store, alternate, _) =>
      store.moveTo(alternate, m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(alternate.exists(n.full.toKey)) } must beOkValue(false -> true) )

  def copyTo =
    propNoShrink((m: KeyEntry, n: KeyEntry, t:TemporaryType) => runR (Keys(m :: Nil), t) { (store, alternate, _) =>
      store.copyTo(alternate, m.full.toKey, n.full.toKey) >>
        store.exists(m.full.toKey).zip(alternate.exists(n.full.toKey)) } must beOkValue(true -> true) )

  def mirrorTo =
    propNoShrink((keys: Keys, t:TemporaryType) => runR(keys, t) { (store, alternate, _) =>
      store.mirrorTo(alternate, Key.Root, Key("mirror")) >> alternate.list(Key("mirror")) } must
        beOkLike((_: List[Key]).toSet must_== full(keys).map(_.prepend("mirror")).toSet) )

  def checksum =
    propNoShrink((m: KeyEntry) => run(Keys(m :: Nil)) { (store, _) =>
      store.checksum(m.full.toKey, MD5) } must beOkValue(Checksum.string(m.value.toString, MD5)) )

  def bytes =
    propNoShrink((m: KeyEntry, bytes: Array[Byte]) => run(Keys(m :: Nil)) { (store, _) =>
      store.bytes.write(m.full.toKey, ByteVector(bytes)) >> store.bytes.read(m.full.toKey) } must beOkValue(ByteVector(bytes)) )

  def strings =
    propNoShrink((m: KeyEntry, s: String) => run(Keys(m :: Nil)) { (store, _) =>
      store.strings.write(m.full.toKey, s, Codec.UTF8) >> store.strings.read(m.full.toKey, Codec.UTF8) } must beOkValue(s) )

  def utf8Strings =
    propNoShrink((m: KeyEntry, s: String) => run(Keys(m :: Nil)) { (store, _) =>
      store.utf8.write(m.full.toKey, s) >> store.utf8.read(m.full.toKey) } must beOkValue(s) )

  def lines =
    propNoShrink((m: KeyEntry, s: List[Int]) => run(Keys(m :: Nil)) { (store, _) =>
      store.lines.write(m.full.toKey, s.map(_.toString), Codec.UTF8) >> store.lines.read(m.full.toKey, Codec.UTF8)  }must beOkValue(s.map(_.toString)) )

  def utf8Lines =
    propNoShrink((m: KeyEntry, s: List[Int]) => run(Keys(m :: Nil)) { (store, _) =>
      store.linesUtf8.write(m.full.toKey, s.map(_.toString)) >> store.linesUtf8.read(m.full.toKey) } must beOkValue(s.map(_.toString)) )

  implicit class ToKey(s: String) {
    def toKey: Key = Key.unsafe(s)
  }
}
