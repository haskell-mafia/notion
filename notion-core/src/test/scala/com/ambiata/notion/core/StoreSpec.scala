package com.ambiata.notion.core

import com.ambiata.notion.core.Arbitraries._

import com.ambiata.mundane.control.{Result => _, _}
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.disorder._
import org.specs2._
import org.specs2.matcher.Parameters
import scodec.bits.ByteVector
import scala.io.Codec
import scalaz.{Store => _, _}, Scalaz._

class StoreSpec extends Specification with ScalaCheck { def is = s2"""

 Store Usage
 ===========

  Store can list keys

    List all keys
    
     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys, value.value, s)
          l <- s.listAll
        } yield l.sorted ==== keys.keys.sorted) }
     
    List all keys from a key prefix
    
     ${ propNoShrink((keys1: KeyFamily, keys2: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          ks1 = keys1.keys.map(Key("sub1")./)
          ks2 = keys2.keys.map(Key("sub2")./)
          _ <- writeKeys(ks1 ++ ks2, value.value, s)
          l <- s.list(Key("sub1"))
        } yield l.sorted ==== ks1.sorted) }

    List all must return an empty list when there are no keys in the store

     ${ propNoShrink((st: StoreTemporary) => for {
          s <- st.store
          l <- s.listAll
        } yield l ==== Nil) }

    List must return an empty list when there are no keys with the given prefix

     ${ propNoShrink((key: Key, st: StoreTemporary) => for {
          s <- st.store
          l <- s.list(key)
        } yield l ==== Nil) }

  Store can get a filtered list of keys

    Filter over all keys in a store
    
     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys, value.value, s)
          f = keys.keys
          l <- s.filterAll(x => x == f.head || x == f.last)
          e = if (f.head == f.last) List(f.head) else List(f.head, f.last)
        } yield l.sorted ==== e.sorted) }

    Filter only keys with a given prefix

     ${ propNoShrink((keys1: KeyFamily, keys2: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          ks1 = keys1.keys.map(Key("sub1")./)
          ks2 = keys2.keys.map(Key("sub2")./)
          _ <- writeKeys(ks1 ++ ks2, value.value, s)
          l <- s.filter(Key("sub1"), x => x == ks1.head || x == ks1.last)
          e = if (ks1.head == ks1.last) List(ks1.head) else List(ks1.head, ks1.last)
        } yield l.sorted ==== e.sorted) }

    Filter over all keys returns an empty list if the predicate isn't found

     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys, value.value, s)
          l <- s.filterAll(_ => false)
        } yield l ==== Nil) }

    Filter over all keys with a given prefix returns an empty list if the predicate isn't found

     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys.map(Key("sub")./), value.value, s)
          l <- s.filter(Key("sub"), _ => false)
        } yield l ==== Nil) }

  Store can find keys

    Find a key in the entire store
    
     ${ propNoShrink((keys: KeyFamily, key: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(key :: keys.keys, value.value, s)
          l <- s.findAll(_ == key)
        } yield l ==== key.some) }

    Find a key only under a given prefix
    
     ${ propNoShrink((keys1: KeyFamily, keys2: KeyFamily, key: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys((key :: keys1.keys).map(Key("sub1")./), value.value, s)
          _ <- writeKeys((key :: keys2.keys).map(Key("sub2")./), value.value, s)
          l <- s.find(Key("sub1"), _ == Key("sub1") / key)
        } yield l ==== (Key("sub1") / key).some) }

    Find returns none if a key can't be found in the store

      ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys, value.value, s)
          l <- s.findAll(_ => false)
        } yield l ==== None) }

    Find returns none if a key can't be found under a prefix

      ${ propNoShrink((keys: KeyFamily, prefix: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys, value.value, s)
          l <- s.find(prefix, _ => false)
        } yield l ==== None) }

  Store can check if keys exist

    Key exists
    
     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys, value.value, s)
          l <- keys.keys.traverseU(s.exists)
        } yield l ==== l.map(_ => true)) }

    Key doesn't exist
    
     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys.map(Key("sub")./), value.value, s)
          e <- s.exists(Key.unsafe("missing"))
        } yield e ==== false) }

    Key doesn't exists if its a prefix

     ${ propNoShrink((key: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- s.utf8.write(key / Key("sub"), value.value)
          e <- s.exists(key)
        } yield e ==== false) }

  Store can delete keys

    Delete an individual key
    
     ${ propNoShrink((keys: KeyFamily, key: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(key :: keys.keys, value.value, s)
          _ <- s.delete(key)
          l <- s.listAll
        } yield l.sorted ==== keys.keys.filter(_ != key).sorted) }

    Delete fails if the key is a prefix

     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- writeKeys(keys.keys.map(Key("sub")./), value.value, s)
          _ <- s.delete(Key("sub"))
        } yield ()) must beFail) }

    Delete fails if the key doesn't exist

     ${ propNoShrink((key: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.delete(key)
        } yield ()) must beFail) }

    Delete all keys with the same prefix
    
     ${ propNoShrink((keys1: KeyFamily, keys2: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys1.keys.map(Key("sub1")./), value.value, s)
          _ <- writeKeys(keys2.keys.map(Key("sub2")./), value.value, s)
          _ <- s.deleteAll(Key("sub1"))
          l <- s.listAll
        } yield l.sorted ==== keys2.keys.map(Key("sub2")./).sorted) }

    Delete all keys in the store
    
     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys, value.value, s)
          _ <- s.deleteAllFromRoot
          l <- s.listAll
        } yield l ==== Nil) }

  Store can move keys within the same store

    Move a key
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(m :: Nil, value.value, s)
          _ <- s.move(m, n)
          e <- s.exists(m)
          d <- s.utf8.read(n)
        } yield e -> d ==== false -> value.value) }

    Move fails when the source key is a prefix
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.utf8.write(m / Key("sub"), value.value)
          _ <- s.move(m, n)
        } yield ()) must beFail) }

    Move fails when the source key doesn't exist

     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.move(m, n)
        } yield ()) must beFail) }

    Move fails when the destination key already exists
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- writeKeys(List(m, n), value.value, s)
          _ <- s.move(m, n)
        } yield ()) must beFail) }

  Store can move keys to another store

    Move a key
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => for {
          f <- st.store
          t <- alt.store
          _ <- f.utf8.write(m, value.value)
          _ <- f.moveTo(t, m, n)
          e <- f.exists(m)
          v <- t.utf8.read(n)
        } yield e -> v ==== false -> value.value) }

    Move fails when the source key is a prefix

     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => (for {
          f <- st.store
          t <- alt.store
          _ <- f.utf8.write(m / Key("sub"), value.value)
          _ <- f.moveTo(t, m, n)
        } yield ()) must beFail) }

    Move fails when the source key doesn't exist

     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => (for {
          f <- st.store
          t <- alt.store
          _ <- f.moveTo(t, m, n)
        } yield ()) must beFail) }

    Move fails when the destination key already exists

     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => (for {
          f <- st.store
          t <- alt.store
          _ <- f.utf8.write(m, value.value)
          _ <- t.utf8.write(n, value.value)
          _ <- f.moveTo(t, m, n)
        } yield ()) must beFail) }

  Store can copy keys within the same store

    Copy a key
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => for {
          s  <- st.store
          _  <- s.utf8.write(m, value.value)
          _  <- s.copy(m, n)
          mv <- s.utf8.read(m)
          nv <- s.utf8.read(n)
        } yield mv ==== nv && mv ==== value.value) }

    Copy fails when source key is a prefix
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.utf8.write(m / Key("sub"), value.value)
          _ <- s.copy(m, n)
        } yield ()) must beFail) }

    Copy fails when source key doesn't exist
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.copy(m, n)
        } yield ()) must beFail) }

    Copy fails when target key already exists
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.utf8.write(m, value.value)
          _ <- s.utf8.write(n, value.value)
          _ <- s.copy(m, n)
        } yield ()) must beFail) }

  Store can copy keys to another store

    Copy a key
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => for {
          f  <- st.store
          t  <- alt.store
          _  <- f.utf8.write(m, value.value)
          _  <- f.copyTo(t, m, n)
          mv <- f.utf8.read(m)
          nv <- t.utf8.read(n)
        } yield mv ==== nv && mv ==== value.value) }

    Copy fails when source key is a prefix
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => (for {
          f <- st.store
          t <- alt.store
          _ <- f.utf8.write(m / Key("sub"), value.value)
          _ <- f.copyTo(t, m, n)
        } yield ()) must beFail) }

    Copy fails when source key doesn't exist
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => (for {
          f <- st.store
          t <- alt.store
          _ <- f.copyTo(t, m, n)
        } yield ()) must beFail) }

    Copy fails when target key already exists
    
     ${ propNoShrink((m: Key, n: Key, value: S, st: StoreTemporary, alt: StoreTemporary) => (for {
          f <- st.store
          t <- alt.store
          _ <- f.utf8.write(m, value.value)
          _ <- t.utf8.write(n, value.value)
          _ <- f.copyTo(t, m, n)
        } yield ()) must beFail) }

  Store can mirror all keys under a prefix to another prefix

    Within the same store

     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(keys.keys.map(Key("sub")./), value.value, s)
          _ <- s.mirror(Key("sub"), Key("mirror"))
          l <- s.list(Key.Root)
        } yield l.sorted ==== (keys.keys.map(Key("sub")./) ++ keys.keys.map(Key("mirror")./)).sorted) }

    To a different store
    
     ${ propNoShrink((keys: KeyFamily, value: S, st: StoreTemporary, alt: StoreTemporary) => for {
          f  <- st.store
          t  <- alt.store
          _  <- writeKeys(keys.keys.map(Key("sub")./), value.value, f)
          _  <- f.mirrorTo(t, Key("sub"), Key("mirror"))
          fl <- f.list(Key.Root)
          tl <- t.list(Key.Root)
        } yield fl.sorted ==== keys.keys.map(Key("sub")./).sorted && tl.sorted ==== keys.keys.map(Key("mirrot")./).sorted) }

    Fails when the destination

  Store should be able to compute a checksum on keys in a store

    Checksum on an individual key works
    
     ${ propNoShrink((m: Key, value: S, st: StoreTemporary) => for {
          s <- st.store: RIO[Store[RIO]]
          _ <- s.utf8.write(m, value.value)
          r <- s.checksum(m, MD5): RIO[Checksum]
        } yield r ==== Checksum.string(value.value, MD5)) }

    Checksum fails if key is a prefix
    
     ${ propNoShrink((m: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store: RIO[Store[RIO]]
          _ <- s.utf8.write(Key("sub") / m, value.value)
          r <- s.checksum(m, MD5): RIO[Checksum]
        } yield ()) must beFail) }

    Checksum fails if key doesn't exist
    
     ${ propNoShrink((m: Key, st: StoreTemporary) => (for {
          s <- st.store: RIO[Store[RIO]]
          r <- s.checksum(m, MD5): RIO[Checksum]
        } yield ()) must beFail) }

  Store can read / write bytes

    From a key

     ${ propNoShrink((m: Key, bytes: Array[Byte], st: StoreTemporary) => for {
          s <- st.store
          _ <- s.bytes.write(m, ByteVector(bytes))
          r <- s.bytes.read(m)
        } yield r ==== ByteVector(bytes)) }

    Fails when key is a prefix

     ${ propNoShrink((m: Key, bytes: Array[Byte], st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.bytes.write(m / Key("sub"), ByteVector(bytes))
          r <- s.bytes.read(m)
        } yield r) must beFail) }

    Fails when key doesn't exist

     ${ propNoShrink((m: Key, st: StoreTemporary) => (for {
          s <- st.store
          r <- s.bytes.read(m)
        } yield r) must beFail) }

  Store can read / write strings
  
    From a key
    
     ${ propNoShrink((m: Key, str: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- s.strings.write(m, str.value, Codec.UTF8)
          r <- s.strings.read(m, Codec.UTF8)
        } yield r ==== str.value) }

    Fails when key is a prefix
    
     ${ propNoShrink((m: Key, str: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.strings.write(m / Key("sub"), str.value, Codec.UTF8)
          r <- s.strings.read(m, Codec.UTF8)
        } yield r) must beFail) }

    Fails when key doesn't exist
    
     ${ propNoShrink((m: Key, st: StoreTemporary) => (for {
          s <- st.store
          r <- s.strings.read(m, Codec.UTF8)
        } yield r) must beFail) }

  Store can read / write utf8 strings
  
    From a key
    
     ${ propNoShrink((m: Key, str: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- s.utf8.write(m, str.value)
          r <- s.utf8.read(m)
        } yield r ==== str.value) }

    Fails when key is a prefix
    
     ${ propNoShrink((m: Key, str: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.utf8.write(m / Key("sub"), str.value)
          r <- s.utf8.read(m)
        } yield r) must beFail) }

    Fails when key doesn't exist
    
     ${ propNoShrink((m: Key, st: StoreTemporary) => (for {
          s <- st.store
          r <- s.utf8.read(m)
        } yield r) must beFail) }

  Store can read / write lines
  
    From a key
    
     ${ propNoShrink((m: Key, v: List[Int], st: StoreTemporary) => for {
          s <- st.store
          _ <- s.lines.write(m, v.map(_.toString), Codec.UTF8)
          r <- s.lines.read(m, Codec.UTF8)
        } yield r ==== v.map(_.toString)) }

    Fails when key is a prefix
    
     ${ propNoShrink((m: Key, v: List[Int], st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.lines.write(m / Key("sub"), v.map(_.toString), Codec.UTF8)
          r <- s.lines.read(m, Codec.UTF8)
        } yield r) must beFail) }

    Fails when key doesn't exist
    
     ${ propNoShrink((m: Key, st: StoreTemporary) => (for {
          s <- st.store
          r <- s.lines.read(m, Codec.UTF8)
        } yield r) must beFail) }

  Store can read / write utf8 lines

    From a key
    
     ${ propNoShrink((m: Key, v: List[Int], st: StoreTemporary) => for {
          s <- st.store
          _ <- s.linesUtf8.write(m, v.map(_.toString))
          r <- s.linesUtf8.read(m)
        } yield r ==== v.map(_.toString)) }

    Fails when key is a prefix
    
     ${ propNoShrink((m: Key, v: List[Int], st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.linesUtf8.write(m / Key("sub"), v.map(_.toString))
          r <- s.linesUtf8.read(m)
        } yield r) must beFail) }

    Fails when key doesn't exist
    
     ${ propNoShrink((m: Key, st: StoreTemporary) => (for {
          s <- st.store
          r <- s.linesUtf8.read(m)
        } yield r) must beFail) }

  Store can do unsafe stream operations

    Can read a key using an InputStream
    
     ${ propNoShrink((m: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- writeKeys(m :: Nil, value.value, s)
          b = new StringBuilder
          _ <- s.unsafe.withInputStream(m)(Streams.read(_).map(b.append(_)).void)
        } yield b.toString ==== value.value) }

    Fails when the key is a prefix

     ${ propNoShrink((m: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- writeKeys(m / Key("sub") :: Nil, value.value, s)
          b = new StringBuilder
          _ <- s.unsafe.withInputStream(m)(Streams.read(_).map(b.append(_)).void)
        } yield b.toString) must beFail) }

    Fails when the key doesn't exist

     ${ propNoShrink((m: Key, st: StoreTemporary) => (for {
          s <- st.store
          b = new StringBuilder
          _ <- s.unsafe.withInputStream(m)(Streams.read(_).map(b.append(_)).void)
        } yield b.toString) must beFail) }

    Can write a key using an OutputStream
    
     ${ propNoShrink((m: Key, value: S, st: StoreTemporary) => for {
          s <- st.store
          _ <- s.unsafe.withOutputStream(m)(o => Streams.write(o, value.value))
          d <- s.utf8.read(m)
        } yield d ==== value.value) }

    Fails when the key already exists

     ${ propNoShrink((m: Key, value: S, st: StoreTemporary) => (for {
          s <- st.store
          _ <- s.utf8.write(m, value.value)
          _ <- s.unsafe.withOutputStream(m)(o => Streams.write(o, value.value))
        } yield ()) must beFail) }

  """

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 15, workers = 1, maxDiscardRatio = 100)

  def writeKeys(keys: List[Key], value: String, store: Store[RIO]): RIO[Unit] =
    keys.traverseU(k => store.utf8.write(k, value)).void
}
