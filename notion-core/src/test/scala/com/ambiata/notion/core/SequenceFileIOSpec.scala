package com.ambiata.notion.core

import com.ambiata.notion.core.LocationIOMatcher._
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.fs.Path

import org.specs2._
import com.ambiata.disorder._

import scalaz._, Scalaz._

class SequenceFileIOSpec extends Specification with ScalaCheck { def is = s2"""

  Can read/write keys from a sequence file             $symmetricKeys
  Can read/write values from a sequence file           $symmetricValues
  Can read/write key values from a sequence file       $symmetricKeyValues

  Keys are null when writing values only               $nullKeys
  Values are null when writing keys only               $nullValues

"""

  def symmetricKeys = prop((tmp: LocationTemporary, list: List[S]) => (for {
    l <- LocationIO.fromRIO(tmp.location)
    _ <- SequenceFileIO.writeKeys[BytesWritable, String](l, tmp.conf, list.map(_.value))(s => new BytesWritable(s.getBytes("UTF-8")))
    e <- LocationIO.exists(l)
    r <- SequenceFileIO.readKeys[BytesWritable, String](l, tmp.conf, new BytesWritable)(k => new String(k.copyBytes, "UTF-8").right)
  } yield e -> r) must beOkValue(tmp.context)(true -> list.map(_.value).right))

  def symmetricValues = prop((tmp: LocationTemporary, list: List[S]) => (for {
    l <- LocationIO.fromRIO(tmp.location)
    _ <- SequenceFileIO.writeValues[BytesWritable, String](l, tmp.conf, list.map(_.value))(s => new BytesWritable(s.getBytes("UTF-8")))
    e <- LocationIO.exists(l)
    r <- SequenceFileIO.readValues[BytesWritable, String](l, tmp.conf, new BytesWritable)(v => new String(v.copyBytes, "UTF-8").right)
  } yield e -> r) must beOkValue(tmp.context)(true -> list.map(_.value).right))

  def symmetricKeyValues = prop((tmp: LocationTemporary, list: List[(S, S)]) => (for {
    l <- LocationIO.fromRIO(tmp.location)
    _ <- SequenceFileIO.writeKeyValues[BytesWritable, BytesWritable, (S, S)](l, tmp.conf, list)({ case (S(k), S(v)) =>
      new BytesWritable(k.getBytes("UTF-8")) -> new BytesWritable(v.getBytes("UTF-8"))
    })
    e <- LocationIO.exists(l)
    r <- SequenceFileIO.readKeyValues[BytesWritable, BytesWritable, (String, String)](l, tmp.conf, new BytesWritable, new BytesWritable)((k, v) =>
      (new String(k.copyBytes, "UTF-8"), new String(v.copyBytes, "UTF-8")).right)
  } yield e -> r) must beOkValue(tmp.context)(true -> list.map({ case (S(k), S(v)) => k -> v }).right))

  def nullKeys = prop((tmp: LocationTemporary, list: List[S]) => (for {
    l <- LocationIO.fromRIO(tmp.location)
    _ <- SequenceFileIO.writeKeys[BytesWritable, String](l, tmp.conf, list.map(_.value))(s => new BytesWritable(s.getBytes("UTF-8")))
    e <- LocationIO.exists(l)
    r <- SequenceFileIO.readKeyValues[BytesWritable, NullWritable, Boolean](l, tmp.conf, new BytesWritable, NullWritable.get)((k, v) =>
      v.isInstanceOf[NullWritable].right)
  } yield e -> r) must beOkValue(tmp.context)(true -> list.map(_ => true).right))

  def nullValues = prop((tmp: LocationTemporary, list: List[S]) => (for {
    l <- LocationIO.fromRIO(tmp.location)
    _ <- SequenceFileIO.writeValues[BytesWritable, String](l, tmp.conf, list.map(_.value))(s => new BytesWritable(s.getBytes("UTF-8")))
    e <- LocationIO.exists(l)
    r <- SequenceFileIO.readKeyValues[NullWritable, BytesWritable, Boolean](l, tmp.conf, NullWritable.get, new BytesWritable)((k, v) =>
      k.isInstanceOf[NullWritable].right)
  } yield e -> r) must beOkValue(tmp.context)(true -> list.map(_ => true).right))

}

