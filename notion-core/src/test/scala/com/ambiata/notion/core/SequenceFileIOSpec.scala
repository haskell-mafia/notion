package com.ambiata.notion.core

import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.saws.core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.specs2._

class SequenceFileIOSpec extends Specification with ScalaCheck { def is = s2"""

  SequenceFileIO should be able to read and write
  ==============================================
    null key, bytes value from a locaiton    $nullBytesLocation
    int key, bytes value from a locaiton     $intBytesLocation

"""
  val conf = new Configuration
  val client = Clients.s3

  def nullBytesLocation = prop((tmp: LocationTemporary, list: List[String]) => for {
    l <- tmp.location
    i <- tmp.io
    s = SequenceFileIO.nullBytes[String](conf, b => new String(b), _.getBytes("UTF-8"))
    _ <- s.writeLocation(l, client, None, list)
    e <- i.exists(l)
    r <- s.readLocation(l, client)
  } yield e -> r ==== true -> list).set(minTestsOk = 5)

  def intBytesLocation = prop((tmp: LocationTemporary, list: List[(Int, String)]) => for {
    l <- tmp.location
    i <- tmp.io
    s = SequenceFileIO.intBytes[(Int, String)](conf, (i, b) => (i, new String(b)), { case (i, s) => (i, s.getBytes("UTF-8"))})
    _ <- s.writeLocation(l, client, None, list)
    e <- i.exists(l)
    r <- s.readLocation(l, client)
  } yield e -> r ==== true -> list).set(minTestsOk = 5)

}
