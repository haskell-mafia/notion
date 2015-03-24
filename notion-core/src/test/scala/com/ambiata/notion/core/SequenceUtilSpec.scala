package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.saws.core._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.specs2._

class SequenceUtilSpec extends Specification with ScalaCheck { def is = s2"""

  Sequence Util should be able to read and write
  ==============================================
  from a location    $location

"""
  val conf = new Configuration
  val client = Clients.s3

  def location = prop((tmp: LocationTemporary, list: List[String]) => for {
    l <- tmp.location
    i <- tmp.io
    _ <- SequenceUtil.writeBytes(l, conf, client, None)(w => RIO.safe(list.foreach(s => w(s.getBytes("UTF-8")))))
    e <- i.exists(l)
    r <- SequenceUtil.readThrift[String](l, conf, client)(r => new String(r, "UTF-8"))
  } yield e -> r ==== true -> list)

}
