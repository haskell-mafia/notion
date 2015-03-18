package com.ambiata.notion.core

import com.ambiata.disorder._
import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._
import org.specs2.matcher.Parameters
import scalaz._, Scalaz._

class StoreTemporarySpec extends Specification with ScalaCheck { def is = s2"""
 StoreTemporary
 ==============

   should clean up its own resources   $resources
   should always return a unique store $conflicts
"""
  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 10, workers = 3, maxSize = 10)

  def resources = prop((st: StoreTemporary, id: Ident, data: String) => for {
    s <- st.store
    _ <- s.utf8.write(Key.unsafe(id.value), data)
    b <- s.exists(Key.unsafe(id.value))
    _ <- RIO.unsafeFlushFinalizers
    a <- s.exists(Key.unsafe(id.value))
  } yield b -> a ==== true -> false)

  def conflicts = prop((st: StoreTemporary, i: NaturalInt) => i.value > 0 ==> (for {
    l <- (1 to i.value % 100).toList.traverseU(i =>
      st.store)
  } yield l.distinct ==== l))
}
