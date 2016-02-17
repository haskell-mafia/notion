package com.ambiata.notion.core

import com.ambiata.disorder._
import com.ambiata.mundane.control._
import com.ambiata.mundane.path._
import com.ambiata.notion.core.LocationIOMatcher._
import org.specs2._
import org.specs2.matcher.Parameters

class LocationTemporarySpec extends Specification with ScalaCheck { def is = s2"""

 LocationTemporary should clean up its own resources
 ===================================================
   files                   $file
   directory               $directory

"""
  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 10, workers = 3)

  def file = prop((tmp: LocationTemporary, data: S) => (for {
    f <- LocationIO.fromRIO(tmp.location)
    _ <- LocationIO.writeUtf8(f, data.value)
    b <- LocationIO.exists(f)
    _ <- LocationIO.fromRIO(RIO.unsafeFlushFinalizers)
    a <- LocationIO.exists(f)
  } yield b -> a) must beOkValue(tmp.context)(true -> false))

  def directory = prop((tmp: LocationTemporary, id: Ident, data: S) => (for {
    f <- LocationIO.fromRIO(tmp.location)
    z = f | Component.unsafe(id.value)
    _ <- LocationIO.writeUtf8(z, data.value)
    b <- LocationIO.exists(f)
    _ <- LocationIO.fromRIO(RIO.unsafeFlushFinalizers)
    a <- LocationIO.exists(f)
  } yield b -> a) must beOkValue(tmp.context)(true -> false))

}
