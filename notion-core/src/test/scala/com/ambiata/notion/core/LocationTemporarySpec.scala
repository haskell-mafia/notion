package com.ambiata.notion.core

import com.ambiata.disorder._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
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

  def file = prop((tmp: LocationTemporary, data: S) => for {
    f <- tmp.location
    i <- tmp.io
    _ <- i.writeUtf8(f, data.value)
    b <- i.exists(f)
    _ <- RIO.unsafeFlushFinalizers
    a <- i.exists(f)
  } yield b -> a ==== true -> false)

  def directory = prop((tmp: LocationTemporary, id: Ident, data: S) => for {
    f <- tmp.location
    i <- tmp.io
    z = f </> FilePath.unsafe(id.value)
    _ <- i.writeUtf8(z, data.value)
    b <- i.exists(f)
    _ <- RIO.unsafeFlushFinalizers
    a <- i.exists(f)
  } yield b -> a ==== true -> false)

}
