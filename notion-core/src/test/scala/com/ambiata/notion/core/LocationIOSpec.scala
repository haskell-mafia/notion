package com.ambiata.notion.core

import org.specs2._
import org.specs2.execute.AsResult
import org.specs2.matcher._
import org.apache.hadoop.conf.Configuration
import org.specs2.specification.FixtureExample

import com.ambiata.disorder._
import com.ambiata.saws.core.Clients
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._

import scalaz._, Scalaz._

class LocationIOSpec extends Specification  with ScalaCheck { def is = s2"""

 The LocationIO class provides functions to read/write/query locations on different systems

   isDirectory             $isDirectory
   isFile                  $isFile
   deleteAll               $deleteAll
   delete                  $delete
   read / write lines      $readWriteLines
   stream lines            $streamLines
   list                    $list
   exists                  $exists

"""
  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 3, workers = 3)

  def isDirectory = prop((loc: LocationTemporary, id: Ident, data: String) => for {
    p <- loc.location
    i <- loc.io
    _ <- i.writeUtf8(p </> FilePath.unsafe(id.value), data)
    e <- i.isDirectory(p)
  } yield e ==== true)

  def isFile = prop((loc: LocationTemporary, data: String) => for {
    p <- loc.location
    i <- loc.io
    _ <- i.writeUtf8(p, data)
    e <- i.isDirectory(p)
  } yield e ==== false)

  def deleteAll = prop((loc: LocationTemporary, dp: DistinctPair[Ident], data: String) => for {
    p <- loc.location
    i <- loc.io
    _ <- i.writeUtf8(p </> FilePath.unsafe(dp.first.value), data)
    _ <- i.writeUtf8(p </> FilePath.unsafe(dp.second.value), data)
    _ <- i.deleteAll(p)
    l <- i.list(p)
  } yield l ==== nil)

  def delete = prop((loc: LocationTemporary, data: String) => for {
    p <- loc.location
    i <- loc.io
    _ <- i.writeUtf8(p, data)
    _ <- i.delete(p)
    e <- i.exists(p)
  } yield e ==== false)

  def readWriteLines = prop((loc: LocationTemporary, lines: List[String]) => {
    // we remove spaces from lines in this test
    // because reading lines will split the text on newlines
    val linesWithoutSpaces = lines.map(_.replaceAll("\\s", ""))
    for {
      p <- loc.location
      i <- loc.io
      _ <- i.writeUtf8Lines(p, linesWithoutSpaces)
      r <- i.readLines(p)
    } yield r ==== linesWithoutSpaces
  })

  def streamLines = prop((loc: LocationTemporary, lines: List[String]) => {
    val linesWithoutSpaces = lines.map(_.replaceAll("\\s", ""))
    for {
      p <- loc.location
      i <- loc.io
      _ <- i.writeUtf8Lines(p, linesWithoutSpaces)
      r <- i.streamLinesUTF8(p, List[String]())(_ :: _).map(_.reverse)
    } yield r ==== linesWithoutSpaces
  })

  def list = prop((loc: LocationTemporary, dp: DistinctPair[Ident], data: String) => for {
    p <- loc.location
    i <- loc.io
    _ <- i.writeUtf8(p </> FilePath.unsafe(dp.first.value), data)
    _ <- i.writeUtf8(p </> FilePath.unsafe(dp.second.value), data)
    r <- i.list(p)
    l = r.length
  } yield r.map(_.render.split("/").last).toSet -> l ==== Set(dp.first.value, dp.second.value) -> 2)

  def exists = prop((loc: LocationTemporary, data: String) => for {
    p <- loc.location
    i <- loc.io
    _ <- i.writeUtf8(p, data)
    e <- i.exists(p)
  } yield e ==== true)
}
