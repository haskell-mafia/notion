package com.ambiata.notion.core

import com.ambiata.mundane.control.RIO
import org.specs2._
import org.specs2.execute.AsResult
import org.specs2.matcher._
import org.apache.hadoop.conf.Configuration
import org.specs2.specification.FixtureExample

import com.ambiata.disorder._
import com.ambiata.saws.core.Clients
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import java.io._

import scalaz._, Scalaz._

class LocationIOSpec extends Specification with ScalaCheck { def is = s2"""

 The LocationIO class provides functions to read/write/query locations on different systems

   isDirectory                                          $isDirectory
   isFile                                               $isFile
   deleteAll                                            $deleteAll
   delete                                               $delete
   read / write lines                                   $readWriteLines
   read / write unsafe                                  $readWriteUnsafe
   stream lines                                         $streamLines
   list                                                 $list
   exists                                               $exists

   first line of a file                                 $fileFirst
   the last line of a file                              $fileLast
   the first lines of a file                            $fileHead
   the last lines of a file                             $fileTail
   the last lines of a file and the number of lines     $fileTailAndLinesNumber
   head of a file with all line numbers <==> readLines  $fileHeadAllIsReadLines
   tail of a file with all line numbers <==> readLines  $fileTailAllIsReadLines

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

  def readWriteUnsafe = prop { (loc: LocationTemporary, text: String) =>
    var read: String = null
    for {
      p <- loc.location
      i <- loc.io
      _ <- i.writeUnsafe(p)(out => RIO.io(new DataOutputStream(out).writeUTF(text)))
      r <- i.readUnsafe(p)(in => RIO.io(read = new DataInputStream(in).readUTF))
    } yield read ==== text
  }

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

  def fileFirst = prop { (loc: LocationTemporary, linesNumber: NaturalIntSmall) =>
    for {
      ll         <- writeLines(loc, linesNumber)
      (l, lines) =  ll
      i          <- loc.io
      line       <- i.firstLine(l)
    } yield line must_== lines.headOption
  }.set(maxSize = 10)

  def fileLast = prop { (loc: LocationTemporary, linesNumber: NaturalIntSmall) =>
    for {
      ll         <- writeLines(loc, linesNumber)
      (l, lines) =  ll
      i          <- loc.io
      line       <- i.lastLine(l)
    } yield line must_== lines.lastOption
  }.set(maxSize = 10)

  def fileHead = prop { (loc: LocationTemporary, linesNumber: NaturalIntSmall, requestedLinesNumber: NaturalIntSmall) =>
    for {
      ll         <- writeLines(loc, linesNumber)
      (l, lines) =  ll
      i          <- loc.io
      head       <- i.head(l, requestedLinesNumber.value)
    } yield head must_== lines.take(requestedLinesNumber.value)
  }.set(maxSize = 10)

  def fileTail = prop { (loc: LocationTemporary, linesNumber: NaturalIntSmall, requestedLinesNumber: NaturalIntSmall) =>
    for {
      ll         <- writeLines(loc, linesNumber)
      (l, lines) =  ll
      i          <- loc.io
      tail       <- i.tail(l, requestedLinesNumber.value)
    } yield tail must_== lines.drop(lines.size - requestedLinesNumber.value)
  }.set(maxSize = 10)

  def fileTailAndLinesNumber = prop { (loc: LocationTemporary, linesNumber: NaturalIntSmall, requestedLinesNumber: NaturalIntSmall) =>
    for {
      ll <- writeLines(loc, linesNumber)
      (l, lines) = ll
      i     <- loc.io
      tailAndNb <- i.tailAndLinesNumber(l, requestedLinesNumber.value)
    } yield tailAndNb._2 must_== linesNumber.value
  }.set(maxSize = 10)

  def fileHeadAllIsReadLines = prop { (loc: LocationTemporary, linesNumber: PositiveIntSmall) =>
    val lines = (1 to linesNumber.value).toList.map("line"+_)
    for {
      path <- LocalTemporary(loc.path).file
      l    =  LocalLocation(path.path)
      i    <- loc.io
      _    <- i.writeUtf8Lines(l, lines)
      head <- i.head(l, lines.size)
      all  <- Files.readLines(path, "UTF-8")
    } yield head must_== all
  }.set(maxSize = 10)

  def fileTailAllIsReadLines = prop { (loc: LocationTemporary, linesNumber: PositiveIntSmall) =>
    val lines = (1 to linesNumber.value).toList.map("line"+_)
    for {
      path <- LocalTemporary(loc.path).file
      l    =  LocalLocation(path.path)
      i    <- loc.io
      _    <- i.writeUtf8Lines(l, lines)
      tail <- i.tail(l, lines.size)
      all  <- Files.readLines(path, "UTF-8")
    } yield tail must_== all
  }.set(maxSize = 10)

  /**
   * HELPERS
   */
  def writeLines(loc: LocationTemporary, linesNumber: NaturalIntSmall): RIO[(Location, List[String])] = {
    val lines = (0 until linesNumber.value).toList.map("line"+_)
    for {
      l <- loc.location
      i <- loc.io
      _ <- i.writeUtf8Lines(l, lines)
    } yield (l, lines)
  }

}
