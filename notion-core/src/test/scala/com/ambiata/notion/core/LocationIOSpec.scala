package com.ambiata.notion.core

import com.ambiata.mundane.control.RIO
import org.specs2._
import org.specs2.matcher.Parameters
import org.specs2.matcher._
import com.ambiata.disorder._
import com.ambiata.mundane.bytes._
import com.ambiata.mundane.io._
import com.ambiata.mundane.path._
import com.ambiata.mundane.path.Arbitraries._
import com.ambiata.mundane.testing.Laws._
import com.ambiata.notion.core.LocationIOMatcher._
import java.io._

import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._

class LocationIOSpec extends Specification with ScalaCheck { def is = s2"""

 LocationIO Laws
 ===============

   equals laws

     ${ equal.laws[LocationIO[Int]] }

   monad laws
   
     ${ monad.laws[LocationIO] }

 LocationIO IO
 =============

  LocationIO should be able to do a recursive list on a location

    A single file/object should be a list of one

     ${ prop((loc: LocationTemporary, data: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p, data.value)
          a <- LocationIO.list(p).map(_.sorted)
        } yield a -> List(p)) must beOkLike(loc.context)(t => t._1 ==== t._2))
      }

    With multiple levels

     ${ prop((loc: LocationTemporary, dp: DistinctPair[Component], data: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p | dp.first, data.value)
          _ <- LocationIO.writeUtf8(p | dp.second | dp.first, data.value)
          a <- LocationIO.list(p).map(_.sorted)
          e = List(p | dp.first, p | dp.second | dp.first).sorted
        } yield a -> e) must beOkLike(loc.context)(t => t._1 ==== t._2))
     }

  LocationIO should be able to do an existance check on a location

    A file/object

     ${ prop((loc: LocationTemporary, data: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p, data.value)
          e <- LocationIO.exists(p)
        } yield e) must beOkValue(loc.context)(true))
      }

    A dir/prefix

     ${ prop((loc: LocationTemporary, c: Component, data: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p | c, data.value)
          e <- LocationIO.exists(p)
        } yield e) must beOkValue(loc.context)(true))
      }

  LocationIO should be able to read/write lines from a location

    From a sinlge file/object

     ${ prop((loc: LocationTemporary, lines: List[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p, lines.map(_.value))
          r <- LocationIO.readLines(p)
        } yield r) must beOkValue(loc.context)(lines.map(_.value)))
      }

    Fails when reading from a location which is not a file/object
   
     ${ prop((loc: LocationTemporary, c: Component, lines: List[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p | c, lines.map(_.value))
          r <- LocationIO.readLines(p)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when reading from a location that doesn't exist
   
     ${ prop((loc: LocationTemporary) => (for {
          p <- LocationIO.fromRIO(loc.location)
          r <- LocationIO.readLines(p)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when writing to a location which already exists

     ${ prop((loc: LocationTemporary, lines: List[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p, lines.map(_.value))
          _ <- LocationIO.writeUtf8Lines(p, lines.map(_.value))
        } yield ()) must beRIOFail(loc.context))
      }

  LocationIO should be able to read/write strings from a location

    From a sinlge file/object

     ${ prop((loc: LocationTemporary, str: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p, str.value)
          r <- LocationIO.readUtf8(p)
        } yield r) must beOkValue(loc.context)(str.value))
      }

    Fails when reading from a location which is not a file/object
   
     ${ prop((loc: LocationTemporary, c: Component, str: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p | c, str.value)
          r <- LocationIO.readUtf8(p)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when reading from a location that doesn't exist
   
     ${ prop((loc: LocationTemporary) => (for {
          p <- LocationIO.fromRIO(loc.location)
          r <- LocationIO.readUtf8(p)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when writing to a location which already exists

     ${ prop((loc: LocationTemporary, str: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p, str.value)
          _ <- LocationIO.writeUtf8(p, str.value)
        } yield ()) must beRIOFail(loc.context))
      }

  LocationIO should be able to read/write bytes from a location

    From a sinlge file/object

     ${ prop((loc: LocationTemporary, value: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeBytes(p, value.value.getBytes)
          r <- LocationIO.readBytes(p)
        } yield r) must beOkValue(loc.context)(value.value.getBytes))
      }

    Fails when reading from a location which is not a file/object

     ${ prop((loc: LocationTemporary, c: Component, value: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeBytes(p | c, value.value.getBytes)
          r <- LocationIO.readBytes(p)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when reading from a location that doesn't exist
   
     ${ prop((loc: LocationTemporary) => (for {
          p <- LocationIO.fromRIO(loc.location)
          r <- LocationIO.readBytes(p)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when writing to a location which already exists

     ${ prop((loc: LocationTemporary, value: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeBytes(p, value.value.getBytes)
          _ <- LocationIO.writeBytes(p, value.value.getBytes)
        } yield ()) must beRIOFail(loc.context))
      }

  LocationIO should be able to read/write using input/output streams

    From a sinlge file/object

     ${ prop { (loc: LocationTemporary, text: S) =>
          var read: String = null
          (for {
            p <- LocationIO.fromRIO(loc.location)
            _ <- LocationIO.writeUnsafe(p)(out => LocationIO.io(new DataOutputStream(out).writeUTF(text.value)))
            r <- LocationIO.readUnsafe(p)(in => LocationIO.io(read = new DataInputStream(in).readUTF))
          } yield read) must beOkValue(loc.context)(text.value)
        }
      }

    Fails when reading from a location which is not a file/object

     ${ prop((loc: LocationTemporary, c: Component, text: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUnsafe(p | c)(out => LocationIO.io(new DataOutputStream(out).writeUTF(text.value)))
          r <- LocationIO.readUnsafe(p)(in => LocationIO.io(new DataInputStream(in).readUTF).void)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when reading from a location that doesn't exist
   
     ${ prop((loc: LocationTemporary) => (for {
          p <- LocationIO.fromRIO(loc.location)
          r <- LocationIO.readUnsafe(p)(in => LocationIO.io(new DataInputStream(in).readUTF).void)
        } yield r) must beRIOFail(loc.context))
      }

    Fails when writing to a location which already exists

     ${ prop((loc: LocationTemporary, text: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUnsafe(p)(out => LocationIO.io(new DataOutputStream(out).writeUTF(text.value)))
          _ <- LocationIO.writeUnsafe(p)(out => LocationIO.io(new DataOutputStream(out).writeUTF(text.value)))
        } yield ()) must beRIOFail(loc.context))
      }

  LocationIO should be able to stream lines

    From a single file/object

     ${ prop((loc: LocationTemporary, lines: List[N]) =>
          (for {
            p <- LocationIO.fromRIO(loc.location)
            _ <- LocationIO.writeUtf8Lines(p, lines.map(_.value))
            r <- LocationIO.streamLinesUTF8(p, List[String]())(_ :: _).map(_.reverse)
          } yield r) must beOkValue(loc.context)(lines.map(_.value)))
      }

  LocationIO should be able to stream bytes

    From a single file/object

     ${ prop((loc: LocationTemporary, value: Array[Byte]) => {
          val buffer = new Array[Byte](value.length)
          (for {
            p <- LocationIO.fromRIO(loc.location)
            _ <- LocationIO.writeBytes(p, value)
            _ <- LocationIO.streamBytes(p, 0)({ case (b, i) => Buffer.copy(b, buffer, i); i + b.length })
          } yield buffer) must beOkValue(loc.context)(value)
        })
      }

  LocationIO should be able to read part of a file/object

    First line

     ${ prop((loc: LocationTemporary, lines: List100[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
          f <- LocationIO.firstLine(p)
        } yield f) must beOkValue(loc.context)(lines.value.headOption.map(_.value)))
      }

    Last line

     ${ prop((loc: LocationTemporary, lines: List100[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
          l <- LocationIO.lastLine(p)
        } yield l) must beOkValue(loc.context)(lines.value.lastOption.map(_.value)))
      }

    First n lines

     ${ prop((loc: LocationTemporary, lines: List100[N], n: NaturalIntSmall) => n.value <= lines.value.length ==> {
          (for {
            p <- LocationIO.fromRIO(loc.location)
            _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
            h <- LocationIO.head(p, n.value)
          } yield h) must beOkValue(loc.context)(lines.value.take(n.value).map(_.value))
        })
      }

    Last n lines

     ${ prop((loc: LocationTemporary, lines: List100[N], n: NaturalIntSmall) => n.value <= lines.value.length ==> {
          (for {
            p <- LocationIO.fromRIO(loc.location)
            _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
            t <- LocationIO.tail(p, n.value)
          } yield t) must beOkValue(loc.context)(lines.value.drop(lines.value.size - n.value).map(_.value))
        })
      }

    Last n lines with the number of total lines

     ${ prop((loc: LocationTemporary, lines: List100[N], n: NaturalIntSmall) => n.value <= lines.value.length ==> {
          (for {
            p <- LocationIO.fromRIO(loc.location)
            _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
            t <- LocationIO.tailAndLinesNumber(p, n.value)
          } yield t._2) must beOkValue(loc.context)(lines.value.length)
        })
      }

    Head of a file with all line numbers <==> readLines

     ${ prop((loc: LocationTemporary, lines: List100[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
          h <- LocationIO.head(p, lines.value.size)
        } yield h) must beOkValue(loc.context)(lines.value.map(_.value)))
      }

    Tail of a file with all line numbers <==> readLines

     ${ prop((loc: LocationTemporary, lines: List100[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
          t <- LocationIO.tail(p, lines.value.size)
        } yield t) must beOkValue(loc.context)(lines.value.map(_.value)))
      }

    Sha1 of a file <==> mundane.Checksum("SHA1")

     ${ prop((loc: LocationTemporary, lines: List100[N]) => (for {
          p  <- LocationIO.fromRIO(loc.location)
          _  <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
          b  <- LocationIO.readBytes(p)
          s1 <- LocationIO.reduceBytes(p, BytesReducer.sha1)
          s2  = Checksum.bytes(b, SHA1).hash
        } yield s1 -> s2) must beOkLike(loc.context)(t => t._1 ==== t._2))
      }

    Line count of a file <==> mundane.LineCount

     ${ prop((loc: LocationTemporary, lines: List100[N]) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8Lines(p, lines.value.map(_.value))
          n <- LocationIO.reduceLinesUTF8(p, LineReducer.linesNumber)
        } yield n) must beOkValue(loc.context)(lines.value.length))
      }

  LocationIO can do delete a location

    delete will only delete a file/object

     ${ prop((loc: LocationTemporary, data: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p, data.value)
          _ <- LocationIO.delete(p)
          e <- LocationIO.exists(p)
        } yield e) must beOkValue(loc.context)(false))
      }

    deleteAll will delete a file/object/dir/prefix

     ${ prop((loc: LocationTemporary, dp: DistinctPair[Component], data: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p | dp.first, data.value)
          _ <- LocationIO.writeUtf8(p | dp.second, data.value)
          _ <- LocationIO.deleteAll(p)
          e <- LocationIO.exists(p)
        } yield e) must beOkValue(loc.context)(false))
      }

    delete fails if the location doesn't exist

     ${ prop((loc: LocationTemporary) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.delete(p)
        } yield ()) must beRIOFail(loc.context))
      }

    deleteAll fails if the location doesn't exist

     ${ prop((loc: LocationTemporary) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.deleteAll(p)
        } yield ()) must beRIOFail(loc.context))
      }

    delete fails if the location is a dir/prefix

     ${ prop((loc: LocationTemporary, c: Component, data: S) => (for {
          p <- LocationIO.fromRIO(loc.location)
          _ <- LocationIO.writeUtf8(p | c, data.value)
          _ <- LocationIO.delete(p)
        } yield ()) must beRIOFail(loc.context))
      }

  LocationIO can copy a file/object from one location to another

    Successful when destination does not exist and overwrite is set to false

     ${ prop((loc1: LocationTemporary, loc2: LocationTemporary, data: S) => (for {
          p1 <- LocationIO.fromRIO(loc1.location)
          p2 <- LocationIO.fromRIO(loc2.location)
          _  <- LocationIO.writeUtf8(p1, data.value)
          _  <- LocationIO.copyFile(p1, p2, false)
          r  <- LocationIO.readUtf8(p2)
        } yield r) must beOkValue(loc1.contextAll)(data.value))
     }

    Successful when destination exists and overwrite is set to true

     ${ prop((loc1: LocationTemporary, loc2: LocationTemporary, data1: S, data2: S) => (for {
          p1 <- LocationIO.fromRIO(loc1.location)
          p2 <- LocationIO.fromRIO(loc2.location)
          _  <- LocationIO.writeUtf8(p1, data1.value)
          _  <- LocationIO.writeUtf8(p2, data2.value)
          _  <- LocationIO.copyFile(p1, p2, true)
          r  <- LocationIO.readUtf8(p2)
        } yield r) must beOkValue(loc1.contextAll)(data1.value))
     }

    Fails when the destination does exist and overwrite is set to false

     ${ prop((loc1: LocationTemporary, loc2: LocationTemporary, data: S) => (for {
          p1 <- LocationIO.fromRIO(loc1.location)
          p2 <- LocationIO.fromRIO(loc2.location)
          _  <- LocationIO.writeUtf8(p1, data.value)
          _  <- LocationIO.writeUtf8(p2, data.value)
          _  <- LocationIO.copyFile(p1, p2, false)
        } yield ()) must beRIOFail(loc1.contextAll))
     }

    Fails when the source doesn't exist

     ${ prop((loc: LocationTemporary) => (for {
          p1 <- LocationIO.fromRIO(loc.location)
          p2 <- LocationIO.fromRIO(loc.location)
          _  <- LocationIO.copyFile(p1, p2, false)
        } yield ()) must beRIOFail(loc.contextAll))
     }

"""
  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 5, workers = 3, maxSize = 10)

  implicit def LocationIOArbitrary[A : Arbitrary]: Arbitrary[LocationIO[A]] =
    Arbitrary(arbitrary[A].map(LocationIO.ok))

  implicit def LocationIOEqual[A]: Equal[LocationIO[Int]] =
    Equal.equal((a, b) => a.run(NoneIOContext).unsafePerformIO == b.run(NoneIOContext).unsafePerformIO)
}
