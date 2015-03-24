package com.ambiata.notion
package core

import java.security.MessageDigest

import com.ambiata.disorder._
import com.ambiata.mundane.data.Lists
import com.ambiata.mundane.io.Checksum
import org.specs2._
import scalaz._, Scalaz._
import org.scalacheck._
import com.ambiata.notion.core.Reducer._

class LineReducerSpec extends Specification with ScalaCheck { def is = s2"""

 A LineReducer is used to reduce lines when reading them from an input stream
  the linesNumber LineReducer returns the number of lines                  $linesNumber
  the tail LineReducer returns the last N lines                            $tail

  2 LineReducers can be zipped together                                    $zip
"""

  def linesNumber = prop { lines: List[N] =>
    Reducer.foldLeft(lines.map(_.value), LineReducer.linesNumber) ==== lines.size
  }

  def tail = prop { (strings: List[N], n: NaturalIntSmall) =>
    val lines = strings.map(_.value)
    Reducer.foldLeft(lines, LineReducer.tail(n.value)) ==== lines.drop(lines.size - n.value)
  }.set(maxSize = 10)

  def zip = prop { (strings: List100[N], r1: LineReducer[_], r2: LineReducer[_]) =>
    val lines = strings.value.map(_.value)
    Reducer.foldLeft(lines, r1 zip r2) == ((Reducer.foldLeft(lines, r1), Reducer.foldLeft(lines, r2)))
  }

  implicit def ArbitraryLineReducer: Arbitrary[LineReducer[_]] =
    Arbitrary(Gen.oneOf(LineReducer.tail(10), LineReducer.linesNumber))

}
