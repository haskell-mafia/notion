package com.ambiata.notion.core

import com.ambiata.disorder._
import com.ambiata.mundane.testing.Laws.functor
import com.ambiata.notion.core.Reducer._
import org.scalacheck.{Gen, Arbitrary}
import Arbitrary._
import org.specs2._
import org.specs2.execute.AsResult
import scalaz._, Scalaz._

class ReducerSpec extends Specification with ScalaCheck { def is = s2"""

 The Functor laws for Reducer must be satisfied ${functor.laws[Reducer[String, ?]]}

"""

  implicit def ArbitraryReducer: Arbitrary[Reducer[String, Int]] =
   Arbitrary(Gen.const(LineReducer.linesNumber))

  // 2 reducers are "equal" if they return the same values when folding lines
  implicit def EqualReducer: scalaz.Equal[Reducer[String, Int]] =
    new scalaz.Equal[Reducer[String, Int]] {
      def equal(a1: Reducer[String, Int], a2: Reducer[String, Int]): Boolean =
        AsResult {
          prop { strings: List10[N] =>
            val lines = strings.value.map(_.value)
            Reducer.foldLeft(lines, a1) ==== Reducer.foldLeft(lines, a2)
          }
        }.isSuccess
    }

}
