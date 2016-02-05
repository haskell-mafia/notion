package com.ambiata.notion.core

import com.ambiata.notion.core.Arbitraries._

import com.ambiata.mundane.testing.Arbitraries._
import com.ambiata.mundane.testing.Laws._
import com.ambiata.disorder._
import org.specs2._
import org.scalacheck.Arbitrary._
import org.scalacheck._

import scalaz._, Scalaz._

class LocationIOResultSpec extends Specification with ScalaCheck { def is = s2"""

 LocationIOResult Laws
 =====================

  equals laws
    
    ${ equal.laws[LocationIOResult[Int]] }

  monad laws
    
    ${ monad.laws[LocationIOResult] }

 Combinators
 ===========

  LocationIOResult.ok.isOk is always true

    ${ prop((i: Int) => LocationIOResult.ok(i).isOk) }

  LocationIOResult.fail.isOk is always false
   
    ${ prop((e: LocationIOError) => !LocationIOResult.fail(e).isOk) }

  LocationIOResult.ok.isFail is always false

    ${ prop((i: Int) => !LocationIOResult.ok(i).isFail) }

  LocationIOResult.fail.isFail is always true
   
    ${ prop((e: LocationIOError) => LocationIOResult.fail(e).isFail) }

"""

  implicit def LocationIOResultArbitrary[A: Arbitrary]: Arbitrary[LocationIOResult[A]] =
    Arbitrary(arbitrary[LocationIOError \/ A].map(LocationIOResult.apply))

  implicit def LocationIOErrorArbitrary: Arbitrary[LocationIOError] =
    Arbitrary(for {
      t <- arbitrary[LocationTemporary]
      e <- Gen.oneOf(arbitrary[Location].map(l => LocationContextMissmatch(l, t.context)), arbitrary[S].map(s => InvalidContext(t.context, s.value)))
    } yield e)
}
