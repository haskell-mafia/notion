package com.ambiata.notion.core

import com.ambiata.mundane.path._
import com.ambiata.mundane.path.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.disorder._
import com.ambiata.notion.core.Arbitraries._

import scalaz._, Scalaz._

class KeySpec extends Specification with ScalaCheck { def is = s2"""

  A key must be

    Associative

     ${ (Key("a") / Key("b")) / Key("c") ==== Key("a") / (Key("b") / Key("c")) }

     ${ (Key("a") | Component("b")) / Key("c") ==== Key("a") / (Key("b") | Component("c")) }

     ${ Key("a/b") / Key("c") ==== Key("a") / Key("b/c") }

    Symmetrical

     ${ prop((key: Key) => Key.fromString(key.name) ==== key.some) }


  Root key has no components

    ${ Key(Vector()).isRoot must beTrue }

    ${ prop((l: NonEmptyList10[Component]) => Key(l.value.toVector).isRoot must beFalse) }

"""

}
