package com.ambiata.notion.core

import org.scalacheck._, Arbitrary._
import Arbitraries._
import org.specs2._
import scalaz._, Scalaz._

class ExecutionLocationSpec extends Specification with ScalaCheck { def is = s2"""

 An Execution location can be turned into
  a location ${prop { location: Location =>
    val executionLocation = ExecutionLocation.fromLocation(location)
    executionLocation.map(_.location) ==== executionLocation.as(location)
  }}

"""
}
