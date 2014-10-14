package com.ambiata.notion.distcopy

import com.ambiata.notion.distcopy.Partition._
import org.specs2._

class PartitionSpec extends Specification with ScalaCheck { def is = s2"""

 Partition with the greedy algorithm
 ===================================

 Correct result with two groups         $two
 Correct result with three groups       $three

"""

  def two  = {
    partitionGreedily(List(1, 2, 3), 2, (i: Int) => i) must_== List(List(1, 2), List(3))
  }

  def three  = {
    partitionGreedily(Vector(1, 2, 3, 4), 3, (i: Int) => i) must_== Vector(Vector(1, 2), Vector(3), Vector(4))
  }

  implicit def listOrdering[A : scala.Ordering]: scala.Ordering[List[A]] = new scala.Ordering[List[A]] {
    def compare(l1: List[A], l2: List[A]) =
      if ((l1 zip l2).forall { case (a1, a2) => implicitly[scala.Ordering[A]].compare(a1, a2) == 1 }) 1 else -1
  }
}
