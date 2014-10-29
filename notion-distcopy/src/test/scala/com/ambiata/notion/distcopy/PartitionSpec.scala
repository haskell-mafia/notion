package com.ambiata.notion.distcopy

import com.ambiata.notion.distcopy.Partition._
import org.specs2._


class PartitionSpec extends Specification with ScalaCheck { def is = s2"""

 Partition with the greedy algorithm
 ===================================

 Correct result with two groups
  ${partitionGreedily(Vector(1, 2, 3), 2, (i: Int) => i) ==== Vector(Vector(1, 2), Vector(3))}

 Correct result with three groups
  ${partitionGreedily(Vector(1, 2, 3, 4), 3, (i: Int) => i) ==== Vector(Vector(1, 2), Vector(3), Vector(4))}

 Correct number of groups
  ${propNoShrink((i: Int) => (i > 0 && (i % 100 != 0)) ==> { partitionGreedily(Vector.fill(i % 100)(1), i % 100, (i: Int) => i).length ==== i % 100 }) }

"""

}
