package com.ambiata.notion.distcopy

/**
 * Functions to partition a set of items with sizes into sets so that the resulting sets have
 * almost the same size.
 *
 * This is used to distribute files to be copied across mappers
 */
object Partition {

  /**
   * Use a greedy algorithm.
   * @return groups of files so that the difference of sizes between groups is minimal
   *         This is a special case of the balanced partition problem
   */
  def partitionGreedily[A](items: Seq[A], n: Int, size: A => Long): List[List[A]] = {
    items.sortWith((a, b) => size(a) > size(b)).foldLeft(Vector.fill(n)(Vector[A]())) { (res, cur) =>
      res.sortBy(_.map(size).map(l => BigInt(l)).sum) match {
        case head +: rest => (cur +: head) +: rest
        case empty => Vector(Vector(cur))
      }
    }.map(_.toList).toList
  }

}

