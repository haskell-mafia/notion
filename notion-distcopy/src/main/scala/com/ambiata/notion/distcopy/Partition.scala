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
  def partitionGreedily[A : scala.reflect.ClassTag](itemsv: Vector[A], n: Int, size: A => Long): Vector[Vector[A]] = {
    val items = itemsv.toArray.sortWith((a, b) => size(a) > size(b))
    val result = Array.fill(n)(new scala.collection.mutable.ListBuffer[A])
    val sizes = new Array[Long](n)
    items.foreach(item => {
      var smallest = 0
      var i = 0; while (i < sizes.length) {
        if (sizes(i) < sizes(smallest))
          smallest = i
        i += 1
      }
      result(smallest) += item
      sizes(smallest) += size(item)
    })
    result.map(_.toVector).toVector
  }
}
