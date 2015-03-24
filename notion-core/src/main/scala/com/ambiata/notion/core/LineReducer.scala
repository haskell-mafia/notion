package com.ambiata.notion
package core

import scala.collection.mutable.Queue
import Reducer._

/**
 * "Reducer-like" class to accumulate values while
 * reading a file and transforming them to a final value
 * when the whole file has been read
 */

object LineReducer {

  /** line reducer counting lines */
  def linesNumber: LineReducer[Int] =
    Reducer.fromMonoid[String, Int](_ => 1)(scalaz.std.anyVal.intInstance)

  /** line reducer keeping the last N read lines */
  def tail(numberOfLines: Int): LineReducer[List[String]] = new LineReducer[List[String]] {
    type S  = Queue[String]

    def init: Queue[String] =
      new Queue[String]

    def reduce(line: String, lines: Queue[String]): Queue[String] = {
      lines.enqueue(line)
      if (lines.size > numberOfLines && !lines.isEmpty) lines.dequeue
      lines
    }

    def finalise(lines: Queue[String]): List[String] =
      lines.toList
  }
}


