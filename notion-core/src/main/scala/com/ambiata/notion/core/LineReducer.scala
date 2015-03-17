package com.ambiata.notion
package core

import java.security.MessageDigest

import com.ambiata.mundane.io.Checksum
import org.apache.commons.codec.binary.Base64

import scala.collection.mutable.Queue
import scalaz.{Foldable, Monoid}

/**
 * "Reducer-like" class to accumulate values while
 * reading a file and transforming them to a final value
 * when the whole file has been read
 */
trait LineReducer[T] {
  type S
  
  /** initial value */
  def init: S

  /** reducer function using the current line and the current state S */
  def reduce(line: String, s: S): S

  /** final value from the last state */
  def finalise(s: S): T
}

object LineReducer {

  /** zip two line reducers together - aka parallel composition */
  implicit class ZippedLineReducer[T1](r1: LineReducer[T1]) {
    def zip[T2](r2: LineReducer[T2]): LineReducer[(T1, T2)] = new LineReducer[(T1, T2)] {
      type S = (r1.S, r2.S)

      def init: S =
        (r1.init, r2.init)

      def reduce(line: String, s: S): S =
        (r1.reduce(line, s._1), r2.reduce(line, s._2))

      def finalise(s: S): (T1, T2) =
        (r1.finalise(s._1), r2.finalise(s._2))
    }
  }

  /** create a reducer from a Monoid and a function transforming each line to a value T */
  def fromMonoid[M : Monoid](f: String => M) = new LineReducer[M] {
    type S = M
    
    def init: S =
      Monoid[S].zero

    def reduce(line: String, s: S): S =
      Monoid[S].append(f(line), s)

    def finalise(s: S): S =
      s
  }

  /** line reducer counting lines */
  def linesNumber: LineReducer[Int] =
    LineReducer.fromMonoid[Int](_ => 1)(scalaz.std.anyVal.intInstance)

  /** line reducer computing a SHA1 */
  def sha1: LineReducer[String] = new LineReducer[String] {
    type S = MessageDigest

    val init: MessageDigest =
      MessageDigest.getInstance("SHA-1")

    def reduce(line: String, md: MessageDigest): MessageDigest = {
      md.update((line+"\n").getBytes("UTF-8"))
      md
    }

    def finalise(md: MessageDigest): String =
      Checksum.toHexString(md.digest)
  }

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

  /** fold from the left a Foldable container of lines */
  def foldLeft[F[_] : Foldable, T](lines: F[String], reducer: LineReducer[T]): T =
    reducer.finalise(Foldable[F].foldLeft(lines, reducer.init)((s: reducer.S, line: String) => reducer.reduce(line, s)))

}


