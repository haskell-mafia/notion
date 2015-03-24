package com.ambiata.notion.core

import scalaz.{Foldable, Monoid, Functor}
import com.ambiata.mundane.bytes.Buffer

/**
 * "Reducer-like" class to accumulate values while
 * reading "parts" (bytes, lines) from a file and transforming them to a final value
 * when the whole file has been read
 */
trait Reducer[P, T] { outer =>
  type S
  
  /** initial value */
  def init: S

  /** reducer function using the current part and the current state S */
  def reduce(part: P, s: S): S

  /** final value from the last state */
  def finalise(s: S): T

  /** zip 2 reducers together to produce a Reducer[P, (T, T2)] */
  def zip[T2](r2: Reducer[P, T2]): Reducer[P, (T, T2)] = new Reducer[P, (T, T2)] {
    type S = (outer.S, r2.S)

    def init: S =
      (outer.init, r2.init)

    def reduce(p: P, s: S): S =
      (outer.reduce(p, s._1), r2.reduce(p, s._2))

    def finalise(s: S): (T, T2) =
      (outer.finalise(s._1), r2.finalise(s._2))
  }

  /** map the final result to a different type */
  def map[U](f: T => U): Reducer[P, U] = new Reducer[P, U] {
    type S = outer.S

    def init: S =
      outer.init

    def reduce(p: P, s: S): S =
      outer.reduce(p, s)

    def finalise(s: S): U =
      f(outer.finalise(s))
  }
}

object Reducer {

  type LineReducer[T] = Reducer[String, T]
  type BytesReducer[T] = Reducer[Buffer, T]

  /** create a part reducer from a Monoid and a function transforming arrary of bytes to a value T */
  def fromMonoid[P, M : Monoid](f: P => M) = new Reducer[P, M] {
    type S = M
    
    def init: S =
      Monoid[S].zero

    def reduce(part: P, s: S): S =
      Monoid[S].append(f(part), s)

    def finalise(s: S): S =
      s
  }

  /** fold from the left a Foldable container of lines */
  def foldLeft[F[_] : Foldable, P, T](content: F[P], reducer: Reducer[P, T]): T =
    reducer.finalise(Foldable[F].foldLeft(content, reducer.init)((s: reducer.S, p: P) => reducer.reduce(p, s)))

  /** Functor instance for a Reducer[P,_]} */
  implicit def ReducerFunctor[P]: Functor[Reducer[P, ?]] = new Functor[Reducer[P, ?]] {
    def map[A, B](fa: Reducer[P, A])(f: A => B): Reducer[P, B] =
      fa.map(f)
  }
}


