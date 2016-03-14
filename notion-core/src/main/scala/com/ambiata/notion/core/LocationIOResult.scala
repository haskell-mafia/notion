package com.ambiata.notion.core

import scalaz._, Scalaz._

case class LocationIOResult[A](result: LocationIOError \/ A) {
  def map[B](f: A => B): LocationIOResult[B] =
    LocationIOResult(result.map(f))

  def flatMap[B](f: A => LocationIOResult[B]): LocationIOResult[B] =
    result match {
      case -\/(e) => LocationIOResult(e.left)
      case \/-(a) => f(a)
    }

  def isOk: Boolean =
    result.isRight

  def isFail: Boolean =
    result.isLeft
}

object LocationIOResult {
  def ok[A](a: A): LocationIOResult[A] =
    LocationIOResult(a.right)

  def fail[A](e: LocationIOError): LocationIOResult[A] =
    LocationIOResult(e.left)

  implicit def LocationIOResultMonad: Monad[LocationIOResult] = new Monad[LocationIOResult] {
    def point[A](v: => A) = ok(v)
    def bind[A, B](m: LocationIOResult[A])(f: A => LocationIOResult[B]) = m.flatMap(f)
  }

  implicit def LocationIOResultEqual[A: Equal]: Equal[LocationIOResult[A]] = {
    implicitly[Equal[LocationIOError \/ A]].contramap(_.result)
  }
}

sealed trait LocationIOError {
  def render: String
}

object LocationIOError {
  implicit def LocationIOErrorEqual: Equal[LocationIOError] =
    Equal.equalA
}

case class LocationContextMissmatch(location: Location, context: IOContext) extends LocationIOError {
  def render: String =
    s"Location/Context mismatch. ${location} / ${context}"
}

case class InvalidContext(context: IOContext, expecting: String) extends LocationIOError {
  def render: String =
    s"Invalid context ${context}, expecting ${expecting}"
}
