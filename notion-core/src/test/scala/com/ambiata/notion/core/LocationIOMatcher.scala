package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.testing._
import org.scalacheck.Prop
import org.specs2._, matcher._, execute.{Result => SpecsResult, Error => _, _}

import scalaz.{Success => _, Failure => _, _}, effect.IO, \&/._

object LocationIOMatcher extends ThrownExpectations with ScalaCheckMatchers {

  def beOk[A](context: IOContext): Matcher[LocationIO[A]] =
    beOkLike(context)(_ => Success())

  def beOkValue[A](context: IOContext)(expected: A): Matcher[LocationIO[A]] =
    beOkLike(context)((actual: A) => new BeEqualTo(expected).apply(createExpectable(actual)).toResult)

  def beOkLike[A](context: IOContext)(check: A => SpecsResult): Matcher[LocationIO[A]] = new Matcher[LocationIO[A]] {
    def apply[S <: LocationIO[A]](attempt: Expectable[S]) = {
      val r = RIOMatcher.beOkLike[LocationIOResult[A]](_ match {
        case LocationIOResult(-\/(e)) => Failure(s"Result failed with '${e.render}'")
        case LocationIOResult(\/-(a)) => check(a)
      }).apply[RIO[LocationIOResult[A]]](attempt.map(_.run(context)))
      result(r.toResult, attempt)
    }
  }

  def beFail[A](context: IOContext): Matcher[LocationIO[A]] =
    beFailLike(context)(_ => Success())

  def beFailWithError[A](context: IOContext)(expected: LocationIOError): Matcher[LocationIO[A]] =
    beFailLike(context)((actual: LocationIOError) => new BeEqualTo(expected).apply(createExpectable(actual)).toResult)

  def beFailLike[A](context: IOContext)(check: LocationIOError => SpecsResult): Matcher[LocationIO[A]] = new Matcher[LocationIO[A]] {
    def apply[S <: LocationIO[A]](attempt: Expectable[S]) = {
      val r = RIOMatcher.beOkLike[LocationIOResult[A]](r => r match {
        case LocationIOResult(-\/(e)) => check(e)
        case LocationIOResult(\/-(a)) => Failure(s"Failure: Result ok with value '${a}'")
      }).apply[RIO[LocationIOResult[A]]](attempt.map(_.run(context)))
      result(r.toResult, attempt)
    }
  }

  def beRIOFail[A](context: IOContext): Matcher[LocationIO[A]] =
    beRIOFailLike(context)(_ => Success())

  def beRIOFailWithMessage[A](context: IOContext)(expected: String): Matcher[LocationIO[A]] =
    beRIOFailWith(context)(This(expected))

  def beRIOFailWith[A](context: IOContext)(expected: String \&/ Throwable): Matcher[LocationIO[A]] =
    beRIOFailLike(context)((actual: String \&/ Throwable) => new BeEqualTo(expected).apply(createExpectable(actual)).toResult)

  def beRIOFailLike[A](context: IOContext)(check: String \&/ Throwable => SpecsResult): Matcher[LocationIO[A]] = new Matcher[LocationIO[A]] {
    def apply[S <: LocationIO[A]](attempt: Expectable[S]) = {
      val r = RIOMatcher.beFailLike[LocationIOResult[A]](check).apply[RIO[LocationIOResult[A]]](attempt.map(_.run(context)))
      result(r.toResult, attempt)
    }
  }
}
