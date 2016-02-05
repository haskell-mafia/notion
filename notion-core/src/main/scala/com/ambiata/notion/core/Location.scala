package com.ambiata.notion.core

import scalaz._, Scalaz._
import com.ambiata.mundane.io._
import com.ambiata.mundane.path._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._

import argonaut._, Argonaut._

/**
 * A location represents a "path" on a file system
 * on either HDFS, S3 or locally
 */
sealed trait Location {

  override def toString: String =
    fold(l => s"LocalLocation(LocalPath(${l.path}))"
       , h => s"HdfsLocation(HdfsPath(${h.path}))"
       , s => s"S3Location(${s.render})")

  def fold[X](l: LocalPath => X, h: HdfsPath => X, s: S3Pattern => X): X =
    this match {
      case LocalLocation(p) => l(p)
      case HdfsLocation(p)  => h(p)
      case S3Location(p)    => s(p)
    }

  def |(component: Component):  Location =
    fold(l => LocalLocation(l | component)
       , h => HdfsLocation(h | component)
       , s => S3Location(s.copy(unknown = S3Operations.concat(s.unknown, component.name))))
}

case class HdfsLocation(path: HdfsPath) extends Location
case class S3Location(pattern: S3Pattern) extends Location
case class LocalLocation(path: LocalPath) extends Location

object Location {
  def fromUri(s: String): String \/ Location =
    \/.fromTryCatchNonFatal(new java.net.URI(s)).leftMap(_.getMessage).flatMap(uri =>
      uri.getScheme match {
        case "hdfs" =>
          HdfsPath.fromURI(uri).cata(HdfsLocation(_).right, s"Invalid HdfsLocation ${s}".left)
        case "s3" =>
          S3Pattern.fromURI(s).cata(S3Location(_).right, s"Invalid S3Location ${s}".left)
        case "file" | null =>
          LocalPath.fromURI(uri).cata(LocalLocation(_).right, s"Invalid LocalLocation ${s}".left)
        case _ =>
          s"Unknown or invalid Location scheme [${uri.getScheme}]".left
      })

  def localLocationFromUri(s: String): String \/ Location =
    fromUri(s).flatMap {
      case l: LocalLocation => \/-(l)
      case e => -\/("Expected a local location, got: "+e)
    }

  def s3LocationFromUri(s: String): String \/ Location =
    fromUri(s).flatMap {
      case l: S3Location => \/-(l)
      case e => -\/("Expected a S3 location, got: "+e)
    }

  def hdfsLocationFromUri(s: String): String \/ Location =
    fromUri(s).flatMap {
      case l: HdfsLocation => \/-(l)
      case e => -\/("Expected a HDFS location, got: "+e)
    }

  implicit def LocationEncodeJson: EncodeJson[Location] =
    EncodeJson({
      case S3Location(s)    => Json("s3"   := Json("bucket" := s.bucket, "key" := s.unknown))
      case HdfsLocation(p)  => Json("hdfs" := Json("path" := p.path.path))
      case LocalLocation(p) => Json("local":= Json("path" := p.path.path))
    })

  implicit def LocationDecodeJson: DecodeJson[Location] =
    DecodeJson(c =>
      tagged("s3",    c, jdecode2L(S3Pattern(_: String, _: String))("bucket", "key")).map(p => S3Location(p):Location) |||
      tagged("hdfs",  c, jdecode1L(Path(_: String))("path")).map(p => HdfsLocation(HdfsPath(p)):Location) |||
      tagged("local", c, jdecode1L(Path(_: String))("path")).map(p => LocalLocation(LocalPath(p)):Location))

  def tagged[A](tag: String, c: HCursor, decoder: DecodeJson[A]): DecodeResult[A] =
    (c --\ tag).hcursor.fold(DecodeResult.fail[A]("Invalid tagged type", c.history))(decoder.decode)

  implicit def LocationEqual: Equal[Location] =
    Equal.equalA

  implicit def LocationOrder: Order[Location] =
    Order.order({
      case (LocalLocation(x), LocalLocation(y)) => x.?|?(y)
      case (LocalLocation(x), HdfsLocation(y))  => Ordering.LT
      case (LocalLocation(x), S3Location(y))    => Ordering.LT
      case (HdfsLocation(x), LocalLocation(y))  => Ordering.GT
      case (HdfsLocation(x), HdfsLocation(y))   => x.?|?(y)
      case (HdfsLocation(x), S3Location(y))     => Ordering.LT
      case (S3Location(x), LocalLocation(y))    => Ordering.GT
      case (S3Location(x), HdfsLocation(y))     => Ordering.GT
      case (S3Location(x), S3Location(y))       => (x.bucket, x.unknown).?|?(y.bucket -> y.unknown)
    })

  implicit def LocationOrdering: scala.Ordering[Location] =
    LocationOrder.toScalaOrdering
}
