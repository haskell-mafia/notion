package com.ambiata.notion.core

import com.ambiata.mundane.reflect.MacrosCompat
import com.ambiata.mundane.path._

import scalaz._, Scalaz._

/**
 * Key to access a value in a key-value Store
 */
case class Key(components: Vector[Component]) {
  override def toString: String =
    s"Key($name)"

  def |(component: Component): Key =
    copy(components = components :+ component)

  def /(key: Key): Key =
    copy(components = components ++ key.components)

  def name: String =
    components.map(_.name).mkString("/")

  def isRoot: Boolean =
    components.isEmpty
}

object Key {

  def apply(s: String): Key =
    macro Macros.attempt

  val Root = Key(Vector())

  def unsafe(s: String): Key =
    new Key(s.split("/").toVector.map(Component.unsafe))

  def fromString(s: String): Option[Key] =
    s.split("/").toList.traverse(Component.create).map(_.toVector).map(new Key(_))

  object Macros extends MacrosCompat {
    def attempt(c: Context)(s: c.Expr[String]): c.Expr[Key] = {
      import c.universe._
      s match {
        case Expr(Literal(Constant(v: String))) =>
          fromString(v) match {
            case Some(s) =>
              c.Expr(q"Key.unsafe(${s.name})")
            case None =>
              c.abort(c.enclosingPosition, s"$s is not a valid Key")
          }
        case _ =>
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(s)}")
      }

    }
  }

  implicit def KeyOrder: Order[Key] =
    Order.order((x, y) => x.name.?|?(y.name))

  implicit def KeyOrdering: scala.Ordering[Key] =
    KeyOrder.toScalaOrdering

}
