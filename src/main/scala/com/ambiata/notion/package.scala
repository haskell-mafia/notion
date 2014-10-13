package com.ambiata

import com.ambiata.mundane.reflect.MacrosCompat

package object notion extends MacrosCompat {

  /**
   * These various methods and macros
   * allow to create Keys and KeyNames from strings
   * where each string is checked so that it doesn't contain a forbidden character (like /)
   */

  implicit def stringToKeyNameSyntax(s: String): KeyNameSyntax =
    macro createKeyNameSyntax

  def createKeyNameSyntax(c: Context)(s: c.Expr[String]): c.Expr[KeyNameSyntax] = {
    import c.universe._
    s match {
      case Expr(Literal(Constant(v: String))) => c.Expr(q"new KeyNameSyntax(${createKeyNameFromString(c)(v)})")
      case _ => c.abort(c.enclosingPosition, s"Not a literal ${showRaw(s)}")
    }

  }

  implicit def keyNameSyntax(keyName: KeyName) =
    new KeyNameSyntax(keyName)

  implicit def ToKeyName(s: String): KeyName =
    macro createKeyName

  def keyNameFromString(s: String): Option[KeyName] =
    if (s.contains("/")) None
    else Some(KeyName.unsafe(s))

  def createKeyName(c: Context)(s: c.Expr[String]): c.Expr[KeyName] = {
    import c.universe._
    s match {
      case Expr(Literal(Constant(v: String))) => createKeyNameFromString(c)(v)
      case _ => c.abort(c.enclosingPosition, s"Not a literal ${showRaw(s)}")
    }
  }

  private def createKeyNameFromString(c: Context)(s: String): c.Expr[KeyName] = {
    import c.universe._
    keyNameFromString(s) match {
      case None     => c.abort(c.enclosingPosition, s"$s is not a valid KeyName. It must not contain a /")
      case Some(fn) => c.Expr(q"KeyName.unsafe(${fn.name})")
    }
  }
}
