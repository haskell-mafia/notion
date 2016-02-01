package com.ambiata.notion.core

import com.ambiata.mundane.path.Component

import java.util.UUID

import com.ambiata.mundane.reflect.MacrosCompat

case class KeyName private(name: String)

object KeyName extends MacrosCompat {

  def unsafe(s: String) = new KeyName(s)

  def fromUUID(uuid: UUID) = new KeyName(uuid.toString)

  def fromString(s: String): Option[KeyName] =
    if (s.contains("/")) None
    else Some(KeyName.unsafe(s))

  def fromComponent(c: Component): KeyName =
    unsafe(c.name)
}

class KeyNameSyntax(name: KeyName) {
  def /(other: KeyName): Key = Key(Vector(name, other))
  def /(other: Key): Key     = Key(name +: other.components)
}
