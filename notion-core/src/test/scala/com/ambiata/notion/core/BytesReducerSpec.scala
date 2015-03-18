package com.ambiata.notion.core

import java.security.MessageDigest

import com.ambiata.disorder._
import com.ambiata.mundane.data.Lists
import com.ambiata.mundane.io.Checksum
import com.ambiata.notion.core.Reducer._
import com.ambiata.mundane.bytes.Buffer
import scalaz._, Scalaz._
import org.scalacheck._
import org.specs2._

class BytesReducerSpec extends Specification with ScalaCheck { def is = s2"""

 A BytesReducer is used to reduce bytes when reading them from an input stream
  the SHA1 BytesReducer returns the SHA1 for a list of lines $sha1
  2 BytesReducers can be zipped together                     $zip
"""

  def sha1 = prop { strings: List100[N] =>
    val lines = strings.value.map(_.value)
    val linesAsString = Lists.prepareForFile(lines)
    val sha1 = Checksum.toHexString(MessageDigest.getInstance("SHA-1").digest(linesAsString.getBytes("UTF-8")))
    val bytes = linesAsString.getBytes("UTF-8")

    Reducer.foldLeft(List(Buffer.wrapArray(bytes, 0, bytes.size)), BytesReducer.sha1) ==== sha1
  }

  def zip = prop { (strings: List100[N], r1: BytesReducer[String], r2: BytesReducer[String]) =>
    val buffer = strings.value.collect { case s if s.value.nonEmpty =>
      val bytes = s.value.getBytes("UTF-8")
      Buffer.wrapArray(bytes, 0, bytes.size)
    }
    Reducer.foldLeft(buffer, r1 zip r2) == ((Reducer.foldLeft(buffer, r1), Reducer.foldLeft(buffer, r2)))
  }

  implicit def ArbitraryBytesReducer: Arbitrary[BytesReducer[String]] =
    Arbitrary(Gen.const(BytesReducer.sha1))

}
