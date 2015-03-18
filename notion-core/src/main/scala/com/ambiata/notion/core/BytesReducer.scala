package com.ambiata.notion
package core

import java.security.MessageDigest

import com.ambiata.mundane.io._
import Reducer._
import com.ambiata.mundane.bytes.Buffer

/**
 * Part reducer for byte arrays
 */
object BytesReducer {

  /** bytes reducer computing a SHA1 */
  def sha1: BytesReducer[String] = new BytesReducer[String] {
    type S = MessageDigest

    val init: MessageDigest =
      MessageDigest.getInstance("SHA-1")

    def reduce(buffer: Buffer, md: MessageDigest): MessageDigest = {
      md.update(buffer.bytes, buffer.offset, buffer.length)
      md
    }

    def finalise(md: MessageDigest): String =
      Checksum.toHexString(md.digest)
  }
}