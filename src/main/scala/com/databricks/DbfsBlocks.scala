package com.databricks

import java.util.Base64
import java.nio.charset.StandardCharsets

sealed trait Block {
  def raw: Array[Byte]
  def str: String = new String(raw)
  def base64: String
}

/**
  * Content of a file read from DBFS
  * @param bytes_read - length of un-encoded data
  * @param data - base64 encoded content
  */
case class ReadBlock(bytes_read: Long, data: String) extends Block {
  override def raw: Array[Byte] = {
    Base64.getDecoder.decode(data.getBytes(StandardCharsets.UTF_8))
  }
  override def base64 = data
}

/**
 * A binary block to upload
 * @param raw - array of byte that should be encoded to base64 and uploaded
 */
case class WriteBlock(raw: Array[Byte]) extends Block {
  override def base64: String = {
    Base64.getEncoder.encodeToString(raw)
  }
}

case class StrBlock(override val str: String) extends Block {
  override def raw: Array[Byte] = str.map(_.toByte).toArray
  override def base64: String = {
    Base64.getEncoder.encodeToString(str.getBytes(StandardCharsets.UTF_8))
  }
}

