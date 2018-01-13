package com.databricks

trait Block {
  def base64: String
}

/**
  * Content of a file read from DBFS
  * @param bytes_read - length of un-encoded data
  * @param data - base64 encoded content
  */
case class ReadBlock(bytes_read: Long, data: String) extends Block {
  override def base64 = data
}

