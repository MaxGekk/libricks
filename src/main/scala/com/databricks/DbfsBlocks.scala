package com.databricks

trait Block {
  def base64: String
}

case class ReadBlock(bytes_read: Long, data: String) extends Block {
  override def base64 = data
}

