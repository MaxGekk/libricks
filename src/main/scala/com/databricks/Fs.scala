package com.databricks

import java.io.{File}
import java.nio.file.{Files}

class Fs(client: ShardClient) extends Dbfs(client) {

  val MAX_BLOCK_SIZE = 1024*1024

  def upload(src: String, dst: String, overwrite: Boolean = true): Unit = {
    val localFile = new File(src)
    if (localFile.length() <= MAX_BLOCK_SIZE) {
      val data = Files.readAllBytes(localFile.toPath)
      val block = WriteBlock(data)

      put(dst, block, overwrite)
    } else {
      throw new NotImplementedError("Uploading file > 1MB is not supported")
    }
  }
}
