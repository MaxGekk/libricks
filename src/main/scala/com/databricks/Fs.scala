package com.databricks

import java.io.{File}
import java.nio.file.{Files}

class Fs(client: ShardClient) extends Dbfs(client) {

  def upload(src: String, dst: String, overwrite: Boolean = true): Unit = {
    val localFile = new File(src)
    if (localFile.length() <= 1024*1024) {
      val data = Files.readAllBytes(localFile.toPath)
      val block = WriteBlock(data)

      put(dst, block, overwrite)
    } else {
      throw new NotImplementedError("Uploading file > 1MB is not supported")
    }
  }
}
