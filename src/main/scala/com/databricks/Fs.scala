package com.databricks

import java.io.File

class Fs(client: ShardClient) extends Dbfs(client) {

  def upload(src: String, dst: String): Unit = {
    val localFile = new File(src)
    if (localFile.length() <= 1024*1024) {
      ???
    } else {
      throw new NotImplementedError("Uploading file > 1MB is not supported")
    }
  }
}
