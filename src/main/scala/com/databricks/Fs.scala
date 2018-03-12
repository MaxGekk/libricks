package com.databricks

class Fs(client: ShardClient) extends Dbfs(client) {

  def upload(localFile: String, remoteFile: String): Unit = {
    ???
  }
}
