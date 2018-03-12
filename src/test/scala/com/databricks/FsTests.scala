package com.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FsTests extends FlatSpec with Matchers with BeforeAndAfter {
  var shard: ShardClient = _
  val testPath = "/libricks-tests-file"

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
    shard.dbfs.mkdirs(testPath)
  }

  after {
    shard.dbfs.delete(testPath, true)
  }

  it should "upload a small file < 1MB" in {
    val fileName = "file_test.txt"
    val localPath = "/Users/maxim/tmp/" + fileName
    val remotePath = testPath + "/" + fileName
    shard.fs.upload(localPath, remotePath)
    val FileInfo(_, isDir, size) = shard.dbfs.getStatus(remotePath)
    val localFile = new java.io.File(localPath)

    size shouldBe localFile.length()
    isDir shouldBe false
  }
}
