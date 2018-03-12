package com.databricks

import java.io.{File, PrintWriter}

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FsTests extends FlatSpec with Matchers with BeforeAndAfter with TestUtils {
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
    withTempDir{ dir =>
      val fileName = "file_test.txt"
      val localPath = dir + "/" + fileName
      val localFile = new File(localPath)

      val pw = new PrintWriter(localFile)
      pw.write("Hello, world")
      pw.close()

      val remotePath = testPath + "/" + fileName
      shard.fs.upload(localPath, remotePath)
      val FileInfo(_, isDir, size) = shard.dbfs.getStatus(remotePath)

      size shouldBe localFile.length()
      isDir shouldBe false
    }
  }
}
