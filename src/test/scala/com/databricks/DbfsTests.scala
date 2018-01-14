package com.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest._

class DbfsTests extends FlatSpec with Matchers with BeforeAndAfter {
  var shard: ShardClient = _
  val testDir = "/libricks-tests"

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
  }

  it should "create new directory, check that it exists and delete it" in {
    shard.dbfs.mkdirs(testDir)
    val info = shard.dbfs.getStatus(testDir)
    assert(info == FileInfo(testDir, true, 0))
    val newName = testDir + "-new"
    shard.dbfs.move(testDir, newName)
    shard.dbfs.delete(newName, false)
  }
}
