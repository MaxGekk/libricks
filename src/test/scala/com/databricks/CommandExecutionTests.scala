package com.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class CommandExecutionTests extends FlatSpec with Matchers with BeforeAndAfter {
  var shard: ShardClient = _
  var contextId: String = _
  val clusterId: String = "1213-124217-drays466"

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
    val IdResult(id) = shard.ec.create("scala", clusterId)
    contextId = id
  }

  after {
    shard.ec.destroy(clusterId, contextId)
  }

  it should "command execution" in {
    val IdResult(commandId) = shard.command.execute("scala", clusterId, contextId,
    "1 + 1")
    var status: String = ""
    var res = FinishedCommand("", "", Results("", ""))
    do {
      res = shard.command.status(clusterId, contextId, commandId)
      println("command: " + res)
      status = res.status
      Thread.sleep(1000)
    } while (status == "Running")

    assert(res.results.data == "res0: Int = 2")
  }
}
