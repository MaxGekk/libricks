package com.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ExecutionContextTests extends FlatSpec with Matchers with BeforeAndAfter {
  var shard: ShardClient = _

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
  }

  it should "roundtrip test" in {
    val clusterId = "1213-124217-drays466"
    val IdResult(contextId) = shard.ec.create("scala", clusterId)
    val ContextIdStatusResult(id, status) = shard.ec.status(clusterId, contextId)
    assert(id == contextId)
    println("status: " + status)
    shard.ec.destroy(clusterId, contextId)
  }
}
