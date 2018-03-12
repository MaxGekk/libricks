package com.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class LibrariesTests extends FlatSpec with Matchers with BeforeAndAfter with TestUtils {
  var shard: ShardClient = _
  val clusterId = "0312-173614-lay60"

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
  }

  it should "get library statuses of all clusters" in {
    val statuses = shard.lib.allClusterStatuses

    statuses.exists(_.cluster_id == clusterId) shouldBe true
  }

  it should "get library statuses of the test cluster" in {
    val statuses = shard.lib.clusterStatus(clusterId)

    statuses.length shouldBe 0
  }
}
