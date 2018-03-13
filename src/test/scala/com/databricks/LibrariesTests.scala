package com.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class LibrariesTests extends FlatSpec with Matchers with BeforeAndAfter with TestUtils {
  var shard: ShardClient = _
  //val clusterId = "0312-173614-lay60"
  val clusterId = "0313-085537-beta41"

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

  it should "install libricks.jar onto the cluster" in {
    val jar = Jar("dbfs:/FileStore/libricks_0_3_SNAPSHOT.jar")
    shard.lib.install(clusterId, List(jar))
    val status = shard.lib.clusterStatus(clusterId)

    status.exists(lib => lib.library == jar && lib.status == "INSTALLING")
  }

  it should "uninstall libricks.jar onto the cluster" in {
    val jar = Jar("dbfs:/FileStore/libricks_0_3_SNAPSHOT.jar")
    shard.lib.uninstall(clusterId, List(jar))
    val status = shard.lib.clusterStatus(clusterId)

    status.exists(lib => lib.library == jar && lib.status == "UNINSTALL_ON_RESTART")
  }
}
