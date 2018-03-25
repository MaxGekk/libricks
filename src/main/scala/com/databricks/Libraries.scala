package com.databricks

case class Jar(jar: String)

case class LibraryFullStatus(
  library: Jar,
  status: String,
  messages: Array[String],
  is_library_for_all_clusters: Boolean
)

case class ClusterLibraryStatuses(
  cluster_id: String,
  library_statuses: List[LibraryFullStatus]
)
case class ClusterLibraryStatusesList(
  statuses: List[ClusterLibraryStatuses]
)

class Libraries(client: ShardClient) extends Endpoint {
  /** Common suffix of paths to libraries endpoints */
  override def url: String = client.url + "/2.0/libraries"

  def install(clusterId: String, libraries: List[Jar]): Unit = {
    libraries foreach {lib =>
      val resp = client.req(s"$url/install", "post",
        s"""|{
            |  "cluster_id":"$clusterId",
            |  "libraries": [{
            |    "jar": "${lib.jar}"
            |  }]
            |}""".stripMargin
      )
      client.extract[Response](resp)
    }
  }

  def uninstall(clusterId: String, libraries: List[Jar]): Unit = {
    libraries foreach {lib =>
      val resp = client.req(s"$url/uninstall", "post",
        s"""|{
            |  "cluster_id":"$clusterId",
            |  "libraries": [{
            |    "jar": "${lib.jar}"
            |  }]
            |}""".stripMargin
      )
      client.extract[Response](resp)
    }
  }

  def clusterStatus(clusterId: String): List[LibraryFullStatus] = {
    val resp = client.req(s"$url/cluster-status", "get",
      s"""{"cluster_id": "$clusterId"}"""
    )

    client.extract[ClusterLibraryStatuses](resp).library_statuses
  }

  def allClusterStatuses: List[ClusterLibraryStatuses] = {
    val resp = client.req(s"$url/all-cluster-statuses", "get")

    client.extract[ClusterLibraryStatusesList](resp).statuses
  }
}
