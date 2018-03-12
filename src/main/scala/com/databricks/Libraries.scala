package com.databricks

sealed trait Library
case class Jar(path: String) extends Library

case class LibraryFullStatus(
  library: Library,
  status: String,
  messages: Array[String],
  is_library_for_all_clusters: Boolean
)
case class LibraryFullStatusList(library_statuses: List[LibraryFullStatus])

case class ClusterLibraryStatuses(
  cluster_id: String,
  library_statuses: Array[LibraryFullStatus]
)
case class ClusterLibraryStatusesList(
  statuses: List[ClusterLibraryStatuses]
)

class Libraries(client: ShardClient) extends Endpoint {
  /** Common suffix of paths to libraries endpoints */
  override def url: String = client.url + "/2.0/libraries"

  def install(clusterId: String, libraries: Iterable[Library]): Unit = {
    ???
  }

  def uninstall(clusterId: String, libraries: Iterable[Library]): Unit = {
    ???
  }

  def clusterStatus(clusterId: String): List[LibraryFullStatus] = {
    val resp = client.req(s"$url/cluster-status", "get",
      s"""{"cluster_id": "$clusterId"}"""
    )

    client.extract[LibraryFullStatusList](resp).library_statuses
  }

  def allClusterStatuses: List[ClusterLibraryStatuses] = {
    val resp = client.req(s"$url/all-cluster-statuses", "get")

    client.extract[ClusterLibraryStatusesList](resp).statuses
  }
}
