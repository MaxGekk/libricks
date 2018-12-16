package com.databricks

/**
 * The result with an id
 * @param id The id for context or command
 */
case class IdResult(id: String)

/**
 * Context id and status
 * @param id Context id
 * @param status Status of context: {"Pending", "Running", "Error"}
 */
case class ContextIdStatusResult(id: String, status: String)

/**
 * Access point for Execution Context API
 * @param client - connection settings to user's shard
 */
class ExecutionContext(client: ShardClient) extends Endpoint {
  /** Common suffix of paths to dbfs endpoints */
  override def url: String = client.url + "/1.2/contexts"

  /**
   * Creates an execution context on a specified cluster for a given programming language.
   *
   * @param language - one of languages: scala, python and sql
   * @param clusterId - cluster identifier.
   */
  def create(language: String, clusterId: String): IdResult = {
    val resp = client.req(s"$url/create", "post",
      s"""{"language": "$language", "clusterId": "${clusterId}"}"""
    )
    client.extract[IdResult](resp)
  }

  /**
   * Shows the status of an existing execution context.
   *
   * @param contextId - identifier of a created context
   */
  def status(clusterId: String, contextId: String): ContextIdStatusResult = {
    val resp = client.req(
      endpoint = s"$url/status?clusterId=${clusterId}&contextId=${contextId}", "get",
      ""
    )
    client.extract[ContextIdStatusResult](resp)
  }

  /**
   * Destroys an execution context.
   *
   * @param clusterId - identifier of a cluster
   * @param contextId - identifier of a created context
   */
  def destroy(clusterId: String, contextId: String): IdResult = {
    val resp = client.req(s"$url/destroy", "post",
      s"""{"contextId": "${contextId}", "clusterId": "${clusterId}"}"""
    )
    client.extract[IdResult](resp)
  }
}
