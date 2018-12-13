package com.databricks

case class CommandStatus(id: String, status: String)

case class Results(resultType: String, data: String)
case class FinishedCommand(id: String, status: String, results: Results)

/**
 * Access point for Command Execution API
 * @param client - connection settings to user's shard
 */
class CommandExecution(client: ShardClient) extends Endpoint {
  /** Common suffix of paths to dbfs endpoints */
  override def url: String = client.url + "/1.2/commands"

  /**
   * Runs a command or file.
   *
   * @param language - one of languages: "scala", "python", "sql", "r"
   * @param clusterId - cluster identifier.
   * @param contextId - identifier of an execution context
   * @param command - command to run in the execution context
   */
  def execute(language: String, clusterId: String, contextId: String, command: String): IdResult = {
    val resp = client.req(s"$url/execute", "post",
      s"""{
         |  "language": "$language",
         |  "clusterId": "${clusterId}",
         |  "contextId": "${contextId}",
         |  "command": "${command}"
         |}""".stripMargin
    )
    client.extract[IdResult](resp)
  }

  /**
   * Shows the status of an existing execution context.
   *
   * @param clusterId - cluster identifier.
   * @param contextId - identifier of a created context
   * @param commandId - identifier of executed command
   */
  def status(clusterId: String, contextId: String, commandId: String): FinishedCommand = {
    val resp = client.req(
      endpoint = s"$url/status?clusterId=${clusterId}&contextId=${contextId}&commandId=${commandId}",
      "get", ""
    )
    client.extract[FinishedCommand](resp)
  }

  /**
   * Cancels one command.
   *
   * @param clusterId - identifier of a cluster
   * @param contextId - identifier of a created context
   * @param commandId - identifier of executed command
   */
  def cancel(clusterId: String, contextId: String, commandId: String): IdResult = {
    val resp = client.req(s"$url/cancel", "post",
      s"""{
         | "contextId": "${contextId}",
         | "clusterId": "${clusterId}",
         | "commandId": "${commandId}"
         | }""".stripMargin
    )
    client.extract[IdResult](resp)
  }
}
