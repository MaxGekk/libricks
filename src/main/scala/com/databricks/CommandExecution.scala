package com.databricks

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type

/**
 * Trait for API command result
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "resultType")
@JsonSubTypes(Array(
  new Type(value = classOf[ApiTableResult], name = "table"),
  new Type(value = classOf[ApiTextResult], name = "text"),
  new Type(value = classOf[ApiImageResult], name = "image"),
  new Type(value = classOf[ApiErrorResult], name = "error")
))
sealed trait ApiCommandResult

/**
 * API command table result
 * @param data Data in table
 * @param schema Schema of the table
 * @param truncated Whether partial results are returned
 * @param isJsonSchema True if we are sending a JSON schema instead of a string representation of
 *                     the Hive type.
 */
case class ApiTableResult(data: List[List[Any]], schema: List[Map[String, Any]],
                          truncated: Boolean, isJsonSchema: Boolean) extends ApiCommandResult

/**
 * API command text result
 * @param data The text result
 */
case class ApiTextResult(data: String) extends ApiCommandResult

/**
 * API command image result
 * @param fileName Name of the image file
 */
case class ApiImageResult(fileName: String) extends ApiCommandResult

/**
 * API command error result
 * @param cause The cause of the error
 */
case class ApiErrorResult(summary: Option[String], cause: String) extends ApiCommandResult

/**
 * API command status and result
 * @param id Command id
 * @param status status of the command: {"Queued", "Running", "Cancelling",
 *               "Finished", "Cancelled", "Error"}
 * @param results Result of the command
 */
case class CommandResult(id: String, status: String, results: ApiCommandResult)

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
    val resp = client.postFile(s"$url/execute", "command", command,
      Map("language" -> language, "clusterId" -> clusterId, "contextId" -> contextId))
    client.extract[IdResult](resp)
  }

  /**
   * Shows the status of an existing execution context.
   *
   * @param clusterId - cluster identifier.
   * @param contextId - identifier of a created context
   * @param commandId - identifier of executed command
   */
  def status(clusterId: String, contextId: String, commandId: String): CommandResult = {
    val resp = client.req(
      endpoint = s"$url/status?clusterId=${clusterId}&contextId=${contextId}&commandId=${commandId}",
      "get", ""
    )
    client.extract[CommandResult](resp)
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
