package com.databricks

/**
  * Interface of REST API endpoint
  */
trait Endpoint {
  /**
    * Suffix of full path to an endpoint. For instance:
    * https://shardname.cloud.databricks.com:443/api/2.0/token/create
    */
  def path: String
}


/**
  * The error returned by Databricks server
  * @param error - unique string identified the error
  * @param msg - reason of the error
  */
class BricksException(error_code: String, message: String) {
  def throwException = error_code match {
    case "INTERNAL_ERROR" => throw new InternalError(message)
    case "RESOURCE_ALREADY_EXISTS" => throw new ResourceAlreadyExists(message)
    case "RESOURCE_DOES_NOT_EXIST" => throw new ResourceDoesNotExists(message)
    case "INVALID_PARAMETER_VALUE" => throw new InvalidParameterValue(message)
    case "IO_ERROR" => throw new IOError(message)
    case "MAX_READ_SIZE_EXCEEDED" => throw new MaxReadSizeExceeded(message)
    case "INVALID_STATE" => throw new InvalidState(message)
  }
}

class InternalError(msg: String) extends Exception
class ResourceAlreadyExists(msg: String) extends Exception
class ResourceDoesNotExists(msg: String) extends Exception
class InvalidParameterValue(msg: String) extends Exception
class IOError(msg: String) extends Exception
class MaxReadSizeExceeded(msg: String) extends Exception
class InvalidState(msg: String) extends Exception