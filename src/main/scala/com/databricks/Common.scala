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
  * The exception is thrown if status code of http response isn't 200
  * @param statusCode - http status code
  * @param msg - message entity of error response
  */
class HttpException(statusCode: Long, msg: Option[String]) extends Exception

/**
  * The error returned by Databricks server
  * @param error - unique string identified the error
  */
class BricksException(error: String) extends Exception

class InternalError extends BricksException("INTERNAL_ERROR")
class ResourceAlreadyExists extends BricksException("RESOURCE_ALREADY_EXISTS")
class ResourceDoesNotExists extends BricksException("RESOURCE_DOES_NOT_EXIST")
class InvalidParameterValue extends BricksException("INVALID_PARAMETER_VALUE")
class IOError extends BricksException("IO_ERROR")
class MaxReadSizeExceeded extends BricksException("MAX_READ_SIZE_EXCEEDED")
class InvalidState extends BricksException("INVALID_STATE")