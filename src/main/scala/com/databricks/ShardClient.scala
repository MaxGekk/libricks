package com.databricks

import java.net.URI
import org.apache.http.client.HttpClient
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Databricks REST API client - entry point of all REST API calls
  * @param client - http request executor
  * @param shard - url of the shard like https://my-shard.cloud.databricks.com:443
  */
case class ShardClient(client: HttpClient, shard: String) extends Endpoint {
  private implicit val formats = DefaultFormats

  /** Common suffix of all endpoints of Databricks public API */
  override lazy val url: String = {
    val https = "https://"
    val withProtocol = if (shard.startsWith(https)) shard else https + shard
    withProtocol + "/api"
  }

  /** Entry point for Databricks Token API like create and delete a token:
   * https://docs.databricks.com/api/latest/tokens.html */
  lazy val token = new Token(this)
  /**
   * Entry point for Databricks Dbfs API:
   * https://docs.databricks.com/api/latest/dbfs.html
   * */
  lazy val dbfs = new Dbfs(this)
  /** An extension of DBFS API */
  lazy val fs = new Fs(this)
  /**
   * Entry point of Library API:
   * https://docs.databricks.com/api/latest/libraries.html#library-api
   * */
  lazy val lib = new Libraries(this)

  /**
    * Makes a REST request to specific endpoint
    *
    * @param endpoint - url like https://my-shard.cloud.databricks.com:443/api/2.0/token/list
    * @param httpMethod - "get" or "post"
    * @param data - entity of the https request. For example, in json format: {"token_id": 42}
    * @return a string with json if http status is 200 otherwise throws an exception
    */
  def req(endpoint: String, httpMethod: String, data: String = ""): String = {
    val request = httpMethod.toUpperCase match {
      case "POST" => new org.apache.http.client.methods.HttpPost(endpoint)
      case _ =>
        new org.apache.http.client.methods.HttpEntityEnclosingRequestBase() {
          setURI(URI.create(endpoint))
          override def getMethod(): String = httpMethod.toUpperCase
        }
    }
    request.setEntity(new StringEntity(data))

    val response = client.execute(request)
    val statusCode = response.getStatusLine.getStatusCode

    statusCode match {
      case 200 => EntityUtils.toString(response.getEntity)
      case 400 =>
        val body = EntityUtils.toString(response.getEntity)
        parse(body).extract[BricksException].throwException
      case _ => throw new HttpException(statusCode)
    }
  }

  def extract[A](json: String)(implicit mf: scala.reflect.Manifest[A]): A = {
    val parsed = parse(json)
    parsed.extract[A]
  }
}
