import java.net.URI
import org.apache.http.client.HttpClient
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

/**
  * Databricks REST API client - entry point of all REST API calls
  * @param client - http request executor
  * @param shard - url of the shard like https://my-shard.cloud.databricks.com:443
  */
case class ShardClient(client: HttpClient, shard: String) extends Endpoint {
  /** Common suffix of all endpoints of Databricks public API */
  override def path: String = shard + "/api"

  /** Entry point for Databricks Token API like create and delete a token. */
  lazy val token = new Token(this)

  /**
    * Makes a REST request to specific endpoint
    * @param endpoint - url like https://my-shard.cloud.databricks.com:443/api/2.0/token/list
    * @param httpMethod - "get" or "post"
    * @param data - entity of the https request. For example, in json format: {"token_id": 42}
    * @return a string with json if http status is 200 otherwise throws [[RestApiReqException]]
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
    if (statusCode == 200) {
      val handler = new org.apache.http.impl.client.BasicResponseHandler()
      val responseJson = handler.handleResponse(response)
      responseJson
    } else {
      val msg = response.getEntity match {
        case null => None
        case entity => Some(EntityUtils.toString(entity))
      }
      throw new RestApiReqException(statusCode, msg)
    }
  }
}
