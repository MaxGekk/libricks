import java.net.URI
import org.apache.http.client.HttpClient
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

case class ShardSession(client: HttpClient, shard: String) extends Endpoint {
  override def path: String = shard + "/api"

  lazy val token = new Token(this)

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
