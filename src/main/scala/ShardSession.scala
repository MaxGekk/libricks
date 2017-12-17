import java.net.URI

import org.apache.http.client.HttpClient
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

case class ShardSession(client: HttpClient, shard: String) {
  def tokens = req("/api/2.0/token/list", "get", "")

  def req(path: String, httpMethod: String, data: String): Either[Error, String] = {
    val request = httpMethod.toUpperCase match {
      case "POST" => new org.apache.http.client.methods.HttpPost(shard + path)
      case _ =>
        new org.apache.http.client.methods.HttpEntityEnclosingRequestBase() {
          setURI(URI.create(shard + path))
          override def getMethod(): String = httpMethod.toUpperCase
        }
    }
    request.setEntity(new StringEntity(data))
    val response = client.execute(request)
    val handler = new org.apache.http.impl.client.BasicResponseHandler()

    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode != 200) {
      val entity = response.getEntity
      val msg = if (entity == null) "null" else EntityUtils.toString(response.getEntity)
      Left(Error(statusCode, msg))
    }

    val responseJson = handler.handleResponse(response)
    Right(responseJson)
  }
}
