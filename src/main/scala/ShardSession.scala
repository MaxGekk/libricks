import org.apache.http.client.HttpClient

case class ShardSession(httpClinet: HttpClient) {
  def tokens: Seq[Token] = ???
}
