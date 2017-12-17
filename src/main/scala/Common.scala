
trait Endpoint {
  def path: String
}

class RestApiReqException(statusCode: Long, msg: String) extends Exception