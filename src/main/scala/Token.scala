import org.json4s._
import org.json4s.jackson.JsonMethods._

case class TokenInfo(token_id: String,
                     creation_time: Long,
                     expiry_time: Long,
                     comment: String
                    )

case class NewToken(token_value: String, token_info: TokenInfo)
case class TokenList(token_infos: List[TokenInfo])

class Token(session: ShardSession) extends Endpoint {
  implicit val formats = DefaultFormats
  override def path: String = session.path + "/2.0/token"

  def create(lifetimeInSec: Long, comment: String): NewToken = {
    val json = session.req(s"${path}/create", "post",
      s"""
         | {
         |   "lifetime_seconds": ${lifetimeInSec},
         |   "comment": "${comment}"
         | }
       """.stripMargin)
    val parsed = parse(json)
    parsed.extract[NewToken]
  }

  def delete(token_id: String): Boolean = {
    val json = session.req(s"${path}/delete", "post",
      s"""
         | {
         |   "token_id": "$token_id"
         | }
       """.stripMargin
    )
    val parsed = parse(json)
    parsed == JObject(List())
  }

  def list: List[TokenInfo] = {
    val json = session.req(s"${path}/list", "get")
    val parsed = parse(json)

    parsed.extract[TokenList].token_infos
  }
}
