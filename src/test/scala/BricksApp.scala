import com.typesafe.config.ConfigFactory

object BricksApp {
  def main(args: Array[String]): Unit = {
    val cs = Shard(ConfigFactory.load("cust-success")).connect

    println(s"tokens = ${cs.token.list}")
    val newToken = cs.token.create(60*60, "test token")
    println(s"newToken = $newToken")
    println(s"tokens = ${cs.token.list}")
    cs.token.delete(newToken.token_info.token_id)
    println(s"tokens = ${cs.token.list}")
  }
}
