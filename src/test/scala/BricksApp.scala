import com.typesafe.config.ConfigFactory

object BricksApp {
  def main(args: Array[String]): Unit = {
    val cs = Shard(ConfigFactory.load("my-shard")).connect

    println(s"tokens = ${cs.token.list}")
    val newToken = cs.token.create(60*60, "stage")
    println(s"newToken = $newToken")
    val tokens = cs.token.list
    println(s"tokens = ${tokens}")
    tokens.collect {
      case tokenInfo if tokenInfo.comment == "stage" => tokenInfo.token_id
    } foreach cs.token.delete
    println(s"tokens = ${cs.token.list}")
  }
}
