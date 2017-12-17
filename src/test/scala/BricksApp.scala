import com.typesafe.config.ConfigFactory

object BricksApp {
  def main(args: Array[String]): Unit = {
    val cs = Shard(ConfigFactory.load("my-shard")).connect
    val tokens = cs.tokens

    println(s"tokens = $tokens")
  }
}
