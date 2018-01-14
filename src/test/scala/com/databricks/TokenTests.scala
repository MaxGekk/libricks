package com.databricks

import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.scalatest._

class TokenTests extends FlatSpec with Matchers with BeforeAndAfter {
  var shard: ShardClient = _
  val lifeTimeInSec: Long = 5*60
  val now = Calendar.getInstance().getTime().toString

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
  }

  it should "create new token and delete it" in {
    val comment = "create test " + now
    val newToken = shard.token.create(lifeTimeInSec, comment)
    shard.token.delete(newToken.token_info.token_id)
  }

  it should "throws ResourceDoesNotExists on deleting of wrong token id" in {
    intercept[ResourceDoesNotExists] {
      shard.token.delete("12345678")
    }
  }

  it should "lists created tokens" in {
    val prefix = "list test"
    val amount = 3
    val tokens = for {
      i <- 0 until amount
      comment = s"$prefix $i $now"
    } yield shard.token.create(lifeTimeInSec, comment)
    val list = shard.token.list
    val filteredTokens = list.filter(_.comment.startsWith(prefix))
    assert(filteredTokens.size == amount)
    tokens.foreach(t => shard.token.delete(t.token_info.token_id))
  }
}
