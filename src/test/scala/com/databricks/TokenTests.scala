package com.databricks

import com.typesafe.config.ConfigFactory
import org.scalatest._

class TokenTests extends FlatSpec with Matchers with BeforeAndAfter {
  var shard: ShardClient = _

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
  }

  it should "create new token and delete it" in {
    val newToken = shard.token.create(5*60, "libricks testing")
    shard.token.delete(newToken.token_info.token_id)
  }

  it should "throws ResourceDoesNotExists on deleting of wrong token id" in {
    intercept[ResourceDoesNotExists] {
      shard.token.delete("12345678")
    }
  }
}
