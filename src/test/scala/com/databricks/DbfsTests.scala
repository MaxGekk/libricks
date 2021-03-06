package com.databricks

import java.nio.ByteBuffer

import com.typesafe.config.ConfigFactory
import org.scalatest._

class DbfsTests extends FlatSpec with Matchers with BeforeAndAfter {
  var shard: ShardClient = _
  val testPath = "/libricks-tests"

  before {
    shard = Shard(ConfigFactory.load("test-shard")).connect
  }

  it should "list a dir and remove it if it exists" in {
    val list = shard.dbfs.list("/")
    val infos = list filter (_.path == testPath)
    assert(infos.size <= 2)
    infos foreach {info => shard.dbfs.delete(info.path, true)}
  }

  it should "create new directory, check that it exists and delete it" in {
    shard.dbfs.mkdirs(testPath)
    val info = shard.dbfs.getStatus(testPath)
    assert(info == FileInfo(testPath, true, 0))
    val newName = testPath + "-new"
    shard.dbfs.move(testPath, newName)
    shard.dbfs.delete(newName, false)
  }

  it should "put a string to a file" in {
    val s = "Hello, Dbfs!"
    shard.dbfs.put(testPath, StrBlock(s), true)
    val info = shard.dbfs.getStatus(testPath)
    assert(info == FileInfo(testPath, false, s.length))
    val newName = testPath + "-new"
    shard.dbfs.move(testPath, newName)
    shard.dbfs.delete(newName, false)
  }

  it should "stream a string to a text file" in {
    val s = "Hello, Dbfs!"

    val id = shard.dbfs.create(testPath, true)
    shard.dbfs.addBlock(id, StrBlock(s))
    shard.dbfs.close(id)

    val info = shard.dbfs.getStatus(testPath)
    assert(info == FileInfo(testPath, false, s.length))
    shard.dbfs.delete(testPath, false)
  }

  it should "stream a binary block to a file" in {
    val data = Array[Byte](0, 1, 3, 4, 5)

    val id = shard.dbfs.create(testPath, true)
    shard.dbfs.addBlock(id, WriteBlock(ByteBuffer.wrap(data)))
    shard.dbfs.close(id)

    val info = shard.dbfs.getStatus(testPath)
    assert(info == FileInfo(testPath, false, data.length))

    val readBlock = shard.dbfs.read(testPath, 0, 10)
    readBlock.raw shouldBe data

    shard.dbfs.delete(testPath, false)
  }

  it should "read the content of created file" in {
    val s = "Hello, Dbfs!"
    shard.dbfs.put(testPath, StrBlock(s), true)
    val block = shard.dbfs.read(testPath, 0, 1000)
    assert(block.str == s)
    shard.dbfs.delete(testPath, false)
  }
}
