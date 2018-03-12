package com.databricks

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.file.Files

class Fs(client: ShardClient) extends Dbfs(client) {

  val MAX_BLOCK_SIZE = 64000
  //val BLOCK_SIZE = 13330
  val BLOCK_SIZE = 4*1024

  def upload(src: String, dst: String, overwrite: Boolean = true): Unit = {
    val localFile = new File(src)
    val fileSize = localFile.length()
    if (fileSize <= MAX_BLOCK_SIZE) {
      val data = Files.readAllBytes(localFile.toPath)
      val block = WriteBlock(ByteBuffer.wrap(data))

      put(dst, block, overwrite)
    } else {
      val bb = new Array[Byte](BLOCK_SIZE)
      val is = new FileInputStream(localFile)
      val bis = new java.io.BufferedInputStream(is)
      var bytesRead = bis.read(bb, 0, BLOCK_SIZE)

      val streamId = create(dst, overwrite)
      while (bytesRead > 0) {
        val block = WriteBlock(ByteBuffer.wrap(bb, 0, bytesRead))
        //println(s"bytesRead = $bytesRead")
        addBlock(streamId, block)
        //println("Added a block")
        bytesRead = bis.read(bb, 0, BLOCK_SIZE)
      }
      close(streamId)
      bis.close()
    }
  }
}