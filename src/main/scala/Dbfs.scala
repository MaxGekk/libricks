/**
  * Attributes of a file or a directory
  * @param path - full path to a file or a directory
  * @param is_dir - true if the path is directory otherwise false
  * @param file_size - the length of the file in bytes or 0 if the path is a directory
  */
case class FileInfo(path: String, is_dir: Boolean, file_size: Long)

/**
  * The identifier should be passed into the [[Dbfs.addBlock]] and [[Dbfs.close]] calls
  * when writing to a file through a stream.
  * @param id - unique stream identifier
  */
case class StreamId(id: Long)

/**
  * Access point for Databricks DBFS Rest API
  * @param client - connection settings to user's shard
  */
class Dbfs(client: ShardClient) extends Endpoint {
  /** Common suffix of paths to token endpoints */
  override def path: String = client.path + "/2.0/dbfs"

  def create(path: String, overwrite: Boolean): StreamId = {
    val resp = client.req(s"$path/create", "post",
      s"""{"path": "$path", "overwrite": ${overwrite.toString}}"""
    )
    client.extract[StreamId](resp)
  }

  trait Block {
    def base64: String
  }

  def addBlock(handle: StreamId, data: Block): Unit = {
    val resp = client.req(s"$path/add-block", "post",
      s"""{"handle": ${handle.id}, "data": "${data.base64}"}"""
    )
    client.extract[Unit](resp)
  }

  def close(handle: StreamId): Unit = {
    val resp = client.req(s"$path/close", "post",
      s"""{"path": ${handle.id}}"""
    )
    client.extract[Unit](resp)
  }

  def put(path: String, contents: Block, overwrite: Boolean) = {
    val resp = client.req(s"$path/put", "post",
      s"""
         | {
         |   "path": "$path",
         |   "contents": "${contents.base64}",
         |   "overwrite": ${overwrite.toString}
         | }
       """.stripMargin
    )
    client.extract[Unit](resp)
  }

  case class ReadBlock(bytes_read: Long, data: String) extends Block {
    override def base64 = data
  }

  def read(path: String, offset: Long, length: Long): Block = {
    val resp = client.req(s"$path/get-status", "get",
      s"""
         | {
         |   "path": "$path",
         |   "offset": $offset,
         |   "length": $length
         | }
       """.stripMargin
    )
    client.extract[ReadBlock](resp)
  }

  def delete(path: String, recursive: Boolean): Unit = {
    val resp = client.req(s"$path/delete", "post",
      s"""{"path": "$path", "recursive": ${recursive.toString}}"""
    )
    client.extract[Unit](resp)
  }

  def getStatus(path: String): FileInfo = {
    val resp = client.req(s"$path/get-status", "get",
      s"""{"path":"$path"}"""
    )
    client.extract[FileInfo](resp)
  }

  def list(path: String): List[FileInfo] = {
    val resp = client.req(s"$path/list", "get",
      s"""{"path":"$path"}"""
    )
    client.extract[List[FileInfo]](resp)
  }

  def mkdirs(path: String): Unit = {
    val resp = client.req(s"$path/mkdirs", "post",
      s"""{"path":"$path"}"""
    )
    client.extract[Unit](resp)
  }

  def move(src: String, dst: String): Unit = {
    val resp = client.req(s"$path/move", "post",
      s"""{"source_path":"$src", "destination_path":"$dst"}"""
    )
    client.extract[Unit](resp)
  }
}
