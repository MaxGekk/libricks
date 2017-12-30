case class FileInfo(path: String, is_dir: Boolean, file_size: Long)

class Dbfs(session: ShardClient) extends Endpoint {
  /** Common suffix of paths to token endpoints */
  override def path: String = session.path + "/2.0/dbfs"

  case class Handle(handle: Long)

  def create(path: String, overwrite: Boolean): Handle = {
    val resp = session.req(s"$path/create", "post",
      s"""
         | {
         |   "path": $path,
         |   "overwrite": "${overwrite.toString}"
         | }
       """.stripMargin)
    session.extract[Handle](resp)
  }

  trait Block {
    def base64: String
  }

  def addBlock(handle: Handle, data: Block): Unit = {
    val resp = session.req(s"$path/add-block", "post",
      s"""
         | {
         |   "handle": ${handle.handle},
         |   "data": ${data.base64}
         | }
       """.stripMargin
    )
    session.extract[Unit](resp)
  }

  def close(handle: Handle): Unit = {
    val resp = session.req(s"$path/close", "post",
      s"""
         | {
         |   "path": ${handle.handle}
         | }
       """.stripMargin)
    session.extract[Unit](resp)
  }

  def put(path: String, contents: Block, overwrite: Boolean) = {
    val resp = session.req(s"$path/put", "post",
      s"""
         | {
         |   "path": $path,
         |   "contents": ${contents.base64},
         |   "overwrite": ${overwrite.toString}
         | }
       """.stripMargin
    )
    session.extract[Unit](resp)
  }

  case class ReadBlock(bytes_read: Long, data: String) extends Block {
    override def base64 = data
  }

  def read(path: String, offset: Long, length: Long): Block = {
    val resp = session.req(s"$path/get-status", "get",
      s"""
         | {
         |   "path": $path,
         |   "offset": $offset,
         |   "length": $length
         | }
       """.stripMargin
    )
    session.extract[ReadBlock](resp)
  }

  def delete(path: String, recursive: Boolean): Unit = {
    val resp = session.req(s"$path/delete", "post",
      s"""
         | {
         |   "path": $path,
         |   "recursive": ${recursive.toString}
         | }
       """.stripMargin
    )
    session.extract[Unit](resp)
  }

  def getStatus(path: String): FileInfo = {
    val resp = session.req(s"$path/get-status", "get",
      s"""
         | {
         |   "path": $path
         | }
       """.stripMargin
    )
    session.extract[FileInfo](resp)
  }

  def list(path: String): List[FileInfo] = {
    val resp = session.req(s"$path/list", "get",
      s"""
         | {
         |   "path": $path
         | }
       """.stripMargin
    )
    session.extract[List[FileInfo]](resp)
  }

  def mkdirs(path: String): Unit = {
    val resp = session.req(s"$path/mkdirs", "post",
      s"""
         | {
         |   "path": $path
         | }
       """.stripMargin)
    session.extract[Unit](resp)
  }

  def move(src: String, dst: String): Unit = {
    val resp = session.req(s"$path/move", "post",
      s"""
         | {
         |   "source_path": $src,
         |   "destination_path": $dst
         | }
       """.stripMargin)
    session.extract[Unit](resp)
  }
}
