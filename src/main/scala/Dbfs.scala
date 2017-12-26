import org.json4s._
import org.json4s.jackson.JsonMethods._

case class FileInfo(path: String, is_dir: Boolean, file_size: Long)

class Dbfs(session: ShardClient) extends Endpoint {
  private implicit val formats = DefaultFormats

  /** Common suffix of paths to token endpoints */
  override def path: String = session.path + "/2.0/dbfs"

  case class Handle(handle: Long)

  def create(path: String, overwrite: Boolean): Handle = {
    val json = session.req(s"${path}/create", "post",
      s"""
         | {
         |   "path": $path,
         |   "overwrite": "${overwrite.toString}"
         | }
       """.stripMargin)
    val parsed = parse(json)
    parsed.extract[Handle]
  }

  class Block
  def addBlock(handle: Handle, data: Block) = ???
  def close(handle: Handle) = {
    val resp = session.req(s"${path}/close", "post",
      s"""
         | {
         |   "path": ${handle.handle},
         | }
       """.stripMargin)
    ()
  }

  def put(path: String, contents: Block, overwrite: Boolean) = ???
  def read(path: String, offset: Long, length: Long): Block = ???

  def delete(path: String, recursive: Boolean): Boolean = ???
  def getStatus(path: String): FileInfo = ???
  def list(path: String): List[FileInfo] = ???

  def mkdirs(path: String) = ???
  def move(src: String, dst: String) = ???
}
