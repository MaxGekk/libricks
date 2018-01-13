package com.databricks

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
      s"""{"path":"$path","contents": "${contents.base64}","overwrite": ${overwrite.toString}}"""
    )
    client.extract[Unit](resp)
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

  /**
    * Deletes a files or a directory in DBFS
    *
    * @param path - absolute path to the file or directory
    * @param recursive - if it is true, delete all sub-directories of the path.
    *                  Deleting empty directories can be done without providing the recursive flag.
    * @throws InvalidParameterValue if the path has wrong format not supported by underlying FS
    * @throws IOError if the path is a non-empty directory and recursive is set to false
    *                 or on other similar errors.
    */
  def delete(path: String, recursive: Boolean): Unit = {
    val resp = client.req(s"$path/delete", "post",
      s"""{"path": "$path", "recursive": ${recursive.toString}}"""
    )
    client.extract[Unit](resp)
  }

  /**
    * Gets the file information of a file or directory.
    *
    * @param path - relative or absolute path to a file or a directory
    * @return Attributes of the file. See [[FileInfo]].
    *         If the path is a relative path, [[FileInfo]] will have the relative path too.
    * @throws ResourceDoesNotExists If the file or directory does not exist
    */
  def getStatus(path: String): FileInfo = {
    val resp = client.req(s"$path/get-status", "get",
      s"""{"path":"$path"}"""
    )
    client.extract[FileInfo](resp)
  }

  /**
    * Lists the contents of a directory, or details of the file. If the path points
    * to a directory, the method lists all sub-directories recursively.
    * The method doesn't guaranties order of [[FileInfo]]s in the returned list.
    *
    * @param path - relative or absolute path to a file or a directory
    * @return all [[FileInfo]]s which are within the given directory. If the path refers
    *         to a file, this will return a single-element list containing that file's FileInfo.
    * @throws ResourceDoesNotExists If the file or directory does not exist
    */
  def list(path: String): List[FileInfo] = {
    val resp = client.req(s"$path/list", "get",
      s"""{"path":"$path"}"""
    )
    client.extract[List[FileInfo]](resp)
  }

  /**
    * Creates DBFS directory and all necessary parent directories.
    * It forms a list of directory names starting from the root directory. If any name contains
    * unsupported characters by underlying FS , it throws [[InvalidParameterValue]]
    * After that it iterates over the list and checks the file status of each element, and:
    * - If a directory doesn't exists, it is created.
    * - If there is a file with the same name, the [[ResourceAlreadyExists]] exception
    *     is thrown in that case
    *
    * @param path - absolute path to new DBFS directory.
    * @throws ResourceAlreadyExists if directory (parent directory) already exists
    * @throws InvalidParameterValue wrong path
    */
  def mkdirs(path: String): Unit = {
    val resp = client.req(s"$path/mkdirs", "post",
      s"""{"path":"$path"}"""
    )
    client.extract[Unit](resp)
  }

  /**
    * Moves (renames) a files/directory in DBFS
    *
    * @param src - existing source path of a files (directory). If the path doesn't exists,
    *            the method throws [[ResourceDoesNotExists]] or [[InvalidParameterValue]] if
    *            the path has wrong format.
    * @param dst - non-existing destination path. If the path exists, the method throws
    *            [[ResourceAlreadyExists]] or [[InvalidParameterValue]] in the case of wrong path.
    * @throws InvalidParameterValue if dst or src path has wrong format
    * @throws ResourceAlreadyExists if destination file exists
    * @throws ResourceDoesNotExists if source file doesn't exists
    * @throws InternalError unable to rename file or directory.
    */
  def move(src: String, dst: String): Unit = {
    val resp = client.req(s"$path/move", "post",
      s"""{"source_path":"$src", "destination_path":"$dst"}"""
    )
    client.extract[Unit](resp)
  }
}
