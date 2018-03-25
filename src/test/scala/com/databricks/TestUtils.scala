package com.databricks

import java.io.{File, IOException}
import java.util.UUID

trait TestUtils {
  var MAX_DIR_CREATION_ATTEMPTS: Int = 10

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "libricks"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
    root: String = System.getProperty("java.io.tmpdir"),
    namePrefix: String = "libricks"): File = {
    val dir = createDirectory(root, namePrefix)

    dir
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  def deleteRecursively(file: File): Unit = {
    if (file != null) {
      deleteRecursivelyUsingJavaIO(file)
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  def withTempDir(f: File => Unit): Unit = {
    val dir = createTempDir().getCanonicalFile
    try {
      f(dir)
    } finally {
      deleteRecursively(dir)
    }
  }

  def deleteRecursivelyUsingJavaIO(file: File): Unit = {
    if (file.isDirectory && !isSymlink(file)) {
      var savedIOException: Exception = null
      for (child <- listFilesSafely(file)) {
        try
          deleteRecursively(child)
        catch {
          case e: IOException =>
            // In case of multiple exceptions, only last one will be thrown
            savedIOException = e
        }
      }
      if (savedIOException != null) throw savedIOException
    }
    val deleted = file.delete
    // Delete can also fail if the file simply did not exist.
    if (!deleted && file.exists) throw new IOException("Failed to delete: " + file.getAbsolutePath)
  }

  def isSymlink(file: File): Boolean = {
    val fileInCanonicalDir: File = if (file.getParent == null) {
      file
    } else {
      new File(file.getParentFile.getCanonicalFile, file.getName)
    }

    !(fileInCanonicalDir.getCanonicalFile == fileInCanonicalDir.getAbsoluteFile)
  }

  private def listFilesSafely(file: File) = {
    if (file.exists) {
      val files = file.listFiles
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      } else {
        files
      }
    } else {
      new Array[File](0)
    }
  }
}
