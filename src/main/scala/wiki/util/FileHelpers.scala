package wiki.util

import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.util.stream
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object FileHelpers extends Logging {

  def glob(pattern: String): Seq[String] = {
    val path      = Paths.get(pattern)
    val directory = Option(path.getParent).getOrElse(Paths.get("."))
    val matcher   = FileSystems.getDefault.getPathMatcher("glob:" + pattern)
    val streams: stream.Stream[Path] = Try(Files.walk(directory)) match {
      case Success(v) =>
        v
      case Failure(ex) =>
        logger.warn(s"Glob failed on $pattern: ${ex.getClass.getName}")
        stream.Stream.empty()
    }
    streams.filter(p => matcher.matches(p)).map(_.toString).iterator().asScala.toSeq
  }

  def readBinaryFile(name: String): Array[Byte] =
    Files.readAllBytes(Paths.get(name))

  def readTextFile(fileName: String): String = {
    val source = Source.fromFile(fileName)(StandardCharsets.UTF_8)
    val lines  = source.getLines().toList
    source.close()
    lines.mkString("\n")
  }

  def writeTextFile(fileName: String, input: String): Unit = {
    val writer = Files.newBufferedWriter(Paths.get(fileName))
    writer.write(input)
    writer.close()
  }

  def deleteFileIfExists(fileName: String): Unit = {
    val path = Paths.get(fileName)

    if (Files.exists(path)) {
      Try(Files.delete(path)) match {
        case Success(_)         =>
        case Failure(exception) => logger.error(s"Failed to delete file $fileName. Error: ${exception.getMessage}")
      }
    } else {
      logger.warn(s"Could not delete file $fileName because it does not exist.")
    }
  }

  def isFileReadable(fileName: String): Boolean = {
    Try {
      val path = Paths.get(fileName)
      Files.exists(path) && Files.isReadable(path)
    }.getOrElse(false)
  }

  def getFileSize(fileName: String): Long = {
    if (isFileReadable(fileName)) {
      Files.size(Paths.get(fileName))
    } else {
      throw new IllegalArgumentException(s"File $fileName does not exist or is not a regular file")
    }
  }
}
