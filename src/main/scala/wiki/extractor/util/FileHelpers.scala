package wiki.extractor.util

import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files, Paths}
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object FileHelpers extends Logging {
  def readTextFile(fileName: String): String = {
    val source = Source.fromFile(fileName)(StandardCharsets.UTF_8)
    val lines = source.getLines().toList
    source.close()
    lines.mkString("\n")
  }

  def glob(pattern: String): Seq[String] = {
    val path = Paths.get(pattern)
    val directory = Option(path.getParent).getOrElse(Paths.get("."))
    val matcher = FileSystems.getDefault.getPathMatcher("glob:" + pattern)

    Files.walk(directory)
      .filter(p => matcher.matches(p))
      .map(_.toString)
      .iterator()
      .asScala
      .toSeq
  }

  def deleteFileIfExists(fileName: String): Unit = {
    val path = Paths.get(fileName)

    if (Files.exists(path)) {
      Try(Files.delete(path)) match {
        case Success(_) =>

        case Failure(exception) =>
          logger.error(s"Failed to delete file $fileName. Error: ${exception.getMessage}")
      }
    } else {
      logger.warn(s"Could not delete file $fileName because it does not exist.")
    }
  }
}
