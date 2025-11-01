package wiki.extractor.language

import wiki.db.Storage
import wiki.util.Logging

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import scala.util.{Failure, Success, Try}

class LanguageModel(db: Storage) extends Logging {

  /**
    * Get a language model from the database or disk, using the database
    * as a cache.
    *
    * @param diskName The name of the model on disk
    * @return         An InputStream for the model data
    */
  def getModel(diskName: String): InputStream = {
    Try(db.mlModel.read(diskName)) match {
      case Success(maybeModel) =>
        maybeModel match {
          case Some(bytes: Array[Byte]) =>
            new ByteArrayInputStream(bytes)
          case None =>
            val stream             = readFromDisk(diskName)
            val bytes: Array[Byte] = stream.readAllBytes()
            db.mlModel.write(diskName, bytes)
            stream.close()
            new ByteArrayInputStream(bytes)
        }
      case Failure(exception) =>
        logger.warn(s"Error trying to load $diskName from ml_model: ${exception.getMessage}")
        readFromDisk(diskName)
    }
  }

  private def readFromDisk(diskName: String): InputStream =
    new FileInputStream(diskName)
}
