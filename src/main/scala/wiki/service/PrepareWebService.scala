package wiki.service

import wiki.db.Storage
import wiki.extractor.util.{ConfiguredProperties, FileHelpers, Logging}

import java.nio.file.NoSuchFileException

object PrepareWebService extends Logging {

  def main(args: Array[String]): Unit = {
    val databaseFileName = args.headOption
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    logger.info(s"Preparing web service data with db $databaseFileName")
    val db = if (FileHelpers.isFileReadable(databaseFileName)) {
      new Storage(fileName = databaseFileName)
    } else {
      throw new NoSuchFileException(s"Database file $databaseFileName is not readable")
    }

    val props = db.configuration.readConfiguredPropertiesOptimistic()

  }

  /**
    * Try to automatically infer the name of the DB file to use for running the
    * service. This only works if there is a single DB file, located in the
    * current working directory. Otherwise, the name must be given manually.
    *
    * @return The name of the file (if it can be inferred)
    */
  private def inferDbFile(): Option[String] = {
    val candidates = FileHelpers.glob("./*.db")
    if (candidates.isEmpty) {
      logger.error("No db file found in current directory. Give db file name as first argument or generate one.")
      None
    } else if (candidates.length > 1) {
      logger.error(s"Found multiple db files: ${candidates.mkString(", ")}. Give db file name as first argument.")
      None
    } else {
      candidates.headOption
    }
  }
}
