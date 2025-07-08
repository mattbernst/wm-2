package wiki.service

import wiki.db.PhaseState.COMPLETED
import wiki.db.Storage
import wiki.util.FileHelpers

import java.nio.file.NoSuchFileException

trait ModelProperties {
  val wsdModelName: String     = "word_sense_disambiguation_ranker"
  val linkingModelName: String = "link_detection_classifier"

  /**
    * Try to automatically infer the name of the DB file to use for models.
    * This only works if there is a single DB file, located in the current
    * working directory. Otherwise, the name must be given manually.
    *
    * @return The name of the file (if it can be inferred)
    */
  def inferDbFile(): Option[String] = {
    val candidates = FileHelpers.glob("./*.db")
    if (candidates.isEmpty) {
      println("No db file found in current directory. Give db file name via command line or generate one.")
      None
    } else if (candidates.length > 1) {
      println(s"Found multiple db files: ${candidates.mkString(", ")}. Give db file name via command line.")
      None
    } else {
      candidates.headOption
    }
  }

  /**
    * Get DB and validate that extraction has completed before using it.
    *
    * @param fileName The name of the SQLite database file
    * @return         A database storage object
    */
  def getDb(fileName: String): Storage = {
    val db = if (FileHelpers.isFileReadable(fileName)) {
      new Storage(fileName = fileName)
    } else {
      throw new NoSuchFileException(s"Database file $fileName is not readable")
    }

    require(
      db.phase.getPhaseState(db.phase.lastPhase).contains(COMPLETED),
      "Extraction has not completed. Finish extraction and training first."
    )

    db
  }
}
