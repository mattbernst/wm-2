package wiki.service

import wiki.db.Storage
import wiki.util.{FileHelpers, Logging}

import java.nio.file.NoSuchFileException

object ShrinkDatabase extends ModelProperties with Logging {

  def main(args: Array[String]): Unit = {

    val conf = new ServiceConf(args.toIndexedSeq)
    val databaseFileName = conf.database.toOption
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    val db = if (FileHelpers.isFileReadable(databaseFileName)) {
      new Storage(fileName = databaseFileName)
    } else {
      throw new NoSuchFileException(s"Database file $databaseFileName is not readable")
    }

    val beforeSize = FileHelpers.getFileSize(databaseFileName)

    val ops = new ServiceOps(db, defaultServiceParams)
    ops.validateWordSenseModel()
    ops.validateLinkingModel()

    logger.info(s"Optimizing $databaseFileName by removing stored markup.")
    db.executeUnsafely("DROP TABLE IF EXISTS markup;")
    db.executeUnsafely("DROP TABLE IF EXISTS markup_z;")
    logger.info("Vacuuming database to reclaim space")
    db.executeUnsafely("VACUUM;")
    val afterSize = FileHelpers.getFileSize(databaseFileName)
    val ratio     = (afterSize / beforeSize.toDouble).toString.take(4)
    logger.info(s"Completed optimization. Size before: $beforeSize After: $afterSize Ratio: $ratio")
  }
}
