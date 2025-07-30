package wiki.service

import org.rogach.scallop.*
import wiki.db.Storage
import wiki.util.{FileHelpers, Logging}

import java.nio.file.NoSuchFileException

object LoadLinkDetectionModel extends ModelProperties with Logging {

  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    // These turn into kebab-case arguments, e.g.
    // sbt "runMain wiki.service.PrepareLinkDetection --database en_wiki.db --link-detection-model en_link_validity.cbm"
    val database: ScallopOption[String]           = opt[String]()
    val linkDetectionModel: ScallopOption[String] = opt[String]()

    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)

    val databaseFileName = conf.database.toOption
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    logger.info(s"Preparing link detection data with db $databaseFileName")
    val db = getDb(databaseFileName)
    prepareModel(db, conf)
  }

  private def prepareModel(db: Storage, conf: Conf): Unit = {
    val props = db.configuration.readConfiguredPropertiesOptimistic()
    if (db.mlModel.read(linkingModelName).isEmpty) {
      val defaultFile = s"pysrc/wiki_${props.language.code}_link_validity.cbm"
      val fileName    = conf.linkDetectionModel.getOrElse(defaultFile)
      if (!FileHelpers.isFileReadable(fileName)) {
        logger.error(s"Link validity model $fileName could not be read")
        logger.error(s"You need to give the CatBoost model with --link-detection-model or run train-link-detector")
        throw new NoSuchFileException(fileName)
      } else {
        val modelData = FileHelpers.readBinaryFile(fileName)
        require(modelData.nonEmpty, s"File $fileName exists but contains no data")
        db.mlModel.write(linkingModelName, modelData)
        val retrieved = db.mlModel.read(linkingModelName).get
        require(retrieved.sameElements(modelData), s"Model data from $fileName does not match db $linkingModelName")
        logger.info(s"Model from $fileName saved to ml_model as $linkingModelName")
      }
    } else {
      logger.info(s"Model already stored in ml_model as $linkingModelName")
    }
  }
}
