package wiki.service

import org.rogach.scallop.*
import wiki.db.Storage
import wiki.util.{FileHelpers, Logging}

import java.nio.file.NoSuchFileException

object LoadDisambiguationModel extends ModelProperties with Logging {

  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    // These turn into kebab-case arguments, e.g.
    // sbt "runMain wiki.service.PrepareDisambiguation --database en_wiki.db --word-sense-model en_word_sense_ranker.cbm"
    val database: ScallopOption[String]       = opt[String]()
    val wordSenseModel: ScallopOption[String] = opt[String]()
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)

    val databaseFileName = conf.database.toOption
      .orElse(inferDbFile())
      .getOrElse(throw new RuntimeException("No database file found or given!"))

    logger.info(s"Preparing word sense disambiguation data with db $databaseFileName")
    val db = getDb(databaseFileName)
    prepareModel(db, conf)
  }

  private def prepareModel(db: Storage, conf: Conf): Unit = {
    val props = db.configuration.readConfiguredPropertiesOptimistic()
    if (db.mlModel.read(wsdModelName).isEmpty) {
      val defaultFile = s"pysrc/wiki_${props.language.code}_word_sense_ranker.cbm"
      val fileName    = conf.wordSenseModel.getOrElse(defaultFile)
      if (!FileHelpers.isFileReadable(fileName)) {
        logger.error(s"Word sense disambiguation model $fileName could not be read")
        logger.error(s"You need to give the CatBoost model with --word-sense-model or run train-disambiguation")
        throw new NoSuchFileException(fileName)
      } else {
        val modelData = FileHelpers.readBinaryFile(fileName)
        require(modelData.nonEmpty, s"File $fileName exists but contains no data")
        db.mlModel.write(wsdModelName, modelData)
        val retrieved = db.mlModel.read(wsdModelName).get
        require(retrieved.sameElements(modelData), s"Model data from $fileName does not match db $wsdModelName")
        logger.info(s"Model from $fileName saved to ml_model as $wsdModelName")
      }
    } else {
      logger.info(s"Model already stored in ml_model as $wsdModelName")
    }
  }
}
